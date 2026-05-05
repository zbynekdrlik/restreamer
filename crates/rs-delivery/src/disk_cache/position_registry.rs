//! EndpointPositionRegistry -- tracks per-endpoint chunk_id for eviction.
//!
//! `EvictionTask` reads `needed_chunks()` each sweep tick to decide which
//! cache files to keep. Each endpoint's needed-set is `[current, current
//! + window]`; the global needed-set is the union across endpoints.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct EndpointWindow {
    pub alias: String,
    pub current_chunk_id: i64,
    pub cache_window_chunks: i64,
}

pub struct EndpointPositionRegistry {
    inner: RwLock<HashMap<String, EndpointWindow>>,
}

impl EndpointPositionRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(HashMap::new()),
        })
    }

    /// Register or re-register an endpoint with the given window size.
    /// On re-register, preserves the existing `current_chunk_id` so an
    /// operator changing `cache_delay_secs` mid-event does not rewind
    /// the read position.
    pub async fn register(&self, alias: String, window_chunks: i64) {
        let mut g = self.inner.write().await;
        let existing = g.get(&alias).map(|w| w.current_chunk_id).unwrap_or(0);
        g.insert(
            alias.clone(),
            EndpointWindow {
                alias,
                current_chunk_id: existing,
                cache_window_chunks: window_chunks,
            },
        );
    }

    pub async fn advance(&self, alias: &str, chunk_id: i64) {
        let mut g = self.inner.write().await;
        if let Some(w) = g.get_mut(alias) {
            w.current_chunk_id = chunk_id;
        }
    }

    pub async fn deregister(&self, alias: &str) {
        let mut g = self.inner.write().await;
        g.remove(alias);
    }

    pub async fn snapshot(&self) -> Vec<EndpointWindow> {
        self.inner.read().await.values().cloned().collect()
    }

    /// Union of `[current, current + window]` across all endpoints.
    pub async fn needed_chunks(&self) -> BTreeSet<i64> {
        let g = self.inner.read().await;
        let mut needed = BTreeSet::new();
        for w in g.values() {
            for id in w.current_chunk_id..=(w.current_chunk_id + w.cache_window_chunks) {
                needed.insert(id);
            }
        }
        needed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[tokio::test]
    async fn register_creates_window_with_zero_position_initially() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 30).await;
        let snap = r.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].alias, "a");
        assert_eq!(snap[0].current_chunk_id, 0);
        assert_eq!(snap[0].cache_window_chunks, 30);
    }

    #[tokio::test]
    async fn advance_updates_current_chunk_id() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 30).await;
        r.advance("a", 42).await;
        let snap = r.snapshot().await;
        assert_eq!(snap[0].current_chunk_id, 42);
    }

    #[tokio::test]
    async fn deregister_removes_endpoint_from_snapshot() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 30).await;
        r.register("b".into(), 30).await;
        r.deregister("a").await;
        let snap = r.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].alias, "b");
    }

    #[tokio::test]
    async fn needed_chunks_unions_per_endpoint_windows() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 5).await;
        r.advance("a", 10).await; // window 10..=15
        r.register("b".into(), 5).await;
        r.advance("b", 100).await; // window 100..=105
        let needed = r.needed_chunks().await;
        let expected: BTreeSet<i64> = (10..=15).chain(100..=105).collect();
        assert_eq!(needed, expected);
    }

    #[tokio::test]
    async fn needed_chunks_handles_overlapping_windows() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 10).await;
        r.advance("a", 50).await; // 50..=60
        r.register("b".into(), 10).await;
        r.advance("b", 55).await; // 55..=65
        let needed = r.needed_chunks().await;
        // Union: 50..=65
        assert_eq!(needed.len(), 16);
        assert!(needed.contains(&50));
        assert!(needed.contains(&65));
    }

    #[tokio::test]
    async fn empty_registry_yields_empty_needed_set() {
        let r = EndpointPositionRegistry::new();
        assert!(r.needed_chunks().await.is_empty());
    }
}
