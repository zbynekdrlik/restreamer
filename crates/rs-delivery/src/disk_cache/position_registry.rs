//! EndpointPositionRegistry -- tracks per-endpoint chunk_id for eviction.
//!
//! `EvictionTask` reads `needed_chunks()` each sweep tick to decide which
//! cache files to keep. Each endpoint's needed-set is `[current, current
//! + window]`; the global needed-set is the union across endpoints.
//!
//! Synchronous lock so `register` returns before any caller can race a
//! concurrent `advance` (#174 review finding 1: a tokio::spawn'd register
//! lost to a same-tick advance silently dropped the endpoint's window).

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use parking_lot::RwLock;

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
    pub fn register(&self, alias: String, window_chunks: i64) {
        let mut g = self.inner.write();
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

    pub fn advance(&self, alias: &str, chunk_id: i64) {
        let mut g = self.inner.write();
        if let Some(w) = g.get_mut(alias) {
            w.current_chunk_id = chunk_id;
        }
    }

    pub fn deregister(&self, alias: &str) {
        let mut g = self.inner.write();
        g.remove(alias);
    }

    pub fn snapshot(&self) -> Vec<EndpointWindow> {
        self.inner.read().values().cloned().collect()
    }

    /// Union of `[current, current + window]` across all endpoints.
    pub fn needed_chunks(&self) -> BTreeSet<i64> {
        let g = self.inner.read();
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

    #[test]
    fn register_creates_window_with_zero_position_initially() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 30);
        let snap = r.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].alias, "a");
        assert_eq!(snap[0].current_chunk_id, 0);
        assert_eq!(snap[0].cache_window_chunks, 30);
    }

    #[test]
    fn advance_updates_current_chunk_id() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 30);
        r.advance("a", 42);
        let snap = r.snapshot();
        assert_eq!(snap[0].current_chunk_id, 42);
    }

    #[test]
    fn advance_before_register_is_silent_noop_then_register_sets_baseline() {
        let r = EndpointPositionRegistry::new();
        // Without a prior register, advance does nothing -- this is the
        // race the sync register() prevents.
        r.advance("a", 99);
        assert!(r.snapshot().is_empty());
        r.register("a".into(), 10);
        // Position starts at 0 (advance got dropped), not 99.
        assert_eq!(r.snapshot()[0].current_chunk_id, 0);
    }

    #[test]
    fn deregister_removes_endpoint_from_snapshot() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 30);
        r.register("b".into(), 30);
        r.deregister("a");
        let snap = r.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].alias, "b");
    }

    #[test]
    fn needed_chunks_unions_per_endpoint_windows() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 5);
        r.advance("a", 10); // window 10..=15
        r.register("b".into(), 5);
        r.advance("b", 100); // window 100..=105
        let needed = r.needed_chunks();
        let expected: BTreeSet<i64> = (10..=15).chain(100..=105).collect();
        assert_eq!(needed, expected);
    }

    #[test]
    fn needed_chunks_handles_overlapping_windows() {
        let r = EndpointPositionRegistry::new();
        r.register("a".into(), 10);
        r.advance("a", 50); // 50..=60
        r.register("b".into(), 10);
        r.advance("b", 55); // 55..=65
        let needed = r.needed_chunks();
        // Union: 50..=65
        assert_eq!(needed.len(), 16);
        assert!(needed.contains(&50));
        assert!(needed.contains(&65));
    }

    #[test]
    fn empty_registry_yields_empty_needed_set() {
        let r = EndpointPositionRegistry::new();
        assert!(r.needed_chunks().is_empty());
    }
}
