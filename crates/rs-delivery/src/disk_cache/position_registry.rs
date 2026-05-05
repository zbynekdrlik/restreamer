//! EndpointPositionRegistry -- tracks per-endpoint chunk_id for eviction.

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EndpointWindow {
    pub alias: String,
    pub current_chunk_id: i64,
    pub cache_window_chunks: i64,
}

pub struct EndpointPositionRegistry {
    _placeholder: (),
}

impl EndpointPositionRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { _placeholder: () })
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
