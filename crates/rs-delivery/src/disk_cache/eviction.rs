//! EvictionTask -- deletes cache files outside any endpoint's window.

pub struct EvictionTask {
    _placeholder: (),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_cache::position_registry::EndpointPositionRegistry;
    use crate::disk_cache::registry::ChunkRegistry;
    use std::collections::BTreeSet;

    fn touch(dir: &std::path::Path, chunk_id: i64) {
        std::fs::write(dir.join(format!("{chunk_id}.bin")), b"x").unwrap();
    }

    fn list_ids(dir: &std::path::Path) -> BTreeSet<i64> {
        let mut out = BTreeSet::new();
        for e in std::fs::read_dir(dir).unwrap() {
            let name = e.unwrap().file_name().into_string().unwrap();
            if let Some(stem) = name.strip_suffix(".bin") {
                if let Ok(n) = stem.parse() {
                    out.insert(n);
                }
            }
        }
        out
    }

    #[tokio::test]
    async fn empty_position_registry_evicts_all_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("evt");
        std::fs::create_dir_all(&dir).unwrap();
        for i in 0..5 {
            touch(&dir, i);
        }
        let positions = EndpointPositionRegistry::new();
        let registry = ChunkRegistry::new();
        let evicted = EvictionTask::run_once(&dir, &positions, &registry)
            .await
            .unwrap();
        assert_eq!(evicted, 5);
        assert!(list_ids(&dir).is_empty());
    }

    #[tokio::test]
    async fn disjoint_endpoint_windows_retain_only_their_unions() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("evt");
        std::fs::create_dir_all(&dir).unwrap();
        // Cache files for chunks 0..200.
        for i in 0..200 {
            touch(&dir, i);
        }
        let positions = EndpointPositionRegistry::new();
        positions.register("a".into(), 30).await;
        positions.advance("a", 10).await; // window 10..=40
        positions.register("b".into(), 30).await;
        positions.advance("b", 100).await; // window 100..=130
        let registry = ChunkRegistry::new();
        EvictionTask::run_once(&dir, &positions, &registry)
            .await
            .unwrap();
        let kept = list_ids(&dir);
        let expected: BTreeSet<i64> = (10..=40).chain(100..=130).collect();
        assert_eq!(kept, expected);
    }

    #[tokio::test]
    async fn endpoint_window_expansion_preserved_next_tick() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("evt");
        std::fs::create_dir_all(&dir).unwrap();
        for i in 0..100 {
            touch(&dir, i);
        }
        let positions = EndpointPositionRegistry::new();
        positions.register("a".into(), 10).await;
        positions.advance("a", 50).await; // window 50..=60
        let registry = ChunkRegistry::new();
        EvictionTask::run_once(&dir, &positions, &registry)
            .await
            .unwrap();
        assert_eq!(list_ids(&dir).len(), 11); // 50..=60
        // Expand window
        positions.register("a".into(), 30).await;
        // No new files written; existing ones outside expanded window stay
        // gone (eviction can't recover deleted files). Only files inside
        // current window survive subsequent ticks.
        EvictionTask::run_once(&dir, &positions, &registry)
            .await
            .unwrap();
        // 50..=80 is the new desired window but 61..80 don't exist on disk.
        // 50..=60 remain.
        assert_eq!(list_ids(&dir).len(), 11);
    }

    #[tokio::test]
    async fn deregistered_endpoint_window_no_longer_protects() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("evt");
        std::fs::create_dir_all(&dir).unwrap();
        for i in 0..30 {
            touch(&dir, i);
        }
        let positions = EndpointPositionRegistry::new();
        positions.register("a".into(), 10).await;
        positions.advance("a", 0).await; // window 0..=10
        let registry = ChunkRegistry::new();
        EvictionTask::run_once(&dir, &positions, &registry)
            .await
            .unwrap();
        assert_eq!(list_ids(&dir).len(), 11);
        positions.deregister("a").await;
        EvictionTask::run_once(&dir, &positions, &registry)
            .await
            .unwrap();
        assert!(list_ids(&dir).is_empty());
    }
}
