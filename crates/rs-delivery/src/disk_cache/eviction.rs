//! EvictionTask -- deletes cache files outside any endpoint's window.
//!
//! Runs periodically. Reads `EndpointPositionRegistry` snapshot, computes
//! the union of `[pos, pos + window]` ranges, deletes any cache file
//! whose chunk_id is not in the union. Eventual consistency: the
//! ChunkRegistry slot for an evicted chunk_id is NOT updated; the next
//! reader request re-creates the slot via DownloadService::request_chunk.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use super::position_registry::EndpointPositionRegistry;
use super::registry::ChunkRegistry;

pub struct EvictionTask;

impl EvictionTask {
    /// Spawn the eviction loop. Returns the JoinHandle the caller stores.
    pub fn spawn(
        cache_dir: std::path::PathBuf,
        positions: Arc<EndpointPositionRegistry>,
        registry: Arc<ChunkRegistry>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                if let Err(e) = Self::run_once(&cache_dir, &positions, &registry).await {
                    tracing::warn!("disk_cache eviction error: {e}");
                }
            }
        })
    }

    /// Single sweep pass: list cache_dir, delete files not in any
    /// endpoint window. Returns number of files deleted.
    pub async fn run_once(
        cache_dir: &Path,
        positions: &EndpointPositionRegistry,
        _registry: &ChunkRegistry,
    ) -> std::io::Result<u64> {
        if !cache_dir.exists() {
            return Ok(0);
        }
        let needed = positions.needed_chunks();
        let mut evicted = 0u64;
        let mut entries = tokio::fs::read_dir(cache_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            // Skip in-flight writes.
            if name_str.ends_with(".part") {
                continue;
            }
            let stem = match name_str.strip_suffix(".bin") {
                Some(s) => s,
                None => continue,
            };
            let chunk_id: i64 = match stem.parse() {
                Ok(n) => n,
                Err(_) => continue,
            };
            if !needed.contains(&chunk_id) {
                match tokio::fs::remove_file(entry.path()).await {
                    Ok(()) => evicted += 1,
                    // Concurrent writer/sweeper may have removed it; benign.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
            }
        }
        if evicted > 0 {
            tracing::info!(evicted, "disk_cache: evicted unreferenced chunks");
        }
        Ok(evicted)
    }
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
        positions.register("a".into(), 30);
        positions.advance("a", 10); // window 10..=40
        positions.register("b".into(), 30);
        positions.advance("b", 100); // window 100..=130
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
        positions.register("a".into(), 10);
        positions.advance("a", 50); // window 50..=60
        let registry = ChunkRegistry::new();
        EvictionTask::run_once(&dir, &positions, &registry)
            .await
            .unwrap();
        assert_eq!(list_ids(&dir).len(), 11); // 50..=60
        // Expand window
        positions.register("a".into(), 30);
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
        positions.register("a".into(), 10);
        positions.advance("a", 0); // window 0..=10
        let registry = ChunkRegistry::new();
        EvictionTask::run_once(&dir, &positions, &registry)
            .await
            .unwrap();
        assert_eq!(list_ids(&dir).len(), 11);
        positions.deregister("a");
        EvictionTask::run_once(&dir, &positions, &registry)
            .await
            .unwrap();
        assert!(list_ids(&dir).is_empty());
    }
}
