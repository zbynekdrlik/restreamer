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
    ///
    /// `audit_ring` (threaded from `DiskCache::new`) receives a rate-limited
    /// (1/min) `DiskCacheChunkEvicted` row whenever a sweep deletes chunks.
    /// The `RateLimiter` lives for the loop's lifetime so the cap spans
    /// sweeps.
    pub fn spawn(
        cache_dir: std::path::PathBuf,
        positions: Arc<EndpointPositionRegistry>,
        registry: Arc<ChunkRegistry>,
        interval: Duration,
        audit_ring: Option<Arc<crate::audit_ring::AuditRing>>,
    ) -> tokio::task::JoinHandle<()> {
        let rl = rs_core::audit::RateLimiter::new();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                match Self::run_once(&cache_dir, &positions, &registry).await {
                    Ok(evicted) if evicted > 0 => Self::emit_evicted(&audit_ring, &rl, evicted),
                    Ok(_) => {}
                    Err(e) => tracing::warn!("disk_cache eviction error: {e}"),
                }
            }
        })
    }

    /// Emit a rate-limited (1/min) `DiskCacheChunkEvicted` audit row. Only
    /// the live spawn loop emits; tests call `run_once` directly without an
    /// audit ring, so eviction behaviour stays test-observable via the
    /// returned count.
    fn emit_evicted(
        audit_ring: &Option<Arc<crate::audit_ring::AuditRing>>,
        rl: &rs_core::audit::RateLimiter,
        evicted: u64,
    ) {
        if let Some(ring) = audit_ring {
            if rl.allow(rs_core::audit::Action::DiskCacheChunkEvicted, "evicted") {
                ring.push_parts(crate::audit_ring::RingRowParts {
                    severity: rs_core::audit::Severity::Info,
                    source: rs_core::audit::Source::Vps,
                    endpoint: None,
                    action: rs_core::audit::Action::DiskCacheChunkEvicted,
                    detail: serde_json::json!({ "evicted": evicted }),
                });
            }
        }
    }

    /// Single sweep pass: list cache_dir, delete files not in any
    /// endpoint window, mark the registry slot as Evicted so a stale
    /// reader can react instead of hitting ENOENT (#174 review
    /// finding 3). Returns number of files deleted.
    pub async fn run_once(
        cache_dir: &Path,
        positions: &EndpointPositionRegistry,
        registry: &Arc<ChunkRegistry>,
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
                    Ok(()) => {
                        registry.mark_evicted(chunk_id);
                        evicted += 1;
                    }
                    // Concurrent writer/sweeper may have removed it; benign.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        registry.mark_evicted(chunk_id);
                    }
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
    async fn evicted_chunk_id_is_marked_in_registry() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("evt");
        std::fs::create_dir_all(&dir).unwrap();
        touch(&dir, 1);
        touch(&dir, 2);
        let positions = EndpointPositionRegistry::new();
        // No registered endpoints: every file is unreferenced.
        let registry = ChunkRegistry::new();
        // Pretend chunks 1 and 2 had previously been marked available.
        registry.mark_available(1, 1);
        registry.mark_available(2, 1);
        EvictionTask::run_once(&dir, &positions, &registry)
            .await
            .unwrap();
        // After eviction, registry must report Evicted (not stale Available).
        let s1 = registry.wait_for_chunk(1).await.unwrap();
        let s2 = registry.wait_for_chunk(2).await.unwrap();
        assert!(matches!(
            s1,
            crate::disk_cache::registry::ChunkAvailability::Evicted
        ));
        assert!(matches!(
            s2,
            crate::disk_cache::registry::ChunkAvailability::Evicted
        ));
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
