//! `DiskCache::reuse_for_event`: a re-init for the event a cache already
//! serves must keep that cache, or a second eviction sweep with an empty
//! position registry deletes the running endpoints' chunks
//! (crash-exhaustion gate, main run 38825629603).

use super::*;
use crate::disk_cache::download_service::FetchedChunk;

/// S3 never answers: these tests only exercise the cache facade and the
/// eviction sweep, never a download.
struct NoS3;

#[async_trait::async_trait]
impl S3Backend for NoS3 {
    async fn fetch(&self, _chunk_id: i64) -> Result<Option<FetchedChunk>, String> {
        Ok(None)
    }
}

async fn cache(dir: &std::path::Path, event_id: &str) -> Arc<DiskCache> {
    let cfg = DiskCacheConfig {
        cache_dir: dir.to_path_buf(),
        // Far beyond any test's runtime: the sweeps run only by hand below.
        eviction_interval_secs: 3600,
        ..Default::default()
    };
    Arc::new(
        DiskCache::new(cfg, Arc::new(NoS3), event_id.to_string(), None)
            .await
            .expect("DiskCache::new"),
    )
}

#[tokio::test]
async fn a_cache_is_reused_for_the_event_it_serves() {
    let tmp = tempfile::tempdir().unwrap();
    let running = cache(tmp.path(), "evt-a").await;
    let kept = DiskCache::reuse_for_event(Some(&running), "evt-a")
        .expect("a re-init for the same event must keep the running cache");
    assert!(Arc::ptr_eq(&kept, &running));
}

#[tokio::test]
async fn another_event_or_no_cache_builds_a_new_one() {
    let tmp = tempfile::tempdir().unwrap();
    let running = cache(tmp.path(), "evt-a").await;
    assert!(DiskCache::reuse_for_event(Some(&running), "evt-b").is_none());
    assert!(DiskCache::reuse_for_event(None, "evt-a").is_none());
}

#[tokio::test]
async fn a_re_init_keeps_the_chunks_the_running_endpoints_need() {
    let tmp = tempfile::tempdir().unwrap();
    let running = cache(tmp.path(), "evt-a").await;
    // A running endpoint sits at chunk 1080 with a 10-chunk window, and
    // chunk 1081 is already on disk.
    running.position_registry.register("ep".into(), 10);
    running.position_registry.advance("ep", 1080);
    let chunk = running.event_dir().join("1081.bin");
    std::fs::write(&chunk, b"flv").unwrap();

    // The host re-inits the same event. Whatever cache init ends up with
    // sweeps the event directory; the running endpoint's chunk must survive.
    let after_init = match DiskCache::reuse_for_event(Some(&running), "evt-a") {
        Some(c) => c,
        None => cache(tmp.path(), "evt-a").await,
    };
    EvictionTask::run_once(
        &after_init.event_dir(),
        &after_init.position_registry,
        &after_init.registry,
    )
    .await
    .unwrap();
    assert!(
        chunk.exists(),
        "a re-init's eviction sweep deleted a chunk inside the running endpoint's window"
    );
}
