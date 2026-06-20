//! S3 chunk-wipe seam for delivery startup. Extracted from `delivery.rs` so the
//! main file stays under the 1000-line CI cap (#252).
//!
//! `start_delivery` wipes any leftover S3 chunks for an event before a fresh
//! delivery cycle. The `EventChunkWiper` trait + `wipe_event_s3_chunks_with`
//! seam let tests mock the S3 side without a live bucket
//! (`delivery_tests::wipe_*`). Re-exported from `crate::delivery` so existing
//! call sites (`crate::delivery::wipe_event_s3_chunks`, the trait, the seam)
//! keep working unchanged.

use std::time::Duration;

use sqlx::SqlitePool;
use tracing::info;

use rs_core::config::Config;
use rs_core::db;

/// Trait abstracting "delete all chunks under a prefix".
///
/// Contract: `event_prefix` is the **bare** event prefix without trailing
/// slash, e.g. `"<client_uuid>/<event_name>"`. Implementations are
/// responsible for whatever path-building they need internally
/// (`S3Client::delete_event_chunks` appends the trailing slash).
/// This decouples mock fidelity from the concrete client and is
/// asserted in `delivery_tests::wipe_calls_delete_with_correct_prefix`.
#[async_trait::async_trait]
pub trait EventChunkWiper: Send + Sync {
    async fn delete_event_chunks(&self, event_prefix: &str) -> Result<u64, String>;
}

#[async_trait::async_trait]
impl EventChunkWiper for rs_endpoint::s3::S3Client {
    async fn delete_event_chunks(&self, event_prefix: &str) -> Result<u64, String> {
        rs_endpoint::s3::S3Client::delete_event_chunks(self, event_prefix)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Delete all S3 chunks under this event's prefix. Bounded by a 60s
/// timeout. Returns `Ok(deleted_count)` on success, `Err(reason)` on
/// failure. Caller is `start_delivery`, which logs the result and
/// proceeds even on failure (the strict-live-edge `compute_target_start_chunk`
/// is the primary correctness barrier; the wipe is belt-and-suspenders).
///
/// Returning `Result` (instead of swallowing errors) gives observability
/// for orphaned-chunk billing tracking (#174 review v2 finding 7).
pub async fn wipe_event_s3_chunks(
    pool: &SqlitePool,
    config: &Config,
    event_id: i64,
) -> Result<u64, String> {
    let s3 = rs_endpoint::s3::S3Client::new(&config.s3).map_err(|e| format!("init: {e}"))?;
    wipe_event_s3_chunks_with(pool, config, event_id, &s3).await
}

/// Test seam: same as `wipe_event_s3_chunks` but takes any `EventChunkWiper`.
pub async fn wipe_event_s3_chunks_with(
    pool: &SqlitePool,
    config: &Config,
    event_id: i64,
    wiper: &dyn EventChunkWiper,
) -> Result<u64, String> {
    let event = db::get_streaming_event_by_id(pool, event_id)
        .await
        .map_err(|e| format!("db lookup: {e}"))?
        .ok_or_else(|| format!("no streaming event with id {event_id}"))?;
    let event_prefix = config.event_s3_prefix(&event.name);
    let fut = wiper.delete_event_chunks(&event_prefix);
    match tokio::time::timeout(Duration::from_secs(60), fut).await {
        Ok(Ok(n)) => {
            info!(
                event_id,
                deleted = n,
                prefix = %event_prefix,
                "Wiped S3 chunks before starting delivery"
            );
            Ok(n)
        }
        Ok(Err(e)) => Err(format!("delete failed: {e}")),
        Err(_) => Err("timed out after 60s".into()),
    }
}
