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

    /// Count the objects still present under this event's prefix. Used by the
    /// post-wipe safety guard (#285) to PROVE the prefix is clean before
    /// delivery proceeds — a successful delete is not proof of emptiness (a
    /// partial delete, a concurrent fossil, or eventual consistency can leave
    /// objects behind). Same `event_prefix` contract as `delete_event_chunks`.
    async fn count_event_chunks(&self, event_prefix: &str) -> Result<u64, String>;
}

#[async_trait::async_trait]
impl EventChunkWiper for rs_endpoint::s3::S3Client {
    async fn delete_event_chunks(&self, event_prefix: &str) -> Result<u64, String> {
        rs_endpoint::s3::S3Client::delete_event_chunks(self, event_prefix)
            .await
            .map_err(|e| e.to_string())
    }

    async fn count_event_chunks(&self, event_prefix: &str) -> Result<u64, String> {
        // `measure_prefix` returns (total_bytes, object_count); count the same
        // trailing-slash set that `delete_event_chunks` targets.
        rs_endpoint::s3::S3Client::measure_prefix(self, &format!("{event_prefix}/"))
            .await
            .map(|(_bytes, objects)| objects)
            .map_err(|e| e.to_string())
    }
}

/// Delete all S3 chunks under this event's prefix, then VERIFY the prefix is
/// provably empty (#285). Bounded by timeouts. Returns `Ok(deleted_count)`
/// only when the prefix is clean afterwards; `Err(reason)` when the delete
/// failed/timed out, when the post-wipe verify failed/timed out, or when any
/// object still remains.
///
/// The caller `start_delivery` now FAILS LOUDLY on `Err` (#285). A dirty
/// prefix — fossils left by a prior cycle after a timed-out or partial wipe —
/// would let the delivery VPS lag-probe jump onto stale objects and broadcast
/// last-session video to the live endpoints. A successful delete is not proof
/// of emptiness (partial delete, concurrent fossil, eventual consistency), so
/// the guard positively verifies the prefix is clean before delivery proceeds.
/// This mirrors the CI-side pre-flight "S3 prefix must be empty" guard.
///
/// Returning `Result` also gives observability for orphaned-chunk billing
/// tracking (#174 review v2 finding 7).
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

    let deleted = match tokio::time::timeout(
        Duration::from_secs(60),
        wiper.delete_event_chunks(&event_prefix),
    )
    .await
    {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => return Err(format!("delete failed: {e}")),
        Err(_) => return Err("timed out after 60s".into()),
    };

    // Wipe-safety guard (#285): PROVE the prefix is empty before delivery
    // proceeds. Fail loudly on any remaining object, or if we cannot even
    // verify emptiness — never silently consume stale chunks.
    let remaining = match tokio::time::timeout(
        Duration::from_secs(30),
        wiper.count_event_chunks(&event_prefix),
    )
    .await
    {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => return Err(format!("post-wipe verify failed: {e}")),
        Err(_) => return Err("post-wipe verify timed out after 30s".into()),
    };
    if remaining > 0 {
        return Err(format!(
            "S3 prefix '{event_prefix}' not clean after wipe: {remaining} object(s) remain"
        ));
    }

    info!(
        event_id,
        deleted,
        prefix = %event_prefix,
        "Wiped + verified clean S3 prefix before starting delivery"
    );
    Ok(deleted)
}
