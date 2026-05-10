//! Mid-stream endpoint management: add/remove endpoints to running delivery VPS,
//! start position resolution for per-endpoint positioning.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tracing::info;

use rs_core::config::Config;
use rs_core::db;

use crate::delivery::{DeliveryOrchestrator, is_delivery_active};
use crate::delivery_helpers::build_endpoint_init_entry;

/// Start position strategy for an endpoint joining a delivery session.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "strategy")]
pub enum StartPosition {
    /// Start from live edge minus buffer delay (default behavior).
    #[default]
    Live,
    /// Start from the first chunk of the event (full replay).
    Beginning,
    /// Resume from a specific chunk ID (used for crash recovery).
    Resume { chunk_id: i64 },
}

/// Resolve a StartPosition into a concrete start_chunk_id for an event.
///
/// - `Live`      → walks back from the live edge accumulating actual
///   per-chunk `duration_ms` until `target_delay_ms` of S3 content is
///   ahead of the new endpoint. This avoids any hardcoded chunk cadence
///   assumption: the FLV chunker emits variable durations (~700-2000ms
///   in production), and an earlier divisor approach (`/ 2000`) under-
///   shot the real buffer by half on production's ~1s configured cadence.
///   For fresh `Start Delivering` after S3 wipe (PR #170), no sent chunks
///   exist → falls back to `compute_target_start_chunk` semantics (1).
/// - `Beginning` → first sequence number (replay from event start)
/// - `Resume`    → passes through the chunk_id directly
pub async fn resolve_start_chunk_id(
    pool: &SqlitePool,
    event_id: i64,
    position: &StartPosition,
    target_delay_ms: u64,
) -> anyhow::Result<i64> {
    match position {
        StartPosition::Resume { chunk_id } => Ok(*chunk_id),
        StartPosition::Beginning => {
            let first = db::get_first_sequence_number_for_event(pool, event_id)
                .await?
                .unwrap_or(1);
            Ok(first)
        }
        StartPosition::Live => {
            db::compute_live_stepback_start_chunk(pool, event_id, target_delay_ms)
                .await
                .map_err(Into::into)
        }
    }
}

/// Compute delivery delay in milliseconds from config and optional per-event override.
pub fn compute_delivery_delay_ms(config: &Config, event_cache_delay_secs: Option<i64>) -> u64 {
    let delay_secs = event_cache_delay_secs
        .map(|s| s as u64)
        .unwrap_or(config.delivery.delivery_delay_secs);
    delay_secs * 1000
}

/// Summary returned by `add_endpoint_to_delivery` so the HTTP handler can
/// emit an audit row with the resolved alias + start chunk.
#[derive(Debug, Clone)]
pub struct AddEndpointOutcome {
    pub alias: String,
    pub start_chunk_id: i64,
}

/// Add a single endpoint to a running delivery VPS mid-stream.
pub async fn add_endpoint_to_delivery(
    orch: &DeliveryOrchestrator,
    pool: &SqlitePool,
    config: &Config,
    event_id: i64,
    endpoint_id: i64,
    start_position: StartPosition,
) -> anyhow::Result<AddEndpointOutcome> {
    // Look up the endpoint config from DB
    let ep = db::get_endpoint_config(pool, endpoint_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Endpoint {endpoint_id} not found"))?;

    // Get running delivery instance for this event
    let instance = db::get_delivery_instance_by_event(pool, event_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No active delivery instance for event {event_id}"))?;

    if !is_delivery_active(&instance.status) {
        return Err(anyhow::anyhow!(
            "Delivery instance is in state '{}', not in an active delivery state",
            instance.status
        ));
    }

    let event = db::get_streaming_event_by_id(pool, event_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Streaming event {event_id} not found"))?;
    let target_delay_ms = compute_delivery_delay_ms(config, event.cache_delay_secs);
    let mut start_chunk_id =
        resolve_start_chunk_id(pool, event_id, &start_position, target_delay_ms).await?;

    // S3 sanity check: the local DB may retain rows for chunks that S3
    // lifecycle has already evicted. Mirrors the fresh-start guard in
    // delivery.rs::init_endpoints — the new endpoint must start at a
    // chunk that actually exists on S3, otherwise its first fetch 404s
    // and rescue mode kicks in immediately.
    if let Ok(s3) = rs_endpoint::s3::S3Client::new(&config.s3) {
        let prefix = config.event_s3_prefix(&event.name);
        match s3
            .find_first_chunk_id_at_or_after(&prefix, start_chunk_id)
            .await
        {
            Ok(Some(actual)) if actual != start_chunk_id => {
                info!(
                    event_id,
                    endpoint = %ep.alias,
                    was = start_chunk_id,
                    actual,
                    "mid-stream add: advanced start to first existing S3 chunk"
                );
                start_chunk_id = actual;
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(event_id, "mid-stream add S3 LIST validation failed: {e}"),
        }
    }

    let chunk_format = &config.inpoint.chunk_format;

    // Reuse the same helper as delivery_init so the VPS receives the same
    // shape — including `pusher`. Without this, mid-stream adds silently
    // fell back to ffmpeg even when the DB row was pusher='rust' (#160 follow-up).
    let body = serde_json::json!({
        "endpoint": build_endpoint_init_entry(&ep, chunk_format, start_chunk_id),
    });

    let delivery_url = format!("http://{}:8000", instance.ipv4);
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{delivery_url}/api/endpoints/add"))
        .bearer_auth(&instance.auth_token)
        .json(&body)
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!(
            "rs-delivery /api/endpoints/add returned {status}: {text}"
        ));
    }

    // Update fast cache
    orch.update_endpoint_fast_cache(event_id, &ep.alias, ep.is_fast)
        .await;

    info!(
        event_id,
        endpoint = %ep.alias,
        start_chunk_id,
        "Added endpoint to running delivery"
    );

    Ok(AddEndpointOutcome {
        alias: ep.alias,
        start_chunk_id,
    })
}

/// Remove a single endpoint from a running delivery VPS mid-stream.
///
/// Pass `force=true` to bypass the remove-last-endpoint guard (used by the
/// cleanup/stop-delivery path, or by the HTTP handler when `x-force-remove:
/// true` is present).
pub async fn remove_endpoint_from_delivery(
    orch: &DeliveryOrchestrator,
    pool: &SqlitePool,
    event_id: i64,
    alias: &str,
    force: bool,
) -> anyhow::Result<bool> {
    let instance = db::get_delivery_instance_by_event(pool, event_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("No active delivery instance for event {event_id}"))?;

    if !is_delivery_active(&instance.status) {
        return Err(anyhow::anyhow!(
            "Delivery instance is in state '{}', not in an active delivery state",
            instance.status
        ));
    }

    // Always compute the endpoint count so callers (and the audit row)
    // know whether this removal left zero endpoints behind.
    let endpoint_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM delivery_endpoint_status WHERE instance_id = ?1")
            .bind(instance.id)
            .fetch_one(pool)
            .await?;
    let was_last_endpoint = endpoint_count <= 1;

    // Remove-last-endpoint guard: if delivery is currently active and this
    // is the only endpoint left, refuse unless the caller explicitly forced.
    if !force {
        let delivering_activated: i64 =
            sqlx::query_scalar("SELECT delivering_activated FROM streaming_events WHERE id = ?1")
                .bind(event_id)
                .fetch_one(pool)
                .await?;
        if delivering_activated != 0 && was_last_endpoint {
            return Err(anyhow::anyhow!(
                "would_leave_zero_endpoints: delivery active and removing '{alias}' leaves 0 endpoints; \
                 pass x-force-remove:true header to override"
            ));
        }
    }

    let delivery_url = format!("http://{}:8000", instance.ipv4);
    let client = reqwest::Client::new();
    let body = serde_json::json!({ "alias": alias });
    let resp = client
        .post(format!("{delivery_url}/api/endpoints/remove"))
        .bearer_auth(&instance.auth_token)
        .json(&body)
        .timeout(Duration::from_secs(10))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!(
            "rs-delivery /api/endpoints/remove returned {status}: {text}"
        ));
    }

    // Remove from fast cache
    orch.remove_endpoint_from_fast_cache(event_id, alias).await;

    info!(
        event_id,
        endpoint = %alias,
        "Removed endpoint from running delivery"
    );

    Ok(was_last_endpoint)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_core::db::{
        create_memory_pool, insert_chunk, run_migrations, set_chunk_sent, upsert_streaming_event,
    };

    #[test]
    fn start_position_default_is_live() {
        let pos = StartPosition::default();
        assert!(matches!(pos, StartPosition::Live));
    }

    async fn pool_with_chunks_at_duration(
        event_name: &str,
        n_sent: i64,
        duration_ms: i64,
    ) -> (SqlitePool, i64) {
        let pool = create_memory_pool().await.unwrap();
        run_migrations(&pool).await.unwrap();
        let event_id = upsert_streaming_event(&pool, event_name).await.unwrap();
        for i in 1..=n_sent {
            let cid = insert_chunk(
                &pool,
                event_id,
                &format!("/tmp/c{i}.bin"),
                100,
                &format!("md5_{i}"),
                duration_ms,
            )
            .await
            .unwrap();
            set_chunk_sent(&pool, cid).await.unwrap();
        }
        (pool, event_id)
    }

    #[tokio::test]
    async fn live_stepback_at_production_1s_cadence() {
        // Production default: chunk_duration_ms=1000. With target_delay=120_000
        // and 200 sent chunks, walk back 120 chunks of 1000 ms each → start=81.
        // New endpoint immediately has 120s of S3 buffer ahead and can push
        // without rebuilding from scratch.
        let (pool, event_id) =
            pool_with_chunks_at_duration("live-1s-cadence-test", 200, 1000).await;
        let start = resolve_start_chunk_id(&pool, event_id, &StartPosition::Live, 120_000)
            .await
            .unwrap();
        assert_eq!(start, 81);
    }

    #[tokio::test]
    async fn live_stepback_at_2s_cadence() {
        // Older / config-overridden cadence: 2s chunks. Walk back 60 chunks.
        let (pool, event_id) = pool_with_chunks_at_duration("live-2s-test", 200, 2000).await;
        let start = resolve_start_chunk_id(&pool, event_id, &StartPosition::Live, 120_000)
            .await
            .unwrap();
        assert_eq!(start, 141);
    }

    #[tokio::test]
    async fn live_stepback_walks_to_oldest_when_few_chunks() {
        // Only 5 chunks at 1s each = 5s total. Cannot cover a 120s delay,
        // so fall back to the earliest sent chunk (1).
        let (pool, event_id) = pool_with_chunks_at_duration("live-shortfall-test", 5, 1000).await;
        let start = resolve_start_chunk_id(&pool, event_id, &StartPosition::Live, 120_000)
            .await
            .unwrap();
        assert_eq!(start, 1);
    }

    #[tokio::test]
    async fn live_stepback_zero_delay_returns_live_edge() {
        let (pool, event_id) = pool_with_chunks_at_duration("live-zero-delay-test", 50, 1000).await;
        let start = resolve_start_chunk_id(&pool, event_id, &StartPosition::Live, 0)
            .await
            .unwrap();
        // No stepback requested → live_edge = 51.
        assert_eq!(start, 51);
    }

    #[tokio::test]
    async fn live_stepback_handles_variable_chunk_durations() {
        // FLV chunker emits variable durations (700-2000 ms in production).
        // Mix 100 chunks: alternating 700ms and 1700ms (avg 1200ms). With
        // target=120_000 the walk should accumulate ~120 sec irrespective
        // of the gaps. 700+1700 = 2400 ms / 2 chunks. 120_000 / 2400 = 50
        // pairs = 100 chunks. We have 100 → walks to oldest (1).
        let pool = create_memory_pool().await.unwrap();
        run_migrations(&pool).await.unwrap();
        let event_id = upsert_streaming_event(&pool, "live-variable-test")
            .await
            .unwrap();
        for i in 1..=100i64 {
            let dur = if i % 2 == 0 { 1700 } else { 700 };
            let cid = insert_chunk(
                &pool,
                event_id,
                &format!("/tmp/c{i}.bin"),
                100,
                &format!("md5_{i}"),
                dur,
            )
            .await
            .unwrap();
            set_chunk_sent(&pool, cid).await.unwrap();
        }
        // Total content = 50 * 2400 = 120_000 ms exactly. Loop accumulates
        // the entire range, accum hits 120_000 on the LAST iteration (chunk 1)
        // and returns 1.
        let start = resolve_start_chunk_id(&pool, event_id, &StartPosition::Live, 120_000)
            .await
            .unwrap();
        assert_eq!(start, 1);

        // Smaller delay: 12_000 ms = 5 pairs = 10 chunks back from edge 101.
        // Walking from chunk 100 down: each pair = 2400 ms. After 5 pairs
        // (chunks 100..91) accum = 12_000, returns 91.
        let start_short = resolve_start_chunk_id(&pool, event_id, &StartPosition::Live, 12_000)
            .await
            .unwrap();
        assert_eq!(start_short, 91);
    }

    #[tokio::test]
    async fn beginning_position_returns_first_sequence() {
        let (pool, event_id) = pool_with_chunks_at_duration("beginning-test", 10, 1000).await;
        let start = resolve_start_chunk_id(&pool, event_id, &StartPosition::Beginning, 120_000)
            .await
            .unwrap();
        assert_eq!(start, 1);
    }

    #[tokio::test]
    async fn resume_position_passes_through() {
        let (pool, event_id) = pool_with_chunks_at_duration("resume-test", 100, 1000).await;
        let start = resolve_start_chunk_id(
            &pool,
            event_id,
            &StartPosition::Resume { chunk_id: 42 },
            120_000,
        )
        .await
        .unwrap();
        assert_eq!(start, 42);
    }

    #[test]
    fn start_position_serde_roundtrip() {
        let positions = vec![
            StartPosition::Live,
            StartPosition::Beginning,
            StartPosition::Resume { chunk_id: 42 },
        ];
        for pos in positions {
            let json = serde_json::to_string(&pos).unwrap();
            let back: StartPosition = serde_json::from_str(&json).unwrap();
            match (&pos, &back) {
                (StartPosition::Live, StartPosition::Live) => {}
                (StartPosition::Beginning, StartPosition::Beginning) => {}
                (StartPosition::Resume { chunk_id: a }, StartPosition::Resume { chunk_id: b }) => {
                    assert_eq!(a, b);
                }
                _ => panic!("Mismatch: {pos:?} vs {back:?}"),
            }
        }
    }
}
