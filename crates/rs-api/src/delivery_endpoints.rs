//! Mid-stream endpoint management: add/remove endpoints to running delivery VPS,
//! start position resolution for per-endpoint positioning.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use tracing::info;

use rs_core::config::Config;
use rs_core::db;

use crate::delivery::{DeliveryOrchestrator, is_delivery_active};

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
/// - `Live`      → latest sequence number (track the current live edge)
/// - `Beginning` → first sequence number (replay from event start)
/// - `Resume`    → passes through the chunk_id directly
pub async fn resolve_start_chunk_id(
    pool: &SqlitePool,
    event_id: i64,
    position: &StartPosition,
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
            // "Live" means the current live edge — latest chunk. Starting from
            // here makes the endpoint track real-time ingest. (Historically
            // this was identical to Beginning; see 2026-04-19 post-mortem.)
            let last_seq = db::get_latest_sequence_number_for_event(pool, event_id)
                .await?
                .unwrap_or(1);
            Ok(last_seq)
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

    let start_chunk_id = resolve_start_chunk_id(pool, event_id, &start_position).await?;

    let chunk_format = &config.inpoint.chunk_format;

    let body = serde_json::json!({
        "endpoint": {
            "alias": ep.alias,
            "service_type": ep.service_type,
            "stream_key": ep.stream_key,
            "is_fast": ep.is_fast,
            "chunk_format": chunk_format,
            "start_chunk_id": start_chunk_id,
        }
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

    #[test]
    fn start_position_default_is_live() {
        let pos = StartPosition::default();
        assert!(matches!(pos, StartPosition::Live));
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
