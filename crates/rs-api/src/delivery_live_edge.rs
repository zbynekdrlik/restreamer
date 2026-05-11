//! Live-edge helpers extracted from `delivery.rs` to stay under the 1000-line
//! cap.  Contains the two pure functions that drive the fast-endpoint
//! live-edge reset at VPS-ready time.

use std::time::Duration;

use sqlx::SqlitePool;
use tokio::sync::mpsc;
use tracing::warn;

use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_core::db;

/// Pure-DB helper: enumerate is_fast endpoints for an event together with
/// the fresh live-edge `start_chunk_id` that should be POSTed to the VPS
/// via /api/endpoints/update_start. Returned tuple is (endpoint, new_start).
///
/// new_start = MAX(sequence_number WHERE sent=1) + 1, computed by the
/// existing `compute_target_start_chunk` helper in rs-core.
///
/// Used by `on_vps_ready` to drive the fast-endpoint live-edge recompute at
/// VPS-ready time. Extracted as a free fn so it can be unit-tested without
/// the orchestrator's full HTTP machinery.
pub async fn compute_fast_endpoint_updates(
    pool: &SqlitePool,
    event_id: i64,
) -> anyhow::Result<Vec<(rs_core::models::EndpointConfig, i64)>> {
    let endpoints = db::get_event_endpoints(pool, event_id).await?;
    let fast_endpoints: Vec<rs_core::models::EndpointConfig> =
        endpoints.into_iter().filter(|e| e.is_fast).collect();
    if fast_endpoints.is_empty() {
        return Ok(Vec::new());
    }
    let new_start = db::compute_target_start_chunk(pool, event_id).await?;
    Ok(fast_endpoints
        .into_iter()
        .map(|ep| (ep, new_start))
        .collect())
}

/// Called at the VPS "delivering" transition. For each is_fast=true
/// endpoint on this event, POST the fresh live-edge start_chunk_id to the
/// VPS via /api/endpoints/update_start so the endpoint begins pushing at
/// the live edge rather than the stale chunk_id computed before VPS
/// creation completed (30-50s ago).
///
/// Non-fast endpoints are skipped — they rely on their original
/// start_chunk_id + buffer prefill.
///
/// Graceful degradation for older VPS binaries that lack the endpoint:
/// 404 / network error => warn-log, NO audit row, continue with next
/// endpoint. Real DB errors from compute_fast_endpoint_updates bubble up.
///
/// `original_start_chunk_id` is whatever start_chunk_id the orchestrator
/// originally computed for this delivery cycle (passed in from the call
/// site so we can populate the audit row's `from_chunk_id` without
/// extra DB lookups).
pub async fn on_vps_ready(
    pool: &SqlitePool,
    audit_tx: Option<&mpsc::Sender<AuditRow>>,
    event_id: i64,
    instance: &rs_core::models::DeliveryInstance,
    original_start_chunk_id: i64,
    client: &reqwest::Client,
) -> anyhow::Result<()> {
    let updates = compute_fast_endpoint_updates(pool, event_id).await?;
    if updates.is_empty() {
        return Ok(());
    }
    let url = format!("http://{}:8000/api/endpoints/update_start", instance.ipv4);
    for (ep, new_start) in updates {
        let payload = serde_json::json!({
            "alias": ep.alias,
            "new_start_chunk_id": new_start,
        });
        let post_result = client
            .post(&url)
            .bearer_auth(&instance.auth_token)
            .json(&payload)
            .timeout(Duration::from_secs(5))
            .send()
            .await;

        let success = match post_result {
            Ok(resp) if resp.status() == reqwest::StatusCode::OK => true,
            Ok(resp) if resp.status() == reqwest::StatusCode::NOT_FOUND => {
                warn!(
                    alias = %ep.alias,
                    "VPS lacks /api/endpoints/update_start (older binary); skipping"
                );
                false
            }
            Ok(resp) => {
                warn!(
                    alias = %ep.alias,
                    status = %resp.status(),
                    "update_start unexpected status; skipping"
                );
                false
            }
            Err(e) => {
                warn!(
                    alias = %ep.alias,
                    error = %e,
                    "update_start network error; skipping"
                );
                false
            }
        };

        if success {
            if let Some(tx) = audit_tx {
                let _ = tx
                    .send(AuditRow {
                        severity: Severity::Info,
                        source: Source::Delivery,
                        event_id: Some(event_id),
                        instance_id: Some(instance.id),
                        endpoint: Some(ep.alias.clone()),
                        action: Action::FastEndpointJumpedToLiveEdge,
                        detail: serde_json::json!({
                            "alias": ep.alias,
                            "from_chunk_id": original_start_chunk_id,
                            "to_chunk_id": new_start,
                            "gap_chunks": new_start - original_start_chunk_id,
                        }),
                        ts_override: None,
                    })
                    .await;
            }
        }
    }
    Ok(())
}
