//! S3-related API handlers: usage measurement and per-event chunk cleanup.
//!
//! Extracted from `handlers.rs` to keep that file under the project's
//! 1000-line per-file cap.

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use futures::StreamExt;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tracing::error;

use rs_core::db;
use rs_endpoint::s3::S3Client;

use crate::state::AppState;

/// Timeout on the overall S3 operation for both clear-s3 and usage.
/// Hetzner S3 is usually responsive, but a large cleanup could exceed
/// a reverse proxy's read timeout. Bound it so the handler doesn't
/// hang indefinitely and the client sees a clean error.
const S3_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);

/// Concurrency for parallel `measure_prefix` calls in `get_s3_usage`.
/// The previous sequential loop issued one round-trip per event prefix,
/// which is O(N) slow on buckets with many events. Parallelising 10 at
/// a time keeps the endpoint snappy without flooding S3.
const MEASURE_CONCURRENCY: usize = 10;

#[derive(Serialize)]
pub struct ClearChunksResponse {
    pub deleted: u64,
}

/// JSON error body returned for S3 handler failures so the frontend can
/// surface a meaningful message instead of a bare "500".
#[derive(Serialize)]
pub struct ErrorBody {
    pub error: String,
}

/// Tagged handler result that serializes as JSON for both success and
/// error branches. Axum's default behaviour for `Result<Json<_>, StatusCode>`
/// drops the body on the error path, which gives the UI nothing to show.
pub enum S3Result<T> {
    Ok(Json<T>),
    Err(StatusCode, Json<ErrorBody>),
}

impl<T: Serialize> IntoResponse for S3Result<T> {
    fn into_response(self) -> Response {
        match self {
            S3Result::Ok(j) => j.into_response(),
            S3Result::Err(code, body) => (code, body).into_response(),
        }
    }
}

fn s3_err<T>(code: StatusCode, msg: impl Into<String>) -> S3Result<T> {
    S3Result::Err(code, Json(ErrorBody { error: msg.into() }))
}

/// POST /events/{id}/clear-s3 — delete all S3 chunks for an event but
/// keep the event row in the DB. Used by the per-event "Clear S3 chunks"
/// dashboard button so the operator can free space without losing the
/// event configuration.
///
/// Concurrency: the handler takes `state.s3_mutation_lock` so only one
/// S3 delete operation runs at a time per process. This prevents two
/// simultaneous delete requests from issuing overlapping S3 LIST+DELETE
/// scans and doubling the load. After the lock is released, the handler
/// re-reads the event flags to detect any streaming activation that
/// raced against the delete (TOCTOU) and logs a warning — the delete
/// has already completed, so recovery is "just restart the stream".
pub async fn clear_event_s3_chunks(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> S3Result<ClearChunksResponse> {
    // Serialize S3 mutations with other delete/clear operations.
    let _guard = state.s3_mutation_lock.lock().await;

    let event = match db::get_streaming_event_by_id(&state.pool, id).await {
        Ok(Some(e)) => e,
        Ok(None) => return s3_err(StatusCode::NOT_FOUND, format!("event {id} not found")),
        Err(e) => {
            error!("Failed to get event {id}: {e}");
            return s3_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to load event: {e}"),
            );
        }
    };

    if event.receiving_activated || event.delivering_activated {
        return s3_err(
            StatusCode::CONFLICT,
            "cannot clear S3 chunks while event is streaming — stop the stream first",
        );
    }

    let config = match state.config_live.read() {
        Ok(c) => c.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    let s3_client = match S3Client::new(&config.s3) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to create S3 client for clear-s3 {id}: {e}");
            return s3_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("S3 client setup failed: {e}"),
            );
        }
    };

    let event_name = event.name.clone();
    let event_prefix = config.event_s3_prefix(&event_name);
    let delete_result = tokio::time::timeout(
        S3_OPERATION_TIMEOUT,
        s3_client.delete_event_chunks(&event_prefix),
    )
    .await;

    let deleted = match delete_result {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => {
            error!("Failed to clear S3 chunks for event {id} ({event_name}): {e}");
            return s3_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("S3 delete failed: {e}"),
            );
        }
        Err(_) => {
            error!(
                "Timeout clearing S3 chunks for event {id} ({event_name}) after {}s",
                S3_OPERATION_TIMEOUT.as_secs()
            );
            return s3_err(
                StatusCode::GATEWAY_TIMEOUT,
                format!(
                    "S3 delete exceeded {}s timeout",
                    S3_OPERATION_TIMEOUT.as_secs()
                ),
            );
        }
    };

    // TOCTOU double-check: re-read the event. If receiving/delivering
    // flipped true during the delete, a concurrent start-stream raced
    // us. The chunks are already gone; we can't recover, but we log so
    // the operator can restart the stream if needed.
    if let Ok(Some(post)) = db::get_streaming_event_by_id(&state.pool, id).await {
        if post.receiving_activated || post.delivering_activated {
            tracing::warn!(
                event_id = id,
                event_name = %event_name,
                "clear-s3 completed but streaming started during the delete — \
                 chunks from the new session may have been removed. Restart the \
                 stream if playback looks broken."
            );
        }
    }

    // Audit: record per-event S3 cleanup so post-mortem can correlate
    // operator cleanup actions with chunk-counter changes.
    rs_core::audit::record(
        &state.audit_tx,
        rs_core::audit::AuditRow {
            severity: rs_core::audit::Severity::Info,
            source: rs_core::audit::Source::Operator,
            event_id: Some(id),
            instance_id: None,
            endpoint: None,
            action: rs_core::audit::Action::S3Cleared,
            detail: serde_json::json!({
                "event_id": id,
                "event_name": event_name,
                "chunks_deleted": deleted,
            }),
            ts_override: None,
        },
    );

    S3Result::Ok(Json(ClearChunksResponse { deleted }))
}

#[derive(Serialize)]
pub struct S3UsageEntry {
    pub event_name: String,
    pub bytes: u64,
    pub objects: u64,
}

#[derive(Serialize)]
pub struct S3UsageResponse {
    pub total_bytes: u64,
    pub total_objects: u64,
    pub by_event: Vec<S3UsageEntry>,
}

/// GET /s3/usage — total and per-event byte/object counts in the S3 bucket.
/// Used by the dashboard to show storage consumption per event.
///
/// Per-prefix `measure_prefix` calls run in parallel (up to
/// MEASURE_CONCURRENCY) so a bucket with many events does not serialise
/// N round-trips. The whole operation is wrapped in a timeout so the
/// handler can't hang the UI indefinitely.
pub async fn get_s3_usage(State(state): State<AppState>) -> S3Result<S3UsageResponse> {
    let config = match state.config_live.read() {
        Ok(c) => c.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    };
    let s3_client = match S3Client::new(&config.s3) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            error!("Failed to create S3 client for usage query: {e}");
            return s3_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("S3 client setup failed: {e}"),
            );
        }
    };

    let base_prefix = config.client_s3_base();
    let usage_future = async move {
        let prefixes = s3_client.list_event_prefixes(&base_prefix).await?;

        // Measure every prefix in parallel. Each measurement is one S3
        // LIST call, so MEASURE_CONCURRENCY parallel calls keeps the
        // endpoint under a second even for tens of events.
        let entries: Vec<Result<S3UsageEntry, rs_endpoint::EndpointError>> =
            futures::stream::iter(prefixes)
                .map(|event_name| {
                    let client = Arc::clone(&s3_client);
                    let base = base_prefix.clone();
                    async move {
                        let full = format!("{base}{event_name}/");
                        let (bytes, objects) = client.measure_prefix(&full).await?;
                        Ok(S3UsageEntry {
                            event_name,
                            bytes,
                            objects,
                        })
                    }
                })
                .buffer_unordered(MEASURE_CONCURRENCY)
                .collect()
                .await;

        // Bail on first error so we don't return partial state that
        // misleads the operator about their storage usage.
        let mut by_event: Vec<S3UsageEntry> = Vec::with_capacity(entries.len());
        for entry in entries {
            by_event.push(entry?);
        }

        let mut total_bytes: u64 = 0;
        let mut total_objects: u64 = 0;
        for e in &by_event {
            total_bytes += e.bytes;
            total_objects += e.objects;
        }
        by_event.sort_by_key(|e| std::cmp::Reverse(e.bytes));

        Ok::<_, rs_endpoint::EndpointError>(S3UsageResponse {
            total_bytes,
            total_objects,
            by_event,
        })
    };

    match tokio::time::timeout(S3_OPERATION_TIMEOUT, usage_future).await {
        Ok(Ok(response)) => S3Result::Ok(Json(response)),
        Ok(Err(e)) => {
            error!("Failed to compute S3 usage: {e}");
            s3_err(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("S3 usage query failed: {e}"),
            )
        }
        Err(_) => {
            error!(
                "Timeout computing S3 usage after {}s",
                S3_OPERATION_TIMEOUT.as_secs()
            );
            s3_err(
                StatusCode::GATEWAY_TIMEOUT,
                format!(
                    "S3 usage query exceeded {}s timeout",
                    S3_OPERATION_TIMEOUT.as_secs()
                ),
            )
        }
    }
}
