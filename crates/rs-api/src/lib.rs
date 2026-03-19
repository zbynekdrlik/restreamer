pub mod delivery;
pub mod handlers;
pub mod router;
#[cfg(test)]
mod router_tests;
pub mod state;
pub mod stream_handlers;
pub mod websocket;
pub mod youtube;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::info;

use rs_core::db;
use rs_core::models::{DeliveryEndpointMetrics, WsEvent};

use crate::state::AppState;

/// Start the API server on the given address.
/// Returns the actual bound address and a JoinHandle for shutdown coordination.
pub async fn serve(
    state: AppState,
    addr: SocketAddr,
) -> anyhow::Result<(SocketAddr, JoinHandle<()>)> {
    // Spawn delivery status broadcast loop if orchestrator is available
    if let Some(ref orch) = state.delivery_orchestrator {
        let orch = Arc::clone(orch);
        let pool = state.pool.clone();
        let ws_tx = state.ws_tx.clone();
        let cached = Arc::clone(&state.cached_delivery);
        let config = state.config.clone();
        tokio::spawn(async move {
            delivery_broadcast_loop(orch, pool, ws_tx, cached, config).await;
        });
    }

    let app = router::build_router(state);
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    info!("API server listening on {local_addr}");

    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("API server error: {e}");
        }
    });

    Ok((local_addr, handle))
}

/// Background loop that polls delivery metrics every 2 seconds and broadcasts
/// WsEvent::DeliveryStatus to all connected WebSocket clients.
#[allow(clippy::too_many_arguments)]
async fn delivery_broadcast_loop(
    orch: Arc<delivery::DeliveryOrchestrator>,
    pool: sqlx::SqlitePool,
    ws_tx: tokio::sync::broadcast::Sender<WsEvent>,
    cached: std::sync::Arc<std::sync::RwLock<state::CachedDeliveryStatus>>,
    config: std::sync::Arc<rs_core::config::Config>,
) {
    // Track previous endpoint alive state for ActivityFeed transitions
    let mut prev_alive: std::collections::HashMap<String, bool> = std::collections::HashMap::new();

    // Track last-known state for predictive buffer drain
    let mut last_success_time: Option<std::time::Instant> = None;
    let mut last_delay_secs: f64 = 0.0;
    let mut last_target_delay: u64 = 0;
    let mut last_event_id: Option<i64> = None;
    let mut last_event_name: Option<String> = None;
    let mut last_state_str = String::from("idle");
    let mut was_predicted = false;

    // Track session start time for display in dashboard
    let mut session_start_time: Option<String> = None;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Find the active streaming event with delivering_activated
        let event = match db::get_streaming_event(&pool).await {
            Ok(Some(e)) if e.delivering_activated => e,
            _ => {
                // Broadcast "none" status when not delivering
                let none_status = state::CachedDeliveryStatus {
                    status: "none".to_string(),
                    ..Default::default()
                };
                if let Ok(mut c) = cached.write() {
                    *c = none_status;
                }
                let _ = ws_tx.send(WsEvent::DeliveryStatus {
                    instance_name: String::new(),
                    status: "none".to_string(),
                    server_ip: None,
                    endpoint_count: 0,
                    endpoints: Vec::new(),
                });
                let _ = ws_tx.send(WsEvent::PipelineState {
                    state: "idle".to_string(),
                    event_id: None,
                    event_name: None,
                    buffer_progress: 0.0,
                    target_delay_secs: 0,
                    current_delay_secs: 0.0,
                    session_start: None,
                    predicted: false,
                });
                prev_alive.clear();
                // Reset prediction state when not delivering
                last_success_time = None;
                was_predicted = false;
                session_start_time = None;
                continue;
            }
        };

        // Initialize session start time on first delivering tick
        if session_start_time.is_none() {
            session_start_time = Some(chrono::Utc::now().to_rfc3339());
        }

        match orch.poll_delivery_metrics(event.id).await {
            Ok((name, status, server_ip, _endpoint_count, endpoints)) => {
                // Supplement empty endpoints with configured placeholders
                let (final_endpoints, final_ep_count) = if endpoints.is_empty() {
                    let configured = db::get_event_endpoints(&pool, event.id)
                        .await
                        .unwrap_or_default();
                    let placeholders: Vec<DeliveryEndpointMetrics> = configured
                        .iter()
                        .map(|ep| DeliveryEndpointMetrics {
                            alias: ep.alias.clone(),
                            alive: false,
                            current_chunk_id: 0,
                            bytes_processed_total: 0,
                            chunks_processed: 0,
                            chunk_delay_secs: 0.0,
                            stall_reason: None,
                            ffmpeg_restart_count: 0,
                            last_error: None,
                        })
                        .collect();
                    let count = placeholders.len() as u32;
                    (placeholders, count)
                } else {
                    let count = endpoints.len() as u32;
                    (endpoints.clone(), count)
                };

                // Cache for instant HTTP retrieval
                if let Ok(mut c) = cached.write() {
                    *c = state::CachedDeliveryStatus {
                        instance_name: name.clone(),
                        status: status.clone(),
                        server_ip: server_ip.clone(),
                        endpoint_count: final_ep_count,
                        endpoints: final_endpoints.clone(),
                    };
                }
                let _ = ws_tx.send(WsEvent::DeliveryStatus {
                    instance_name: name,
                    status,
                    server_ip,
                    endpoint_count: final_ep_count,
                    endpoints: final_endpoints.clone(),
                });

                // Compute and broadcast PipelineState
                let any_alive = final_endpoints.iter().any(|m| m.alive);
                let state_str = if any_alive { "streaming" } else { "buffering" };

                let target_delay = event
                    .cache_delay_secs
                    .map(|s| s as u64)
                    .unwrap_or(config.delivery.delivery_delay_secs);

                // Compute buffer: local S3 count if VPS not yet responding, else real VPS delay
                let (current_delay, buffer_progress) = if endpoints.is_empty() {
                    // VPS not responding — use local S3 buffer as progress indicator
                    let sent = db::get_sent_chunk_count_for_event(&pool, event.id)
                        .await
                        .unwrap_or(0);
                    let chunk_dur = config.inpoint.chunk_duration_ms as f64 / 1000.0;
                    let local_buf = sent as f64 * chunk_dur;
                    let progress = if target_delay > 0 {
                        (local_buf / target_delay as f64).min(1.0)
                    } else {
                        0.0
                    };
                    (local_buf, progress)
                } else {
                    let delay = final_endpoints
                        .iter()
                        .filter(|m| m.chunk_delay_secs > 0.0)
                        .map(|m| m.chunk_delay_secs)
                        .fold(f64::MAX, f64::min);
                    let delay = if delay == f64::MAX { 0.0 } else { delay };
                    let progress = if target_delay > 0 {
                        (delay / target_delay as f64).min(1.0)
                    } else {
                        1.0
                    };
                    (delay, progress)
                };

                // Emit restoration event if recovering from prediction
                if was_predicted {
                    let _ = ws_tx.send(WsEvent::ActivityFeed {
                        timestamp: chrono::Utc::now().to_rfc3339(),
                        severity: "info".to_string(),
                        message: "Delivery VPS connection restored".to_string(),
                        source: "delivery".to_string(),
                    });
                    was_predicted = false;
                }

                // Save last-known state for predictive drain
                last_success_time = Some(std::time::Instant::now());
                last_delay_secs = current_delay;
                last_target_delay = target_delay;
                last_event_id = Some(event.id);
                last_event_name = Some(event.name.clone());
                last_state_str = state_str.to_string();

                let _ = ws_tx.send(WsEvent::PipelineState {
                    state: state_str.to_string(),
                    event_id: Some(event.id),
                    event_name: Some(event.name.clone()),
                    buffer_progress,
                    target_delay_secs: target_delay,
                    current_delay_secs: current_delay,
                    session_start: session_start_time.clone(),
                    predicted: false,
                });

                // Emit ActivityFeed for endpoint state transitions
                for ep in &final_endpoints {
                    let was_alive = prev_alive.get(&ep.alias).copied().unwrap_or(false);
                    if ep.alive && !was_alive {
                        let _ = ws_tx.send(WsEvent::ActivityFeed {
                            timestamp: chrono::Utc::now().to_rfc3339(),
                            severity: "info".to_string(),
                            message: format!("Endpoint '{}' is now streaming", ep.alias),
                            source: "delivery".to_string(),
                        });
                    } else if !ep.alive && was_alive {
                        let reason = ep.stall_reason.as_deref().unwrap_or("unknown");
                        let _ = ws_tx.send(WsEvent::ActivityFeed {
                            timestamp: chrono::Utc::now().to_rfc3339(),
                            severity: "warning".to_string(),
                            message: format!("Endpoint '{}' stalled: {}", ep.alias, reason),
                            source: "delivery".to_string(),
                        });
                    }
                    prev_alive.insert(ep.alias.clone(), ep.alive);
                }
            }
            Err(e) => {
                tracing::debug!("Delivery metrics poll failed: {e}");
                if let Some(last_time) = last_success_time {
                    let elapsed = last_time.elapsed().as_secs_f64();
                    let predicted_delay = (last_delay_secs - elapsed).max(0.0);
                    let predicted_progress = if last_target_delay > 0 {
                        (predicted_delay / last_target_delay as f64).min(1.0)
                    } else {
                        0.0
                    };
                    let predicted_state = if predicted_delay <= 0.0 {
                        "buffer_exhausted"
                    } else {
                        &last_state_str
                    };
                    let _ = ws_tx.send(WsEvent::PipelineState {
                        state: predicted_state.to_string(),
                        event_id: last_event_id,
                        event_name: last_event_name.clone(),
                        buffer_progress: predicted_progress,
                        target_delay_secs: last_target_delay,
                        current_delay_secs: predicted_delay,
                        session_start: session_start_time.clone(),
                        predicted: true,
                    });
                    // Emit disconnect notice once (within first poll after failure)
                    if elapsed < 3.0 {
                        let _ = ws_tx.send(WsEvent::ActivityFeed {
                            timestamp: chrono::Utc::now().to_rfc3339(),
                            severity: "warning".to_string(),
                            message: format!(
                                "Delivery VPS unreachable — predicted buffer: {:.0}s",
                                predicted_delay
                            ),
                            source: "delivery".to_string(),
                        });
                    }
                    was_predicted = true;
                }
            }
        }
    }
}
