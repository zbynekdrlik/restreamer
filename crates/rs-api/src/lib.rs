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
use rs_core::models::WsEvent;

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
                });
                prev_alive.clear();
                continue;
            }
        };

        match orch.poll_delivery_metrics(event.id).await {
            Ok((name, status, server_ip, endpoint_count, endpoints)) => {
                // Cache for instant HTTP retrieval
                if let Ok(mut c) = cached.write() {
                    *c = state::CachedDeliveryStatus {
                        instance_name: name.clone(),
                        status: status.clone(),
                        server_ip: server_ip.clone(),
                        endpoint_count,
                        endpoints: endpoints.clone(),
                    };
                }
                let _ = ws_tx.send(WsEvent::DeliveryStatus {
                    instance_name: name,
                    status,
                    server_ip,
                    endpoint_count,
                    endpoints: endpoints.clone(),
                });

                // Compute and broadcast PipelineState
                let any_alive = endpoints.iter().any(|m| m.alive);
                let state_str = if any_alive { "streaming" } else { "buffering" };

                let target_delay = event
                    .cache_delay_secs
                    .map(|s| s as u64)
                    .unwrap_or(config.delivery.delivery_delay_secs);

                let current_delay = endpoints
                    .iter()
                    .filter(|m| m.chunk_delay_secs > 0.0)
                    .map(|m| m.chunk_delay_secs)
                    .fold(f64::MAX, f64::min);
                let current_delay = if current_delay == f64::MAX {
                    0.0
                } else {
                    current_delay
                };

                let buffer_progress = if target_delay > 0 {
                    (current_delay / target_delay as f64).min(1.0)
                } else {
                    1.0
                };

                let _ = ws_tx.send(WsEvent::PipelineState {
                    state: state_str.to_string(),
                    event_id: Some(event.id),
                    event_name: Some(event.name.clone()),
                    buffer_progress,
                    target_delay_secs: target_delay,
                    current_delay_secs: current_delay,
                    session_start: None,
                });

                // Emit ActivityFeed for endpoint state transitions
                for ep in &endpoints {
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
            }
        }
    }
}
