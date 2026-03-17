pub mod delivery;
pub mod handlers;
pub mod router;
pub mod state;
pub mod websocket;

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
        tokio::spawn(async move {
            delivery_broadcast_loop(orch, pool, ws_tx, cached).await;
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
async fn delivery_broadcast_loop(
    orch: Arc<delivery::DeliveryOrchestrator>,
    pool: sqlx::SqlitePool,
    ws_tx: tokio::sync::broadcast::Sender<WsEvent>,
    cached: std::sync::Arc<std::sync::RwLock<state::CachedDeliveryStatus>>,
) {
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
                    endpoints,
                });
            }
            Err(e) => {
                tracing::debug!("Delivery metrics poll failed: {e}");
            }
        }
    }
}
