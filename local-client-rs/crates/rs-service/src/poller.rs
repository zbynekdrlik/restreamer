use std::time::Duration;

use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use rs_core::db;
use rs_core::models::WsEvent;
use rs_endpoint::manager_api::ManagerClient;

/// Polls the manager server every 5 seconds for active streaming events.
///
/// Behavior per response code:
/// - 200: Create/update streaming event, set receiving + delivering active
/// - 403: Set delivering_activated = false
/// - 404: Delete local streaming event
pub struct Poller {
    pool: SqlitePool,
    manager: ManagerClient,
    user_uuid: String,
    ws_tx: broadcast::Sender<WsEvent>,
    interval: Duration,
}

impl Poller {
    pub fn new(
        pool: SqlitePool,
        manager: ManagerClient,
        user_uuid: String,
        ws_tx: broadcast::Sender<WsEvent>,
    ) -> Self {
        Self {
            pool,
            manager,
            user_uuid,
            ws_tx,
            interval: Duration::from_secs(5),
        }
    }

    /// For testing: set a custom poll interval.
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Run the poller until shutdown.
    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) {
        info!("Poller started (interval: {:?})", self.interval);

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Poller shutting down");
                    break;
                }
                _ = tokio::time::sleep(self.interval) => {
                    self.poll_once().await;
                }
            }
        }
    }

    async fn poll_once(&self) {
        debug!("Polling manager for active stream");

        match self.manager.get_active_stream(&self.user_uuid).await {
            Ok(Some(stream)) => {
                let server_ip = stream.server_ip.as_deref().unwrap_or("");
                match db::upsert_streaming_event(
                    &self.pool,
                    &stream.identifier,
                    stream.short_description.as_deref(),
                    server_ip,
                )
                .await
                {
                    Ok(_) => {
                        info!("Active stream: {}", stream.identifier);
                        let _ = self.ws_tx.send(WsEvent::StreamingEvent {
                            action: "active".to_string(),
                            identifier: Some(stream.identifier),
                            receiving: true,
                            delivering: true,
                        });
                        let _ = self.ws_tx.send(WsEvent::ManagerPoll {
                            status_code: 200,
                            message: "active stream found".to_string(),
                        });
                    }
                    Err(e) => {
                        error!("Failed to upsert streaming event: {e}");
                    }
                }
            }
            Ok(None) => {
                // 404 — no active stream, delete local event
                if let Ok(Some(event)) = db::get_streaming_event(&self.pool).await {
                    if let Err(e) = db::delete_streaming_event(&self.pool, event.id).await {
                        error!("Failed to delete streaming event: {e}");
                    } else {
                        info!("Deleted local streaming event (manager 404)");
                    }
                }
                let _ = self.ws_tx.send(WsEvent::ManagerPoll {
                    status_code: 404,
                    message: "no active stream".to_string(),
                });
            }
            Err(rs_endpoint::EndpointError::ManagerForbidden) => {
                // 403 — disable delivering
                if let Ok(Some(event)) = db::get_streaming_event(&self.pool).await {
                    if let Err(e) = db::update_streaming_event_flags(
                        &self.pool,
                        event.id,
                        event.receiving_activated,
                        false,
                    )
                    .await
                    {
                        error!("Failed to update streaming event flags: {e}");
                    } else {
                        warn!("Delivering disabled (manager 403)");
                    }
                }
                let _ = self.ws_tx.send(WsEvent::ManagerPoll {
                    status_code: 403,
                    message: "delivering not authorized".to_string(),
                });
            }
            Err(e) => {
                error!("Manager poll failed: {e}");
                let _ = self.ws_tx.send(WsEvent::Error {
                    service: "poller".to_string(),
                    message: e.to_string(),
                });
            }
        }
    }
}
