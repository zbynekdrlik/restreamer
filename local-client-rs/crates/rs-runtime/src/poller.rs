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
                    Ok(event_id) => {
                        // Clean up stale streaming events (Issue #23)
                        match db::delete_other_streaming_events(&self.pool, event_id).await {
                            Ok(0) => {}
                            Ok(n) => info!("Cleaned up {n} stale streaming event(s)"),
                            Err(e) => error!("Failed to clean up stale events: {e}"),
                        }

                        info!("Active stream: {}", stream.identifier);
                        if let Err(e) = self.ws_tx.send(WsEvent::StreamingEvent {
                            action: "active".to_string(),
                            identifier: Some(stream.identifier),
                            receiving: true,
                            delivering: true,
                        }) {
                            debug!("No WS subscribers for StreamingEvent: {e}");
                        }
                        if let Err(e) = self.ws_tx.send(WsEvent::ManagerPoll {
                            status_code: 200,
                            message: "active stream found".to_string(),
                        }) {
                            debug!("No WS subscribers for ManagerPoll: {e}");
                        }
                    }
                    Err(e) => {
                        error!("Failed to upsert streaming event: {e}");
                    }
                }
            }
            Ok(None) => {
                // 404 — no active stream, delete local event
                match db::get_streaming_event(&self.pool).await {
                    Ok(Some(event)) => {
                        if let Err(e) = db::delete_streaming_event(&self.pool, event.id).await {
                            error!("Failed to delete streaming event: {e}");
                        } else {
                            info!("Deleted local streaming event (manager 404)");
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        error!("Failed to query streaming event for 404 cleanup: {e}");
                    }
                }
                if let Err(e) = self.ws_tx.send(WsEvent::ManagerPoll {
                    status_code: 404,
                    message: "no active stream".to_string(),
                }) {
                    debug!("No WS subscribers for ManagerPoll: {e}");
                }
            }
            Err(rs_endpoint::EndpointError::ManagerForbidden) => {
                // 403 — disable delivering
                match db::get_streaming_event(&self.pool).await {
                    Ok(Some(event)) => {
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
                    Ok(None) => {}
                    Err(e) => {
                        error!("Failed to query streaming event for 403 update: {e}");
                    }
                }
                if let Err(e) = self.ws_tx.send(WsEvent::ManagerPoll {
                    status_code: 403,
                    message: "delivering not authorized".to_string(),
                }) {
                    debug!("No WS subscribers for ManagerPoll: {e}");
                }
            }
            Err(e) => {
                error!("Manager poll failed: {e}");
                if let Err(send_err) = self.ws_tx.send(WsEvent::Error {
                    service: "poller".to_string(),
                    message: e.to_string(),
                }) {
                    debug!("No WS subscribers for Error: {send_err}");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::{Json, Router};
    use rs_core::db;
    use sqlx::Row;
    use tokio::net::TcpListener;

    async fn start_mock_server(app: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    async fn setup_db() -> SqlitePool {
        let pool = db::create_pool(std::path::Path::new(":memory:"))
            .await
            .unwrap();
        db::run_migrations(&pool).await.unwrap();
        db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
        pool
    }

    async fn mock_200() -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "identifier": "stream-001",
            "short_description": "Test Stream",
            "server_ip": "10.0.0.1"
        }))
    }

    async fn mock_403() -> axum::http::StatusCode {
        axum::http::StatusCode::FORBIDDEN
    }

    async fn mock_404() -> axum::http::StatusCode {
        axum::http::StatusCode::NOT_FOUND
    }

    #[tokio::test]
    async fn poll_200_creates_streaming_event() {
        let app = Router::new().route("/api/get_active_stream/", get(mock_200));
        let base_url = start_mock_server(app).await;
        let pool = setup_db().await;
        let (ws_tx, mut ws_rx) = broadcast::channel::<WsEvent>(16);
        let manager = ManagerClient::new(&base_url).unwrap();

        let poller = Poller {
            pool: pool.clone(),
            manager,
            user_uuid: "test-uuid".to_string(),
            ws_tx,
            interval: Duration::from_millis(50),
        };
        poller.poll_once().await;

        // Verify streaming event was created in DB
        let event = db::get_streaming_event(&pool).await.unwrap();
        assert!(event.is_some());
        let event = event.unwrap();
        assert_eq!(event.identifier, Some("stream-001".to_string()));

        // Verify WebSocket events were sent
        let ws_event = ws_rx.recv().await.unwrap();
        match ws_event {
            WsEvent::StreamingEvent {
                action,
                identifier,
                receiving,
                delivering,
            } => {
                assert_eq!(action, "active");
                assert_eq!(identifier, Some("stream-001".to_string()));
                assert!(receiving);
                assert!(delivering);
            }
            other => panic!("Expected StreamingEvent, got: {other:?}"),
        }

        let ws_event = ws_rx.recv().await.unwrap();
        match ws_event {
            WsEvent::ManagerPoll {
                status_code,
                message,
            } => {
                assert_eq!(status_code, 200);
                assert!(message.contains("active"));
            }
            other => panic!("Expected ManagerPoll, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn poll_404_deletes_streaming_event() {
        let app = Router::new().route("/api/get_active_stream/", get(mock_404));
        let base_url = start_mock_server(app).await;
        let pool = setup_db().await;
        let (ws_tx, mut ws_rx) = broadcast::channel::<WsEvent>(16);
        let manager = ManagerClient::new(&base_url).unwrap();

        // First create a streaming event
        db::upsert_streaming_event(&pool, "old-stream", Some("Old"), "10.0.0.1")
            .await
            .unwrap();
        let before = db::get_streaming_event(&pool).await.unwrap();
        assert!(before.is_some());

        let poller = Poller {
            pool: pool.clone(),
            manager,
            user_uuid: "test-uuid".to_string(),
            ws_tx,
            interval: Duration::from_millis(50),
        };
        poller.poll_once().await;

        // Verify streaming event was deleted
        let after = db::get_streaming_event(&pool).await.unwrap();
        assert!(after.is_none());

        let ws_event = ws_rx.recv().await.unwrap();
        match ws_event {
            WsEvent::ManagerPoll { status_code, .. } => assert_eq!(status_code, 404),
            other => panic!("Expected ManagerPoll 404, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn poll_403_disables_delivering() {
        let app = Router::new().route("/api/get_active_stream/", get(mock_403));
        let base_url = start_mock_server(app).await;
        let pool = setup_db().await;
        let (ws_tx, mut ws_rx) = broadcast::channel::<WsEvent>(16);
        let manager = ManagerClient::new(&base_url).unwrap();

        // Create a streaming event with both flags active
        db::upsert_streaming_event(&pool, "stream-x", Some("Test"), "10.0.0.1")
            .await
            .unwrap();
        let event = db::get_streaming_event(&pool).await.unwrap().unwrap();
        // Enable both flags
        db::update_streaming_event_flags(&pool, event.id, true, true)
            .await
            .unwrap();

        let poller = Poller {
            pool: pool.clone(),
            manager,
            user_uuid: "test-uuid".to_string(),
            ws_tx,
            interval: Duration::from_millis(50),
        };
        poller.poll_once().await;

        // Verify delivering was disabled but receiving remains
        let event = db::get_streaming_event(&pool).await.unwrap().unwrap();
        assert!(event.receiving_activated);
        assert!(!event.delivering_activated);

        let ws_event = ws_rx.recv().await.unwrap();
        match ws_event {
            WsEvent::ManagerPoll { status_code, .. } => assert_eq!(status_code, 403),
            other => panic!("Expected ManagerPoll 403, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn poll_200_cleans_up_stale_events() {
        let app = Router::new().route("/api/get_active_stream/", get(mock_200));
        let base_url = start_mock_server(app).await;
        let pool = setup_db().await;
        let (ws_tx, _ws_rx) = broadcast::channel::<WsEvent>(16);
        let manager = ManagerClient::new(&base_url).unwrap();

        // Pre-populate a stale event that should be cleaned up
        let stale_id = db::upsert_streaming_event(&pool, "stale-event", Some("Stale"), "10.0.0.99")
            .await
            .unwrap();
        // Verify stale event exists
        assert!(
            db::get_streaming_event_by_id(&pool, stale_id)
                .await
                .unwrap()
                .is_some()
        );

        let poller = Poller {
            pool: pool.clone(),
            manager,
            user_uuid: "test-uuid".to_string(),
            ws_tx,
            interval: Duration::from_millis(50),
        };
        poller.poll_once().await;

        // After poll, only the new event (stream-001) should exist
        let event = db::get_streaming_event(&pool).await.unwrap().unwrap();
        assert_eq!(event.identifier, Some("stream-001".to_string()));

        // Stale event should be gone
        assert!(
            db::get_streaming_event_by_id(&pool, stale_id)
                .await
                .unwrap()
                .is_none()
        );

        // Count all events — should be exactly 1
        let count: i32 = sqlx::query("SELECT COUNT(*) as c FROM streaming_events")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get("c");
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn poll_connection_error_sends_error_event() {
        // Point to a port that's not listening
        let pool = setup_db().await;
        let (ws_tx, mut ws_rx) = broadcast::channel::<WsEvent>(16);
        let manager = ManagerClient::new("http://127.0.0.1:1").unwrap();

        let poller = Poller {
            pool: pool.clone(),
            manager,
            user_uuid: "test-uuid".to_string(),
            ws_tx,
            interval: Duration::from_millis(50),
        };
        poller.poll_once().await;

        let ws_event = ws_rx.recv().await.unwrap();
        match ws_event {
            WsEvent::Error { service, message } => {
                assert_eq!(service, "poller");
                assert!(!message.is_empty());
            }
            other => panic!("Expected Error event, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn poller_runs_and_shuts_down() {
        let app = Router::new().route("/api/get_active_stream/", get(mock_404));
        let base_url = start_mock_server(app).await;
        let pool = setup_db().await;
        let (ws_tx, _ws_rx) = broadcast::channel::<WsEvent>(16);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let manager = ManagerClient::new(&base_url).unwrap();

        let poller = Poller {
            pool: pool.clone(),
            manager,
            user_uuid: "test-uuid".to_string(),
            ws_tx,
            interval: Duration::from_millis(50),
        };
        let handle = tokio::spawn(async move { poller.run(shutdown_rx).await });

        // Let it poll at least once
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = shutdown_tx.send(());

        // Should complete without panic
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .unwrap()
            .unwrap();
    }
}
