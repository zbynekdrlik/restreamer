use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::EndpointError;

/// Client for communicating with the restreamer manager server.
pub struct ManagerClient {
    client: Client,
    base_url: String,
}

/// Notification sent to manager server when a chunk is uploaded to S3.
/// Field names match Django ChunkSerializer: chunk_id, chunk_identifier, chunk_size.
#[derive(Debug, Serialize)]
pub struct ChunkUploadNotification {
    pub chunk_id: i64,
    pub chunk_identifier: String,
    pub chunk_size: i64,
}

#[derive(Debug, Deserialize)]
pub struct ActiveStreamResponse {
    pub identifier: String,
    pub short_description: Option<String>,
    pub server_ip: Option<String>,
}

/// Response from manager server's check-chunk endpoint.
/// Field name matches Django ChunkExistsView: chunk_exists.
#[derive(Debug, Deserialize)]
pub struct CheckChunkResponse {
    pub chunk_exists: bool,
}

impl ManagerClient {
    pub fn new(base_url: &str) -> Result<Self, EndpointError> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|e| EndpointError::Manager(format!("failed to build HTTP client: {e}")))?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
        })
    }

    /// Poll for active streaming event.
    /// Returns Ok(Some(..)) on 200, Ok(None) on 404, Err on 403 or other errors.
    pub async fn get_active_stream(
        &self,
        user_uuid: &str,
    ) -> Result<Option<ActiveStreamResponse>, EndpointError> {
        let url = format!("{}/api/get_active_stream/", self.base_url);
        debug!("Polling manager: {url}");

        let response = self
            .client
            .get(&url)
            .query(&[("user_uuid", user_uuid)])
            .send()
            .await
            .map_err(|e| EndpointError::Manager(format!("failed to reach manager: {e}")))?;

        match response.status().as_u16() {
            200 => {
                let body = response
                    .json::<ActiveStreamResponse>()
                    .await
                    .map_err(|e| EndpointError::Manager(format!("invalid response: {e}")))?;
                info!("Active stream: {}", body.identifier);
                Ok(Some(body))
            }
            403 => {
                warn!("Manager returned 403 — delivering not authorized");
                Err(EndpointError::ManagerForbidden)
            }
            404 => {
                debug!("No active stream");
                Ok(None)
            }
            status => Err(EndpointError::Manager(format!(
                "unexpected status: {status}"
            ))),
        }
    }

    /// Notify manager that a chunk has been uploaded to S3.
    pub async fn notify_chunk_uploaded(
        &self,
        notification: &ChunkUploadNotification,
    ) -> Result<(), EndpointError> {
        let url = format!("{}/chunk-upload/", self.base_url);
        debug!("Notifying manager: {url}");

        let response = self
            .client
            .post(&url)
            .json(notification)
            .send()
            .await
            .map_err(|e| EndpointError::Manager(format!("notification failed: {e}")))?;

        if !response.status().is_success() {
            return Err(EndpointError::Manager(format!(
                "notification returned status {}",
                response.status()
            )));
        }

        Ok(())
    }

    /// Verify a chunk was received by the manager.
    /// Request fields match Django ChunkExistsView: se_identifier, chunk_id.
    pub async fn check_chunk(
        &self,
        event_identifier: &str,
        chunk_id: i64,
    ) -> Result<bool, EndpointError> {
        let url = format!("{}/api/check-chunk/", self.base_url);

        let response = self
            .client
            .post(&url)
            .json(&serde_json::json!({
                "se_identifier": event_identifier,
                "chunk_id": chunk_id,
            }))
            .send()
            .await
            .map_err(|e| EndpointError::Manager(format!("check-chunk failed: {e}")))?;

        if !response.status().is_success() {
            return Err(EndpointError::Manager(format!(
                "check-chunk returned status {}",
                response.status()
            )));
        }

        let body: CheckChunkResponse = response
            .json()
            .await
            .map_err(|e| EndpointError::Manager(format!("invalid check-chunk response: {e}")))?;

        Ok(body.chunk_exists)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use tokio::net::TcpListener;

    async fn start_mock_server(app: Router) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    #[test]
    fn manager_client_builds_urls() {
        let client = ManagerClient::new("https://restreamer.newlevel.media/").unwrap();
        assert_eq!(client.base_url, "https://restreamer.newlevel.media");

        let client = ManagerClient::new("https://restreamer.newlevel.media").unwrap();
        assert_eq!(client.base_url, "https://restreamer.newlevel.media");
    }

    #[test]
    fn chunk_upload_notification_serializes() {
        let notification = ChunkUploadNotification {
            chunk_id: 1,
            chunk_identifier: "evt-1".to_string(),
            chunk_size: 1024,
        };
        let json = serde_json::to_string(&notification).unwrap();
        assert!(json.contains("\"chunk_id\":1"));
        assert!(json.contains("\"chunk_identifier\":\"evt-1\""));
        assert!(json.contains("\"chunk_size\":1024"));
    }

    // --- Integration tests with mock HTTP server ---

    async fn mock_active_stream_200() -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "identifier": "test-stream-001",
            "short_description": "Sunday Service",
            "server_ip": "192.168.1.100"
        }))
    }

    async fn mock_active_stream_403() -> axum::http::StatusCode {
        axum::http::StatusCode::FORBIDDEN
    }

    async fn mock_active_stream_404() -> axum::http::StatusCode {
        axum::http::StatusCode::NOT_FOUND
    }

    #[tokio::test]
    async fn get_active_stream_200_returns_stream() {
        let app = Router::new().route("/api/get_active_stream/", get(mock_active_stream_200));
        let base_url = start_mock_server(app).await;
        let client = ManagerClient::new(&base_url).unwrap();

        let result = client.get_active_stream("test-uuid").await.unwrap();
        assert!(result.is_some());
        let stream = result.unwrap();
        assert_eq!(stream.identifier, "test-stream-001");
        assert_eq!(stream.short_description, Some("Sunday Service".to_string()));
        assert_eq!(stream.server_ip, Some("192.168.1.100".to_string()));
    }

    #[tokio::test]
    async fn get_active_stream_403_returns_forbidden() {
        let app = Router::new().route("/api/get_active_stream/", get(mock_active_stream_403));
        let base_url = start_mock_server(app).await;
        let client = ManagerClient::new(&base_url).unwrap();

        let result = client.get_active_stream("test-uuid").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            EndpointError::ManagerForbidden => {} // expected
            other => panic!("Expected ManagerForbidden, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn get_active_stream_404_returns_none() {
        let app = Router::new().route("/api/get_active_stream/", get(mock_active_stream_404));
        let base_url = start_mock_server(app).await;
        let client = ManagerClient::new(&base_url).unwrap();

        let result = client.get_active_stream("test-uuid").await.unwrap();
        assert!(result.is_none());
    }

    async fn mock_chunk_upload_ok() -> axum::http::StatusCode {
        axum::http::StatusCode::OK
    }

    async fn mock_chunk_upload_error() -> axum::http::StatusCode {
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    }

    #[tokio::test]
    async fn notify_chunk_uploaded_success() {
        let app = Router::new().route("/chunk-upload/", post(mock_chunk_upload_ok));
        let base_url = start_mock_server(app).await;
        let client = ManagerClient::new(&base_url).unwrap();

        let notification = ChunkUploadNotification {
            chunk_id: 1,
            chunk_identifier: "evt-1".to_string(),
            chunk_size: 1024,
        };
        let result = client.notify_chunk_uploaded(&notification).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn notify_chunk_uploaded_server_error() {
        let app = Router::new().route("/chunk-upload/", post(mock_chunk_upload_error));
        let base_url = start_mock_server(app).await;
        let client = ManagerClient::new(&base_url).unwrap();

        let notification = ChunkUploadNotification {
            chunk_id: 1,
            chunk_identifier: "evt-1".to_string(),
            chunk_size: 1024,
        };
        let result = client.notify_chunk_uploaded(&notification).await;
        assert!(result.is_err());
    }

    async fn mock_check_chunk_exists() -> Json<serde_json::Value> {
        Json(serde_json::json!({ "chunk_exists": true }))
    }

    async fn mock_check_chunk_not_exists() -> Json<serde_json::Value> {
        Json(serde_json::json!({ "chunk_exists": false }))
    }

    #[tokio::test]
    async fn check_chunk_exists() {
        let app = Router::new().route("/api/check-chunk/", post(mock_check_chunk_exists));
        let base_url = start_mock_server(app).await;
        let client = ManagerClient::new(&base_url).unwrap();

        let result = client.check_chunk("evt-1", 1).await;
        assert_eq!(result.unwrap(), true);
    }

    #[tokio::test]
    async fn check_chunk_not_exists() {
        let app = Router::new().route("/api/check-chunk/", post(mock_check_chunk_not_exists));
        let base_url = start_mock_server(app).await;
        let client = ManagerClient::new(&base_url).unwrap();

        let result = client.check_chunk("evt-1", 1).await;
        assert_eq!(result.unwrap(), false);
    }

    #[tokio::test]
    async fn get_active_stream_connection_refused() {
        // Point to a port that's not listening
        let client = ManagerClient::new("http://127.0.0.1:1").unwrap();
        let result = client.get_active_stream("test-uuid").await;
        assert!(result.is_err());
    }
}
