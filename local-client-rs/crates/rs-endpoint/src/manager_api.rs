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

#[derive(Debug, Serialize)]
pub struct ChunkUploadNotification {
    pub event_identifier: String,
    pub chunk_filename: String,
    pub data_size: i64,
    pub md5: String,
}

#[derive(Debug, Deserialize)]
pub struct ActiveStreamResponse {
    pub identifier: String,
    pub short_description: Option<String>,
    pub server_ip: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CheckChunkResponse {
    pub verified: bool,
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
    pub async fn check_chunk(
        &self,
        event_identifier: &str,
        chunk_filename: &str,
    ) -> Result<bool, EndpointError> {
        let url = format!("{}/api/check-chunk/", self.base_url);

        let response = self
            .client
            .post(&url)
            .json(&serde_json::json!({
                "event_identifier": event_identifier,
                "chunk_filename": chunk_filename,
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

        Ok(body.verified)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            event_identifier: "evt-1".to_string(),
            chunk_filename: "chunk_000001.bin".to_string(),
            data_size: 1024,
            md5: "abc123".to_string(),
        };
        let json = serde_json::to_string(&notification).unwrap();
        assert!(json.contains("evt-1"));
        assert!(json.contains("chunk_000001.bin"));
    }
}
