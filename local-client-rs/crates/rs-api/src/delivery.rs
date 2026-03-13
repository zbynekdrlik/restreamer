use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::SqlitePool;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use rs_cloud::hetzner::HetznerClient;
use rs_core::config::Config;
use rs_core::db;
use rs_core::models::DeliveryInstance;
use rs_youtube::oauth;
use rs_youtube::streams;

/// Orchestrates Hetzner VPS delivery instances and YouTube status checks.
///
/// Created only when Hetzner API token is configured. Manages the full lifecycle
/// of delivery VPS instances: create, health-poll, init endpoints, stop, delete.
pub struct DeliveryOrchestrator {
    pool: SqlitePool,
    config: Config,
    hetzner: HetznerClient,
    /// Tracks poll_and_init background tasks by instance ID for cancellation on stop.
    poll_handles: Arc<Mutex<HashMap<i64, JoinHandle<()>>>>,
}

/// Result of starting a delivery instance.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StartDeliveryResult {
    pub instance_id: i64,
    pub hetzner_id: i64,
    pub name: String,
    pub server_type: String,
    pub status: String,
    /// Auth token generated for this delivery instance (used for API auth).
    #[serde(skip)]
    pub auth_token: String,
}

/// Result of querying delivery status.
#[derive(Debug, serde::Serialize)]
pub struct DeliveryStatus {
    pub instance: Option<DeliveryInstance>,
    pub server_ready: bool,
    pub endpoints: Vec<EndpointDeliveryStatus>,
}

#[derive(Debug, serde::Serialize)]
pub struct EndpointDeliveryStatus {
    pub alias: String,
    pub alive: bool,
    pub buff_size_bytes: i64,
    pub current_chunk_id: i64,
}

/// Result of querying YouTube status.
#[derive(Debug, serde::Serialize)]
pub struct YouTubeStatus {
    pub authenticated: bool,
    pub stream_receiving: Option<bool>,
    pub error: Option<String>,
}

impl DeliveryOrchestrator {
    /// Access the database pool (e.g. for background error handling).
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub fn new(pool: SqlitePool, config: Config) -> Option<Self> {
        let token = &config.hetzner.api_token;
        if token.is_empty() {
            return None;
        }
        Some(Self {
            pool,
            hetzner: HetznerClient::new(token),
            config,
            poll_handles: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Create with a custom Hetzner base URL (for testing).
    pub fn with_base_url(pool: SqlitePool, config: Config, base_url: &str) -> Self {
        Self {
            pool,
            hetzner: HetznerClient::with_base_url(&config.hetzner.api_token, base_url),
            config,
            poll_handles: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns the poll_handles map for tracking background tasks.
    pub fn poll_handles(&self) -> Arc<Mutex<HashMap<i64, JoinHandle<()>>>> {
        Arc::clone(&self.poll_handles)
    }

    /// Start a delivery instance for the given event.
    ///
    /// Creates a Hetzner VPS, records it in the DB, polls until running,
    /// then POSTs /api/init to the rs-delivery binary on the VPS.
    pub async fn start_delivery(&self, event_id: i64) -> anyhow::Result<StartDeliveryResult> {
        // Check for existing active instance
        if let Some(existing) = db::get_delivery_instance_by_event(&self.pool, event_id).await? {
            if existing.status != "deleted" {
                return Ok(StartDeliveryResult {
                    instance_id: existing.id,
                    hetzner_id: existing.hetzner_id,
                    name: existing.name,
                    server_type: existing.server_type,
                    status: existing.status,
                    auth_token: String::new(),
                });
            }
        }

        // Get event endpoints to determine server size
        let endpoints = db::get_event_endpoints(&self.pool, event_id).await?;
        let server_type = rs_cloud::select_server_type(endpoints.len());

        let name = format!("rs-delivery-evt{event_id}");
        let binary_url = format!(
            "{}/{}/rs-delivery",
            self.config.s3.endpoint, self.config.s3.bucket,
        );

        let mut labels = HashMap::new();
        labels.insert("app".to_string(), "restreamer".to_string());
        labels.insert("event_id".to_string(), event_id.to_string());

        // S3 credentials are passed via cloud-init (written to env file on disk)
        // so they never travel over plaintext HTTP to the delivery VPS.
        let s3_creds = rs_cloud::DeliveryS3Credentials {
            bucket: self.config.s3.bucket.clone(),
            region: self.config.s3.region.clone(),
            endpoint: self.config.s3.endpoint.clone(),
            access_key_id: self.config.s3.access_key_id.clone(),
            secret_access_key: self.config.s3.secret_access_key.clone(),
        };

        // Generate a random auth token for this delivery instance (cross-platform)
        let auth_token = uuid::Uuid::new_v4().to_string().replace('-', "");

        // Find the snapshot or fall back to bootstrap cloud-init
        // Both paths download the latest binary from S3 to ensure newest version runs
        let (image, user_data) = match self.find_delivery_image().await {
            Ok(snapshot_id) => (
                snapshot_id,
                rs_cloud::snapshot_cloud_init(&binary_url, &s3_creds, &auth_token),
            ),
            Err(_) => {
                info!(
                    "No delivery snapshot found, bootstrapping from {}",
                    binary_url
                );
                (
                    "ubuntu-24.04".to_string(),
                    rs_cloud::bootstrap_cloud_init(&binary_url, &s3_creds, &auth_token),
                )
            }
        };

        let server = self
            .hetzner
            .create_server(
                &name,
                server_type,
                &self.config.hetzner.location,
                &image,
                std::slice::from_ref(&self.config.hetzner.ssh_key_name),
                &user_data,
                labels,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Hetzner create_server failed: {e}"))?;

        let ipv4 = server.public_net.ipv4.ip.clone();
        let instance_id = db::create_delivery_instance(
            &self.pool,
            server.id,
            &name,
            &ipv4,
            server_type,
            Some(event_id),
        )
        .await?;

        info!(
            hetzner_id = server.id,
            instance_id,
            ipv4 = %ipv4,
            "Created delivery instance"
        );

        Ok(StartDeliveryResult {
            instance_id,
            hetzner_id: server.id,
            name,
            server_type: server_type.to_string(),
            status: "creating".to_string(),
            auth_token,
        })
    }

    /// Poll the delivery server for readiness and init endpoints once ready.
    pub async fn poll_and_init(
        &self,
        instance_id: i64,
        event_id: i64,
        event_name: &str,
        auth_token: &str,
    ) -> anyhow::Result<()> {
        let instance = db::get_delivery_instance(&self.pool, instance_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("delivery instance {instance_id} not found"))?;

        // Poll Hetzner until server is running
        let hetzner_id = instance.hetzner_id;
        for attempt in 0..60 {
            let server = self
                .hetzner
                .get_server(hetzner_id)
                .await
                .map_err(|e| anyhow::anyhow!("get_server failed: {e}"))?;

            if server.status == "running" {
                let ipv4 = server.public_net.ipv4.ip.clone();
                db::update_delivery_instance_status(&self.pool, instance_id, "running").await?;
                info!(hetzner_id, ipv4 = %ipv4, "Delivery server is running");
                break;
            }

            if attempt == 59 {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for server {hetzner_id} to start"
                ));
            }

            tokio::time::sleep(Duration::from_secs(5)).await;
        }

        // Wait for rs-delivery HTTP to be ready
        let instance = db::get_delivery_instance(&self.pool, instance_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("instance disappeared"))?;

        let delivery_url = format!("http://{}:8000", instance.ipv4);
        let client = reqwest::Client::new();

        // Wait for rs-delivery to become ready (cloud-init can take several minutes)
        for attempt in 0..60 {
            match client
                .get(format!("{delivery_url}/api/health"))
                .timeout(Duration::from_secs(5))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    info!(
                        attempt,
                        "rs-delivery health check passed on {}", instance.ipv4
                    );
                    break;
                }
                Ok(resp) => {
                    if attempt % 6 == 0 {
                        info!(attempt, status = %resp.status(), "Health check returned non-OK");
                    }
                    if attempt == 59 {
                        return Err(anyhow::anyhow!(
                            "rs-delivery returned {} after {} attempts",
                            resp.status(),
                            attempt + 1
                        ));
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(e) => {
                    if attempt % 6 == 0 {
                        info!(attempt, error = %e, "Health check connection failed");
                    }
                    if attempt == 59 {
                        return Err(anyhow::anyhow!(
                            "Timeout waiting for rs-delivery on {}: {}",
                            instance.ipv4,
                            e
                        ));
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }

        // POST /api/init to configure endpoints.
        // S3 credentials are already on the VPS via cloud-init env file.
        // Query chunk ID, retrying briefly in case chunks haven't been committed yet
        let mut start_chunk_id = None;
        for attempt in 0..10 {
            match db::get_first_chunk_id_for_event(&self.pool, event_id).await {
                Ok(Some(id)) => {
                    start_chunk_id = Some(id);
                    break;
                }
                Ok(None) => {
                    info!(
                        event_id,
                        attempt, "No chunks found for event yet, retrying..."
                    );
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
                Err(e) => {
                    warn!(event_id, "Failed to query chunk ID: {e}");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
        let start_chunk_id = start_chunk_id
            .ok_or_else(|| anyhow::anyhow!("No chunks found for event {event_id} after 30s"))?;
        info!(event_id, start_chunk_id, "Starting delivery from chunk");

        let endpoints = db::get_event_endpoints(&self.pool, event_id).await?;
        let init_body = serde_json::json!({
            "endpoints": endpoints.iter().map(|ep| {
                serde_json::json!({
                    "alias": ep.alias,
                    "service_type": ep.service_type,
                    "stream_key": ep.stream_key,
                })
            }).collect::<Vec<_>>(),
            "s3_config": {
                "bucket": self.config.s3.bucket,
                "region": self.config.s3.region,
                "endpoint": self.config.s3.endpoint,
                "access_key_id": "from-env",
                "secret_access_key": "from-env",
            },
            "event_identifier": event_name,
            "start_chunk_id": start_chunk_id,
        });

        let resp = client
            .post(format!("{delivery_url}/api/init"))
            .bearer_auth(auth_token)
            .json(&init_body)
            .timeout(Duration::from_secs(30))
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "rs-delivery /api/init failed: {status} - {body}"
            ));
        }

        let init_resp = resp.text().await.unwrap_or_default();
        info!(event_id, init_resp = %init_resp, "Init response received");

        db::update_delivery_instance_health(&self.pool, instance_id).await?;
        info!(event_id, "Delivery endpoints initialized successfully");

        Ok(())
    }

    /// Get delivery status for an event.
    pub async fn get_delivery_status(&self, event_id: i64) -> anyhow::Result<DeliveryStatus> {
        let instance = db::get_delivery_instance_by_event(&self.pool, event_id).await?;

        let (server_ready, endpoints) = match &instance {
            Some(inst) if inst.status == "running" => {
                // Fetch live status from rs-delivery
                let delivery_url = format!("http://{}:8000", inst.ipv4);
                let client = reqwest::Client::new();

                match client
                    .get(format!("{delivery_url}/api/status"))
                    .timeout(Duration::from_secs(10))
                    .send()
                    .await
                {
                    Ok(resp) if resp.status().is_success() => {
                        let body: serde_json::Value = resp.json().await.unwrap_or_default();
                        let ep_entries = body["endpoints"].as_array().cloned().unwrap_or_default();

                        let mut statuses = Vec::new();
                        for entry in ep_entries {
                            let alias = entry["alias"].as_str().unwrap_or("").to_string();
                            let alive = entry["alive"].as_bool().unwrap_or(false);
                            let buff = entry["buff_size_bytes"].as_i64().unwrap_or(0);
                            let chunk_id = entry["current_chunk_id"].as_i64().unwrap_or(0);

                            // Update DB with latest status
                            if let Err(e) = db::upsert_delivery_endpoint_status(
                                &self.pool, inst.id, &alias, alive, buff, chunk_id,
                            )
                            .await
                            {
                                warn!("Failed to update endpoint status: {e}");
                            }

                            statuses.push(EndpointDeliveryStatus {
                                alias,
                                alive,
                                buff_size_bytes: buff,
                                current_chunk_id: chunk_id,
                            });
                        }

                        db::update_delivery_instance_health(&self.pool, inst.id)
                            .await
                            .ok();

                        (true, statuses)
                    }
                    Ok(resp) => {
                        warn!(
                            status = %resp.status(),
                            "Delivery status check returned non-success"
                        );
                        (false, Vec::new())
                    }
                    Err(e) => {
                        warn!("Delivery status check failed: {e}");
                        (false, Vec::new())
                    }
                }
            }
            Some(inst) => {
                info!(
                    status = %inst.status,
                    "Delivery instance not in running state"
                );
                (false, Vec::new())
            }
            _ => (false, Vec::new()),
        };

        Ok(DeliveryStatus {
            instance,
            server_ready,
            endpoints,
        })
    }

    /// Stop delivery for an event: POST /api/stop, then delete Hetzner server.
    pub async fn stop_delivery(&self, event_id: i64) -> anyhow::Result<()> {
        let instance = db::get_delivery_instance_by_event(&self.pool, event_id).await?;
        let instance = match instance {
            Some(i) => i,
            None => return Ok(()),
        };

        // Abort any running poll_and_init background task for this instance
        if let Some(handle) = self.poll_handles.lock().await.remove(&instance.id) {
            handle.abort();
            info!(
                instance_id = instance.id,
                "Aborted poll_and_init background task"
            );
        }

        db::update_delivery_instance_status(&self.pool, instance.id, "stopping").await?;

        // Best-effort: tell rs-delivery to stop endpoints
        if instance.status == "running" {
            let client = reqwest::Client::new();
            let delivery_url = format!("http://{}:8000", instance.ipv4);
            let _ = client
                .post(format!("{delivery_url}/api/stop"))
                .json(&serde_json::json!({"alias": null}))
                .timeout(Duration::from_secs(10))
                .send()
                .await;
        }

        // Delete Hetzner server
        if let Err(e) = self.hetzner.delete_server(instance.hetzner_id).await {
            error!(
                hetzner_id = instance.hetzner_id,
                "Failed to delete Hetzner server: {e}"
            );
        }

        db::update_delivery_instance_status(&self.pool, instance.id, "deleted").await?;
        info!(
            hetzner_id = instance.hetzner_id,
            event_id, "Delivery instance stopped and deleted"
        );

        Ok(())
    }

    /// Check YouTube stream receiving status using stored OAuth tokens.
    pub async fn check_youtube_status(&self) -> YouTubeStatus {
        let tokens = match db::get_youtube_oauth(&self.pool).await {
            Ok(Some(t)) => t,
            Ok(None) => {
                return YouTubeStatus {
                    authenticated: false,
                    stream_receiving: None,
                    error: Some("No YouTube OAuth tokens configured".to_string()),
                };
            }
            Err(e) => {
                return YouTubeStatus {
                    authenticated: false,
                    stream_receiving: None,
                    error: Some(format!("DB error: {e}")),
                };
            }
        };

        // Check if token needs refresh
        let access_token = if oauth::is_token_expired(tokens.expires_at.as_deref()) {
            let oauth_tokens = rs_youtube::OAuthTokens {
                access_token: tokens.access_token.clone(),
                refresh_token: tokens.refresh_token.clone(),
                token_uri: tokens.token_uri.clone(),
                client_id: tokens.client_id.clone(),
                client_secret: tokens.client_secret.clone(),
                scopes: tokens.scopes.clone(),
                expires_at: tokens.expires_at.clone(),
            };

            match oauth::refresh_access_token(&oauth_tokens).await {
                Ok(resp) => {
                    let new_expires = resp.expires_in.map(|secs| {
                        (chrono::Utc::now() + chrono::Duration::seconds(secs as i64)).to_rfc3339()
                    });

                    if let Err(e) = db::upsert_youtube_oauth(
                        &self.pool,
                        &resp.access_token,
                        resp.refresh_token
                            .as_deref()
                            .unwrap_or(&tokens.refresh_token),
                        &tokens.token_uri,
                        &tokens.client_id,
                        &tokens.client_secret,
                        &tokens.scopes,
                        new_expires.as_deref(),
                    )
                    .await
                    {
                        warn!("Failed to save refreshed token: {e}");
                    }

                    resp.access_token
                }
                Err(e) => {
                    return YouTubeStatus {
                        authenticated: true,
                        stream_receiving: None,
                        error: Some(format!("Token refresh failed: {e}")),
                    };
                }
            }
        } else {
            tokens.access_token.clone()
        };

        match streams::is_stream_receiving(&access_token).await {
            Ok(receiving) => YouTubeStatus {
                authenticated: true,
                stream_receiving: Some(receiving),
                error: None,
            },
            Err(e) => YouTubeStatus {
                authenticated: true,
                stream_receiving: None,
                error: Some(format!("YouTube API error: {e}")),
            },
        }
    }

    async fn find_delivery_image(&self) -> anyhow::Result<String> {
        let label = &self.config.hetzner.snapshot_label;
        let snapshots = self
            .hetzner
            .list_snapshots(Some(&format!("app={label}")))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to list snapshots: {e}"))?;

        if snapshots.is_empty() {
            return Err(anyhow::anyhow!("No snapshot with label app={label} found"));
        }

        let latest = snapshots.last().unwrap();
        Ok(latest.id.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn orchestrator_none_without_token() {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let config = Config::for_testing();
        assert!(DeliveryOrchestrator::new(pool, config).is_none());
    }

    #[tokio::test]
    async fn orchestrator_some_with_token() {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let mut config = Config::for_testing();
        config.hetzner.api_token = "test-token".to_string();
        assert!(DeliveryOrchestrator::new(pool, config).is_some());
    }

    #[tokio::test]
    async fn youtube_status_no_tokens() {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let mut config = Config::for_testing();
        config.hetzner.api_token = "test-token".to_string();
        let orch = DeliveryOrchestrator::new(pool, config).unwrap();

        let status = orch.check_youtube_status().await;
        assert!(!status.authenticated);
        assert!(status.error.is_some());
    }

    #[tokio::test]
    async fn stop_delivery_noop_when_no_instance() {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let mut config = Config::for_testing();
        config.hetzner.api_token = "test-token".to_string();
        let orch = DeliveryOrchestrator::new(pool, config).unwrap();

        // Should not error when no instance exists
        orch.stop_delivery(999).await.unwrap();
    }

    #[tokio::test]
    async fn get_delivery_status_no_instance() {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        let mut config = Config::for_testing();
        config.hetzner.api_token = "test-token".to_string();
        let orch = DeliveryOrchestrator::new(pool, config).unwrap();

        let status = orch.get_delivery_status(999).await.unwrap();
        assert!(status.instance.is_none());
        assert!(!status.server_ready);
        assert!(status.endpoints.is_empty());
    }
}
