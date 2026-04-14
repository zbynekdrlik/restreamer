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
use rs_core::models::{DeliveryEndpointMetrics, DeliveryInstance};

/// Returns true if the DB-side status represents a live delivery instance
/// that we can talk to over HTTP. The orchestrator transitions instances
/// through `creating → booting → initializing → delivering → stopping →
/// deleted` (plus `failed` on error). The post-boot states all have rs-delivery
/// listening on :8000; before boot we have no IP, and after stopping/deleted
/// the VPS is gone. We keep `running` in the match for backwards-compatibility
/// with older rows that predate the fine-grained status states.
pub(crate) fn is_delivery_active(status: &str) -> bool {
    matches!(
        status,
        "booting" | "initializing" | "delivering" | "running"
    )
}

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
    /// Cached endpoint configs per event_id, populated at init time.
    /// Key: event_id, Value: HashMap<alias, is_fast>
    endpoint_fast_cache: Arc<Mutex<HashMap<i64, HashMap<String, bool>>>>,
    /// Resume positions for auto-restart after VPS crash.
    /// Key: event_id, Value: HashMap<alias, last_known_chunk_id>
    resume_positions: Arc<Mutex<HashMap<i64, HashMap<String, i64>>>>,
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EndpointRestartRecord {
    pub timestamp_ms: i64,
    pub chunk_id: i64,
    pub lifetime_secs: u64,
    pub reason: String,
    pub stderr_tail: Option<String>,
    pub backoff_secs: u64,
}

#[derive(Debug, serde::Serialize)]
pub struct EndpointDeliveryStatus {
    pub alias: String,
    pub alive: bool,
    pub current_chunk_id: i64,
    pub bytes_processed_total: i64,
    pub chunks_processed: i64,
    pub chunk_delay_secs: f64,
    pub stall_reason: Option<String>,
    pub ffmpeg_restart_count: u32,
    pub last_error: Option<String>,
    pub ffmpeg_last_stderr: Option<String>,
    pub is_fast: bool,
    /// Per-endpoint audit log of recent ffmpeg restarts (capped at 100).
    /// Empty when the rs-delivery binary on the VPS is older than this
    /// field's introduction.
    #[serde(default)]
    pub restart_history: Vec<EndpointRestartRecord>,
    /// Current delivery mode: "normal", "warmup", "rescue", or "recovering".
    /// None when the rs-delivery binary on the VPS is older than the
    /// rescue-mode feature.
    #[serde(default)]
    pub delivery_mode: Option<String>,
    /// ETA in seconds until rescue mode exits. None when not in rescue mode.
    #[serde(default)]
    pub rescue_eta_secs: Option<u64>,
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
            endpoint_fast_cache: Arc::new(Mutex::new(HashMap::new())),
            resume_positions: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Create with a custom Hetzner base URL (for testing).
    pub fn with_base_url(pool: SqlitePool, config: Config, base_url: &str) -> Self {
        Self {
            pool,
            hetzner: HetznerClient::with_base_url(&config.hetzner.api_token, base_url),
            config,
            poll_handles: Arc::new(Mutex::new(HashMap::new())),
            endpoint_fast_cache: Arc::new(Mutex::new(HashMap::new())),
            resume_positions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns the poll_handles map for tracking background tasks.
    pub fn poll_handles(&self) -> Arc<Mutex<HashMap<i64, JoinHandle<()>>>> {
        Arc::clone(&self.poll_handles)
    }

    /// Update fast cache for a single endpoint (used by mid-stream add).
    pub async fn update_endpoint_fast_cache(&self, event_id: i64, alias: &str, is_fast: bool) {
        let mut cache = self.endpoint_fast_cache.lock().await;
        cache
            .entry(event_id)
            .or_default()
            .insert(alias.to_string(), is_fast);
    }

    /// Remove an endpoint from the fast cache (used by mid-stream remove).
    pub async fn remove_endpoint_from_fast_cache(&self, event_id: i64, alias: &str) {
        let mut cache = self.endpoint_fast_cache.lock().await;
        if let Some(map) = cache.get_mut(&event_id) {
            map.remove(alias);
        }
    }

    /// Returns a reference to the config.
    pub fn config(&self) -> &Config {
        &self.config
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
            &auth_token,
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

        // Poll Hetzner until server is running. Poll every 1s (not 5s)
        // so we detect "running" as soon as possible — avoids wasting
        // up to 5s of user-visible warmup time.
        let hetzner_id = instance.hetzner_id;
        for attempt in 0..300 {
            let server = self
                .hetzner
                .get_server(hetzner_id)
                .await
                .map_err(|e| anyhow::anyhow!("get_server failed: {e}"))?;

            if server.status == "running" {
                let ipv4 = server.public_net.ipv4.ip.clone();
                // Hetzner says VM is running, but rs-delivery service is not
                // yet ready (cloud-init still downloading the binary and
                // starting the service). Use "booting" so the dashboard
                // shows the correct phase to the operator — they were
                // confused that "creating" jumped to "running" instantly
                // but actual readiness took 60+ more seconds.
                db::update_delivery_instance_status(&self.pool, instance_id, "booting").await?;
                info!(hetzner_id, ipv4 = %ipv4, "VPS booted, waiting for rs-delivery");
                break;
            }

            if attempt == 299 {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for server {hetzner_id} to start"
                ));
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // Wait for rs-delivery HTTP to be ready
        let instance = db::get_delivery_instance(&self.pool, instance_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("instance disappeared"))?;

        let delivery_url = format!("http://{}:8000", instance.ipv4);
        let client = reqwest::Client::new();

        // Wait for rs-delivery to become ready. Poll every 1s so the
        // moment cloud-init finishes we detect it — cuts up to 5s off
        // user-visible warmup time.
        for attempt in 0..300 {
            match client
                .get(format!("{delivery_url}/api/health"))
                .timeout(Duration::from_secs(3))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    info!(
                        attempt,
                        "rs-delivery health check passed on {}", instance.ipv4
                    );
                    // rs-delivery is healthy. Move to "initializing" phase
                    // while we wait for buffer-fill and call /api/init —
                    // operator can see this is normal startup, not a hang.
                    db::update_delivery_instance_status(&self.pool, instance_id, "initializing")
                        .await?;
                    break;
                }
                Ok(resp) => {
                    if attempt % 30 == 0 {
                        info!(attempt, status = %resp.status(), "Health check returned non-OK");
                    }
                    if attempt == 299 {
                        return Err(anyhow::anyhow!(
                            "rs-delivery returned {} after {} attempts",
                            resp.status(),
                            attempt + 1
                        ));
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Err(e) => {
                    if attempt % 30 == 0 {
                        info!(attempt, error = %e, "Health check connection failed");
                    }
                    if attempt == 299 {
                        return Err(anyhow::anyhow!(
                            "Timeout waiting for rs-delivery on {}: {}",
                            instance.ipv4,
                            e
                        ));
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        // POST /api/init to configure endpoints.
        // S3 credentials are already on the VPS via cloud-init env file.

        let endpoints = db::get_event_endpoints(&self.pool, event_id).await?;

        // Cache is_fast per alias for this event
        // (avoids re-querying in get_delivery_status)
        let fast_map: HashMap<String, bool> = endpoints
            .iter()
            .map(|ep| (ep.alias.clone(), ep.is_fast))
            .collect();
        self.endpoint_fast_cache
            .lock()
            .await
            .insert(event_id, fast_map);

        // Resolve effective cache delay
        let event = db::get_streaming_event_by_id(&self.pool, event_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Event {event_id} not found"))?;
        let delay_secs = event
            .cache_delay_secs
            .map(|s| s as u64)
            .unwrap_or(self.config.delivery.delivery_delay_secs);
        let target_delay_ms = (delay_secs * 1000) as i64;

        // Check if we have resume positions from a crash recovery
        let resume_pos = self.resume_positions.lock().await.remove(&event_id);
        let is_resume = resume_pos.is_some();

        let start_chunk_id;
        if is_resume {
            // For resume: skip chunk-wait phase, chunks already exist from prior session
            let first_seq = db::get_first_sequence_number_for_event(&self.pool, event_id)
                .await?
                .unwrap_or(1);
            start_chunk_id = first_seq;
            info!(
                event_id,
                start_chunk_id, "Resuming delivery after crash recovery"
            );
        } else {
            // When rescue_video_url is configured, skip the full cache-fill
            // pre-wait and initialize the VPS as soon as the first chunk
            // exists. The VPS-side endpoint_loop handles warmup by playing
            // the rescue video while its own buffer fills. Without this,
            // viewers see nothing during the initial 120s cache-fill.
            //
            // When rescue_video_url is None, wait for the full target delay
            // (legacy behaviour) — nothing can play to viewers anyway.
            let has_rescue_video = event.rescue_video_url.is_some();
            let wait_target_ms = if has_rescue_video {
                1 // First chunk is enough; VPS plays rescue video during its own fill
            } else {
                target_delay_ms
            };

            let max_wait_secs = 900;
            for attempt in 0..max_wait_secs {
                let sent_ms = db::get_sent_duration_ms(&self.pool, event_id)
                    .await
                    .unwrap_or(0);
                if sent_ms >= wait_target_ms {
                    info!(
                        event_id,
                        sent_ms,
                        wait_target_ms,
                        has_rescue_video,
                        "Sent content duration meets target (init VPS)"
                    );
                    break;
                }
                if attempt % 10 == 0 {
                    info!(
                        event_id,
                        sent_ms,
                        wait_target_ms,
                        attempt,
                        "Waiting for sent duration ({sent_ms}ms / {wait_target_ms}ms)"
                    );
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            let first_seq_val = db::get_first_sequence_number_for_event(&self.pool, event_id)
                .await?
                .ok_or_else(|| {
                    anyhow::anyhow!("No chunks found for event {event_id} after wait")
                })?;
            start_chunk_id = first_seq_val;
            info!(
                event_id,
                start_chunk_id, "Starting delivery from first chunk"
            );
        }

        let chunk_format = &self.config.inpoint.chunk_format;
        let init_body = serde_json::json!({
            "endpoints": endpoints.iter().map(|ep| {
                // Use per-endpoint resume position if available
                let ep_start = resume_pos.as_ref()
                    .and_then(|rp| rp.get(&ep.alias).copied())
                    .unwrap_or(start_chunk_id);
                serde_json::json!({
                    "alias": ep.alias,
                    "service_type": ep.service_type,
                    "stream_key": ep.stream_key,
                    "is_fast": ep.is_fast,
                    "chunk_format": chunk_format,
                    "start_chunk_id": ep_start,
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
            "delivery_delay_ms": target_delay_ms,
            "rescue_video_url": event.rescue_video_url,
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
        // Init succeeded — endpoints are now warming up / delivering.
        // The dashboard reads this status to show "Delivering" instead
        // of "Initializing".
        db::update_delivery_instance_status(&self.pool, instance_id, "delivering").await?;
        info!(event_id, "Delivery endpoints initialized successfully");

        Ok(())
    }

    /// Get delivery status for an event.
    pub async fn get_delivery_status(&self, event_id: i64) -> anyhow::Result<DeliveryStatus> {
        let instance = db::get_delivery_instance_by_event(&self.pool, event_id).await?;

        // Read cached is_fast map (populated in init_endpoints, empty before init)
        let fast_map = {
            let cache = self.endpoint_fast_cache.lock().await;
            cache.get(&event_id).cloned().unwrap_or_default()
        };

        let (server_ready, endpoints) = match &instance {
            Some(inst) if is_delivery_active(&inst.status) => {
                // Fetch live status from rs-delivery
                let delivery_url = format!("http://{}:8000", inst.ipv4);
                let client = reqwest::Client::new();

                match client
                    .get(format!("{delivery_url}/api/status"))
                    .bearer_auth(&inst.auth_token)
                    .timeout(Duration::from_secs(5))
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
                            let chunk_id = entry["current_chunk_id"].as_i64().unwrap_or(0);
                            let bytes_total = entry["bytes_processed_total"].as_i64().unwrap_or(0);
                            let chunks_processed = entry["chunks_processed"].as_i64().unwrap_or(0);
                            let stall_reason =
                                entry["stall_reason"].as_str().map(|s| s.to_string());
                            let ffmpeg_restart_count =
                                entry["ffmpeg_restart_count"].as_u64().unwrap_or(0) as u32;
                            let last_error = entry["last_error"].as_str().map(|s| s.to_string());
                            let ffmpeg_last_stderr =
                                entry["ffmpeg_last_stderr"].as_str().map(|s| s.to_string());
                            let delivery_mode =
                                entry["delivery_mode"].as_str().map(|s| s.to_string());
                            let rescue_eta_secs = entry["rescue_eta_secs"].as_u64();
                            let restart_history: Vec<EndpointRestartRecord> =
                                entry["restart_history"]
                                    .as_array()
                                    .map(|arr| {
                                        arr.iter()
                                            .filter_map(|v| {
                                                serde_json::from_value::<EndpointRestartRecord>(
                                                    v.clone(),
                                                )
                                                .ok()
                                            })
                                            .collect()
                                    })
                                    .unwrap_or_default();

                            // Persist restart records to DB for post-mortem analysis.
                            // The dedup INSERT ignores records already saved from previous polls.
                            for record in &restart_history {
                                if let Err(e) = db::insert_delivery_restart_record(
                                    &self.pool,
                                    inst.id,
                                    inst.event_id,
                                    &alias,
                                    record.timestamp_ms,
                                    record.chunk_id,
                                    record.lifetime_secs as i64,
                                    &record.reason,
                                    record.stderr_tail.as_deref(),
                                    record.backoff_secs as i64,
                                )
                                .await
                                {
                                    warn!("Failed to persist restart record: {e}");
                                }
                            }

                            // Compute cache delay using actual content duration from DB
                            let chunk_delay_secs =
                                db::get_cache_duration_secs(&self.pool, event_id, chunk_id)
                                    .await
                                    .unwrap_or(0.0);

                            // Update DB with latest status
                            if let Err(e) = db::upsert_delivery_endpoint_status(
                                &self.pool,
                                inst.id,
                                &alias,
                                alive,
                                chunks_processed,
                                chunk_id,
                                bytes_total,
                            )
                            .await
                            {
                                warn!("Failed to update endpoint status: {e}");
                            }

                            statuses.push(EndpointDeliveryStatus {
                                alias: alias.clone(),
                                alive,
                                current_chunk_id: chunk_id,
                                bytes_processed_total: bytes_total,
                                chunks_processed,
                                chunk_delay_secs,
                                stall_reason,
                                ffmpeg_restart_count,
                                last_error,
                                ffmpeg_last_stderr,
                                is_fast: fast_map.get(&alias).copied().unwrap_or(false),
                                restart_history,
                                delivery_mode,
                                rescue_eta_secs,
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

    /// Poll delivery metrics and return data suitable for WsEvent broadcast.
    /// Returns (instance_name, status, server_ip, endpoint_count, Vec<DeliveryEndpointMetrics>).
    pub async fn poll_delivery_metrics(
        &self,
        event_id: i64,
    ) -> anyhow::Result<(
        String,
        String,
        Option<String>,
        u32,
        Vec<DeliveryEndpointMetrics>,
    )> {
        let status = self.get_delivery_status(event_id).await?;

        let (name, inst_status, server_ip) = match &status.instance {
            Some(inst) => (
                inst.name.clone(),
                inst.status.clone(),
                Some(inst.ipv4.clone()),
            ),
            None => ("none".to_string(), "none".to_string(), None),
        };

        let metrics: Vec<DeliveryEndpointMetrics> = status
            .endpoints
            .into_iter()
            .map(|ep| DeliveryEndpointMetrics {
                alias: ep.alias,
                alive: ep.alive,
                current_chunk_id: ep.current_chunk_id,
                bytes_processed_total: ep.bytes_processed_total,
                chunks_processed: ep.chunks_processed,
                chunk_delay_secs: ep.chunk_delay_secs,
                stall_reason: ep.stall_reason,
                ffmpeg_restart_count: ep.ffmpeg_restart_count,
                last_error: ep.last_error,
                is_fast: ep.is_fast,
                delivery_mode: ep.delivery_mode,
                rescue_eta_secs: ep.rescue_eta_secs,
            })
            .collect();

        let endpoint_count = metrics.len() as u32;
        Ok((name, inst_status, server_ip, endpoint_count, metrics))
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

        // Clear cached endpoint configs for this event
        self.endpoint_fast_cache.lock().await.remove(&event_id);

        db::update_delivery_instance_status(&self.pool, instance.id, "stopping").await?;

        // Best-effort: tell rs-delivery to stop endpoints
        if is_delivery_active(&instance.status) {
            let client = reqwest::Client::new();
            let delivery_url = format!("http://{}:8000", instance.ipv4);
            let _ = client
                .post(format!("{delivery_url}/api/stop"))
                .json(&serde_json::json!({"alias": null}))
                .timeout(Duration::from_secs(10))
                .send()
                .await;
        }

        // Capture VPS logs before deletion for post-mortem analysis.
        // Best-effort: if the VPS is unresponsive, we still proceed with deletion.
        if is_delivery_active(&instance.status) {
            let client = reqwest::Client::new();
            let delivery_url = format!("http://{}:8000", instance.ipv4);
            match client
                .get(format!("{delivery_url}/api/logs?limit=5000"))
                .bearer_auth(&instance.auth_token)
                .timeout(Duration::from_secs(10))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    match resp.json::<serde_json::Value>().await {
                        Ok(body) => {
                            // Format log entries as text lines for human-readable storage
                            let log_text = body["entries"]
                                .as_array()
                                .map(|entries| {
                                    entries
                                        .iter()
                                        .rev() // API returns newest-first, store chronologically
                                        .map(|e| {
                                            format!(
                                                "[{}] {} {}",
                                                e["level"].as_str().unwrap_or("?"),
                                                e["target"].as_str().unwrap_or("?"),
                                                e["message"].as_str().unwrap_or("")
                                            )
                                        })
                                        .collect::<Vec<_>>()
                                        .join("\n")
                                })
                                .unwrap_or_default();

                            if !log_text.is_empty() {
                                if let Err(e) = db::insert_delivery_log(
                                    &self.pool,
                                    instance.id,
                                    instance.event_id,
                                    &log_text,
                                )
                                .await
                                {
                                    warn!("Failed to persist VPS logs: {e}");
                                } else {
                                    info!(
                                        instance_id = instance.id,
                                        lines = log_text.lines().count(),
                                        "Captured VPS logs before deletion"
                                    );
                                }
                            }
                        }
                        Err(e) => warn!("Failed to parse VPS log response: {e}"),
                    }
                }
                Ok(resp) => {
                    warn!(status = %resp.status(), "VPS log capture returned non-success");
                }
                Err(e) => {
                    warn!("VPS log capture failed (VPS may be unresponsive): {e}");
                }
            }
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

    /// Monitor delivery VPS health continuously. Auto-restart on persistent failure.
    ///
    /// Runs every 30s. After 3 consecutive failures (90s), stops the dead VPS and
    /// creates a new one, resuming endpoints from their last known chunk positions.
    /// Retries indefinitely — the 90s detection window provides natural throttling.
    pub async fn monitor_delivery_health(
        self: &Arc<Self>,
        event_id: i64,
        instance_id: i64,
        _cached_delivery: std::sync::Arc<std::sync::RwLock<crate::state::CachedDeliveryStatus>>,
        ws_tx: tokio::sync::broadcast::Sender<rs_core::models::WsEvent>,
    ) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await; // skip immediate tick

        let mut consecutive_failures = 0u32;
        let client = reqwest::Client::new();

        loop {
            interval.tick().await;

            // Check if event is still delivering (operator may have stopped)
            match db::get_streaming_event_by_id(&self.pool, event_id).await {
                Ok(Some(evt)) if !evt.delivering_activated => {
                    info!(
                        event_id,
                        "Health monitor stopping: event no longer delivering"
                    );
                    return;
                }
                Ok(None) => {
                    info!(event_id, "Health monitor stopping: event deleted");
                    return;
                }
                Err(e) => {
                    warn!(event_id, "Health monitor DB error (event): {e}");
                }
                _ => {}
            }

            // Check if instance still exists and is running
            let instance = match db::get_delivery_instance(&self.pool, instance_id).await {
                Ok(Some(inst)) if is_delivery_active(&inst.status) => inst,
                Ok(Some(inst)) => {
                    info!(
                        event_id,
                        status = %inst.status,
                        "Health monitor stopping: instance no longer running"
                    );
                    return;
                }
                Ok(None) => {
                    info!(event_id, "Health monitor stopping: instance deleted");
                    return;
                }
                Err(e) => {
                    warn!(event_id, "Health monitor DB error: {e}");
                    continue;
                }
            };

            // Check health
            let healthy = match client
                .get(format!("http://{}:8000/api/health", instance.ipv4))
                .bearer_auth(&instance.auth_token)
                .timeout(Duration::from_secs(10))
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => true,
                Ok(resp) => {
                    warn!(
                        event_id,
                        status = %resp.status(),
                        "Delivery VPS health returned non-success"
                    );
                    false
                }
                Err(e) => {
                    warn!(event_id, "Delivery VPS health check failed: {e}");
                    false
                }
            };

            if healthy {
                if consecutive_failures > 0 {
                    info!(
                        event_id,
                        previous_failures = consecutive_failures,
                        "Delivery VPS health recovered"
                    );
                }
                consecutive_failures = 0;
                db::update_delivery_instance_health(&self.pool, instance_id)
                    .await
                    .ok();
            } else {
                consecutive_failures += 1;
                error!(
                    event_id,
                    consecutive_failures,
                    "Delivery VPS health check failed ({consecutive_failures}/3)"
                );

                if consecutive_failures >= 3 {
                    // DO NOT restart VPS — "unreachable" usually means stream.lan
                    // lost internet, not that the VPS crashed. The VPS keeps running
                    // and self-recovers when internet returns.
                    error!(
                        event_id,
                        consecutive_failures,
                        "Delivery VPS unreachable for 90s — monitoring continues, VPS NOT restarted"
                    );
                    let _ = ws_tx.send(rs_core::models::WsEvent::ActivityFeed {
                        timestamp: chrono::Utc::now().to_rfc3339(),
                        severity: "warning".to_string(),
                        message: "Delivery VPS unreachable — waiting for network recovery"
                            .to_string(),
                        source: "delivery".to_string(),
                    });
                }
            }
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

// Tests are in delivery_tests.rs
