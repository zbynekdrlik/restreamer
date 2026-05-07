use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::SqlitePool;
use tokio::sync::{Mutex, MutexGuard, mpsc};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use rs_cloud::hetzner::HetznerClient;
use rs_core::audit::{Action, AuditRow, RateLimiter, Severity, Source};
use rs_core::config::Config;
use rs_core::db;

pub(crate) use crate::delivery_helpers::{
    build_endpoint_init_entry, is_delivery_active, is_delivery_or_spawning,
    is_permanent_hetzner_error, persist_delivery_log_to_disk,
};

// Re-exports so existing call sites (`crate::delivery::Foo`, test modules,
// external uses of `rs_api::delivery::Foo`) keep working after the split.
pub use crate::delivery_audit_mirror::mirror_vps_audit;
pub use crate::delivery_status::{
    DeliveryStatus, EndpointDeliveryStatus, EndpointRestartRecord, YouTubeStatus,
};
#[cfg(test)]
pub(crate) use crate::delivery_status::{
    load_restart_history_from_db, pick_last_error_line_inline,
};

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
    /// Audit channel. `None` in tests; emit sites are guarded by `if let Some`.
    audit_tx: Option<mpsc::Sender<AuditRow>>,
}

/// Rate limiter for noisy delivery audit rows (VpsUnreachable). Emits at
/// most 1 row per minute per (action, key) pair — see `RateLimiter`.
pub(crate) static DELIVERY_RL: std::sync::LazyLock<RateLimiter> =
    std::sync::LazyLock::new(RateLimiter::new);

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
            audit_tx: None,
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
            audit_tx: None,
        }
    }

    /// Attach an audit channel. Every lifecycle event (VpsCreating, VpsReady,
    /// VpsDeleted, VpsUnreachable, DeliveryInitSent, DeliveryInitResponse)
    /// emits through this channel. Call once at construction.
    pub fn with_audit_tx(mut self, tx: mpsc::Sender<AuditRow>) -> Self {
        self.audit_tx = Some(tx);
        self
    }

    /// Access the audit sender for internal emit helpers.
    #[allow(dead_code)]
    pub(crate) fn audit_tx(&self) -> Option<&mpsc::Sender<AuditRow>> {
        self.audit_tx.as_ref()
    }

    /// Returns the poll_handles map for tracking background tasks.
    pub fn poll_handles(&self) -> Arc<Mutex<HashMap<i64, JoinHandle<()>>>> {
        Arc::clone(&self.poll_handles)
    }

    /// Lock and return the endpoint fast cache. Used by `delivery_status.rs`
    /// (sibling module) because that file lives outside `delivery.rs` and
    /// can't access the private field directly.
    pub(crate) async fn endpoint_fast_cache_lock(
        &self,
    ) -> MutexGuard<'_, HashMap<i64, HashMap<String, bool>>> {
        self.endpoint_fast_cache.lock().await
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
        // Reuse existing instance only when it's serving traffic or mid-spawn
        // (`is_delivery_or_spawning`). Stale rows (`failed` / `stopped` /
        // `stopping` / unknown) are cleaned up below — see #165.
        if let Some(existing) = db::get_delivery_instance_by_event(&self.pool, event_id).await? {
            if is_delivery_or_spawning(&existing.status) {
                return Ok(StartDeliveryResult {
                    instance_id: existing.id,
                    hetzner_id: existing.hetzner_id,
                    name: existing.name,
                    server_type: existing.server_type,
                    status: existing.status,
                    auth_token: existing.auth_token,
                });
            }
            // Stale row: mark deleted, fall through to spawn fresh.
            // Stop-race-safe: stop_delivery owns its captured hetzner_id
            // so the old VPS is always deleted regardless of rewrites.
            tracing::warn!(
                event_id,
                instance_id = existing.id,
                status = %existing.status,
                "Marking stale delivery_instance row as deleted before spawning new VPS"
            );
            if let Err(e) =
                db::update_delivery_instance_status(&self.pool, existing.id, "deleted").await
            {
                // Worst case: two non-deleted rows; ORDER BY id DESC picks the new one.
                tracing::error!(instance_id = existing.id, "stale-row delete failed: {e}");
            }
        }

        // Wipe S3 chunks for this event before spawning VPS (operator policy
        // 2026-05-07): every "Start Delivering" must begin from a clean S3
        // state so the VPS cannot replay any chunks produced before delivery
        // start. Combined with `compute_target_start_chunk` returning live-
        // edge (latest+1), this guarantees pushed chunks are produced AFTER
        // delivery start.
        if let Some(event) = db::get_streaming_event_by_id(&self.pool, event_id).await? {
            let event_prefix = self.config.event_s3_prefix(&event.name);
            match rs_endpoint::s3::S3Client::new(&self.config.s3) {
                Ok(s3_client) => {
                    let fut = s3_client.delete_event_chunks(&event_prefix);
                    match tokio::time::timeout(Duration::from_secs(60), fut).await {
                        Ok(Ok(n)) => info!(
                            event_id,
                            deleted = n,
                            prefix = %event_prefix,
                            "Wiped S3 chunks before starting delivery"
                        ),
                        Ok(Err(e)) => warn!(event_id, "S3 wipe-on-start failed (continuing): {e}"),
                        Err(_) => warn!(
                            event_id,
                            "S3 wipe-on-start timed out after 60s (continuing)"
                        ),
                    }
                }
                Err(e) => warn!(event_id, "S3 client init for wipe-on-start failed: {e}"),
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
        labels.insert("client_uuid".to_string(), self.config.client_uuid.clone());

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

        // Audit: VPS creation request sent to Hetzner.
        if let Some(tx) = &self.audit_tx {
            rs_core::audit::record(
                tx,
                AuditRow {
                    severity: Severity::Info,
                    source: Source::Delivery,
                    event_id: Some(event_id),
                    instance_id: None,
                    endpoint: None,
                    action: Action::VpsCreating,
                    detail: serde_json::json!({
                        "server_type": server_type,
                        "datacenter": self.config.hetzner.location,
                    }),
                    ts_override: None,
                },
            );
        }

        // Combine primary SSH key with any extra debug/operator keys so a
        // single VPS can be accessed by CI (primary) AND humans (extras)
        // without key rotation or rebuild. Order matters only for cloud-init
        // display; Hetzner installs all listed keys into /root/.ssh/authorized_keys.
        let mut ssh_key_names: Vec<String> = vec![self.config.hetzner.ssh_key_name.clone()];
        ssh_key_names.extend(self.config.hetzner.extra_ssh_key_names.iter().cloned());

        let server = self
            .hetzner
            .create_server(
                &name,
                server_type,
                &self.config.hetzner.location,
                &image,
                &ssh_key_names,
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
        let boot_start = std::time::Instant::now();
        let instance = db::get_delivery_instance(&self.pool, instance_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("delivery instance {instance_id} not found"))?;

        // Poll Hetzner until server is running. Poll every 1s (not 5s)
        // so we detect "running" as soon as possible — avoids wasting
        // up to 5s of user-visible warmup time.
        let hetzner_id = instance.hetzner_id;
        for attempt in 0..300 {
            // 401/403/404 = permanent: fail fast (#174 review finding 4).
            let server = match self.hetzner.get_server(hetzner_id).await {
                Ok(s) => s,
                Err(e) if is_permanent_hetzner_error(&e.to_string()) => {
                    return Err(anyhow::anyhow!("get_server permanent error: {e}"));
                }
                Err(e) if attempt < 299 => {
                    if attempt % 30 == 0 {
                        warn!(attempt, error = %e, "get_server transient, retrying");
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                Err(e) => return Err(anyhow::anyhow!("get_server failed after 300 attempts: {e}")),
            };

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
                    // Audit: VPS booted + rs-delivery answered health check.
                    if let Some(tx) = &self.audit_tx {
                        rs_core::audit::record(
                            tx,
                            AuditRow {
                                severity: Severity::Info,
                                source: Source::Delivery,
                                event_id: Some(event_id),
                                instance_id: Some(instance_id),
                                endpoint: None,
                                action: Action::VpsReady,
                                detail: serde_json::json!({
                                    "hetzner_id": hetzner_id,
                                    "ipv4": instance.ipv4,
                                    "boot_secs": boot_start.elapsed().as_secs(),
                                }),
                                ts_override: None,
                            },
                        );
                    }
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

        let mut start_chunk_id;
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
            // Wait for the full target duration of content on S3 before
            // creating the VPS. The previous "rescue video bridges the
            // gap, so wait for 1 chunk" shortcut produced non-deterministic
            // cache at delivery start because VPS-side warmup can exit
            // with fewer real seconds of content than the duration sum
            // suggests (zero-duration chunks on session reset). Waiting
            // orchestrator-side guarantees target content exists before
            // the VPS boots.
            let wait_target_ms = target_delay_ms;

            let max_wait_secs = 900;
            for attempt in 0..max_wait_secs {
                let sent_ms = db::get_sent_duration_ms(&self.pool, event_id)
                    .await
                    .unwrap_or(0);
                if sent_ms >= wait_target_ms {
                    info!(
                        event_id,
                        sent_ms, wait_target_ms, "Sent content duration meets target (init VPS)"
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
            // Compute the start chunk that gives exactly target_delay_ms of
            // buffer from the latest sent chunk. If total content <= target
            // (normal case), this returns first_seq and VPS warmup waits for
            // more. If content > target (VPS boot exceeded target, or OBS was
            // started early), this returns a later chunk so cache starts at
            // exactly the target instead of overshooting.
            start_chunk_id =
                db::compute_target_start_chunk(&self.pool, event_id, target_delay_ms).await?;
            // Orphan DB rows from prior s3_cleared can point at chunks
            // that no longer exist on S3; advance to the first chunk_id
            // that actually exists or the producer hangs (#174).
            if let Ok(s3) = rs_endpoint::s3::S3Client::new(&self.config.s3) {
                let p = self.config.event_s3_prefix(event_name);
                match s3.find_first_chunk_id_at_or_after(&p, start_chunk_id).await {
                    Ok(Some(actual)) if actual != start_chunk_id => {
                        info!(
                            event_id,
                            was = start_chunk_id,
                            actual,
                            "advanced to first S3 chunk"
                        );
                        start_chunk_id = actual;
                    }
                    Ok(_) => {}
                    Err(e) => warn!(event_id, "S3 LIST validation failed: {e}"),
                }
            }
            info!(
                event_id,
                start_chunk_id, "Starting delivery (live-edge computed)"
            );
        }

        let chunk_format = &self.config.inpoint.chunk_format;
        let init_body = serde_json::json!({
            "endpoints": endpoints.iter().map(|ep| {
                // Use per-endpoint resume position if available
                let ep_start = resume_pos.as_ref()
                    .and_then(|rp| rp.get(&ep.alias).copied())
                    .unwrap_or(start_chunk_id);
                build_endpoint_init_entry(ep, chunk_format, ep_start)
            }).collect::<Vec<_>>(),
            "s3_config": {
                "bucket": self.config.s3.bucket,
                "region": self.config.s3.region,
                "endpoint": self.config.s3.endpoint,
                "access_key_id": "from-env",
                "secret_access_key": "from-env",
            },
            "event_identifier": self.config.event_s3_prefix(event_name),
            "start_chunk_id": start_chunk_id,
            "delivery_delay_ms": target_delay_ms,
            "rescue_video_url": event.rescue_video_url,
        });

        // Audit: init payload dispatched to rs-delivery.
        if let Some(tx) = &self.audit_tx {
            rs_core::audit::record(
                tx,
                AuditRow {
                    severity: Severity::Info,
                    source: Source::Delivery,
                    event_id: Some(event_id),
                    instance_id: Some(instance_id),
                    endpoint: None,
                    action: Action::DeliveryInitSent,
                    detail: serde_json::json!({
                        "event_id": event_id,
                        "endpoints_count": endpoints.len(),
                        "start_chunk_id": start_chunk_id,
                        "pushers": endpoints.iter()
                            .map(|ep| serde_json::json!({
                                "alias": ep.alias,
                                "pusher": ep.pusher,
                            }))
                            .collect::<Vec<_>>(),
                    }),
                    ts_override: None,
                },
            );
        }

        let resp = client
            .post(format!("{delivery_url}/api/init"))
            .bearer_auth(auth_token)
            .json(&init_body)
            .timeout(Duration::from_secs(30))
            .send()
            .await?;

        let init_status = resp.status();
        if !init_status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            // Audit: init failed. Severity Error so it survives back-pressure.
            if let Some(tx) = &self.audit_tx {
                rs_core::audit::record(
                    tx,
                    AuditRow {
                        severity: Severity::Error,
                        source: Source::Delivery,
                        event_id: Some(event_id),
                        instance_id: Some(instance_id),
                        endpoint: None,
                        action: Action::DeliveryInitResponse,
                        detail: serde_json::json!({
                            "event_id": event_id,
                            "status": format!("{init_status}"),
                            "body": body,
                        }),
                        ts_override: None,
                    },
                );
            }
            return Err(anyhow::anyhow!(
                "rs-delivery /api/init failed: {init_status} - {body}"
            ));
        }

        let init_resp = resp.text().await.unwrap_or_default();
        info!(event_id, init_resp = %init_resp, "Init response received");

        // Audit: init succeeded.
        if let Some(tx) = &self.audit_tx {
            rs_core::audit::record(
                tx,
                AuditRow {
                    severity: Severity::Info,
                    source: Source::Delivery,
                    event_id: Some(event_id),
                    instance_id: Some(instance_id),
                    endpoint: None,
                    action: Action::DeliveryInitResponse,
                    detail: serde_json::json!({
                        "event_id": event_id,
                        "endpoints_started": endpoints.len(),
                        "status": "ok",
                    }),
                    ts_override: None,
                },
            );
        }

        db::update_delivery_instance_health(&self.pool, instance_id).await?;
        // Init succeeded — endpoints are now warming up / delivering.
        // The dashboard reads this status to show "Delivering" instead
        // of "Initializing".
        db::update_delivery_instance_status(&self.pool, instance_id, "delivering").await?;
        info!(event_id, "Delivery endpoints initialized successfully");

        // Spawn the per-delivery clock-skew probe now that the VPS is confirmed
        // healthy and delivering. Single spawn point — do NOT spawn this probe
        // from delivery_handlers.rs or stream_handlers.rs.
        let vps_base_url = format!("http://{}:8000", instance.ipv4);
        crate::clock_skew_probe::spawn_skew_probe(self.pool.clone(), event_id, vps_base_url);
        info!(event_id, "Clock-skew probe started");

        Ok(())
    }

    /// Stop delivery for an event: POST /api/stop, then delete Hetzner server.
    ///
    /// Race note: between the `"stopping"` status write below and the
    /// final `"deleted"` status write at the end of this function, a
    /// concurrent `start_delivery` may run. That call sees status =
    /// `"stopping"` (not in `is_delivery_or_spawning`), classifies the
    /// row as stale, and rewrites it to `"deleted"`. Harmless: this
    /// function captures `instance.hetzner_id` BEFORE either write, so
    /// the OLD VPS still gets deleted on Hetzner regardless of how many
    /// times the row is rewritten. No orphan VPS billing leak. Symmetric
    /// to the comment in `start_delivery`.
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
                                persist_delivery_log_to_disk(
                                    instance.id,
                                    instance.event_id,
                                    &log_text,
                                );
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
        let mut delete_reason = "operator_stop";
        if let Err(e) = self.hetzner.delete_server(instance.hetzner_id).await {
            error!(
                hetzner_id = instance.hetzner_id,
                "Failed to delete Hetzner server: {e}"
            );
            delete_reason = "delete_error";
        }

        db::update_delivery_instance_status(&self.pool, instance.id, "deleted").await?;
        info!(
            hetzner_id = instance.hetzner_id,
            event_id, "Delivery instance stopped and deleted"
        );

        // Audit: VPS destroyed.
        if let Some(tx) = &self.audit_tx {
            rs_core::audit::record(
                tx,
                AuditRow {
                    severity: Severity::Info,
                    source: Source::Delivery,
                    event_id: Some(event_id),
                    instance_id: Some(instance.id),
                    endpoint: None,
                    action: Action::VpsDeleted,
                    detail: serde_json::json!({
                        "hetzner_id": instance.hetzner_id,
                        "ipv4": instance.ipv4,
                        "reason": delete_reason,
                    }),
                    ts_override: None,
                },
            );
        }

        Ok(())
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
