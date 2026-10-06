//! The production A/V-gate rig (#357): the CI event, its delivery VPS and the
//! Hetzner check, driven through the SAME code paths as the dashboard's
//! Start/Stop Stream (`stream_handlers::start_stream` / `stop_stream`). So the
//! single-event guard (refuse while another event is live) and the #354
//! ingest-skew gate apply to the gate exactly as they apply to an operator.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::extract::{Path as UrlPath, Query, State};
use axum::http::StatusCode;
use rs_core::db;
use rs_core::models::StreamingEvent;
use rs_youtube::manage::{ManageClient, ManageCredentials};

use crate::av_gate::{AvGateRig, AvGateTimings, RigDelivery, RigEvent, StartEventError};
use crate::av_gate_driver::{ClientFactory, SessionCtx, supervise_maintenance};
use crate::state::AppState;
use crate::stream_handlers::{StartStreamQuery, start_stream, stop_stream};

pub(crate) struct AppRig {
    state: AppState,
}

impl AppRig {
    pub(crate) fn new(state: AppState) -> Self {
        Self { state }
    }
}

/// `delivery_instances.status` -> where the delivery is.
pub(crate) fn delivery_phase(status: Option<&str>) -> RigDelivery {
    match status {
        Some("delivering" | "running") => RigDelivery::Delivering,
        Some("creating" | "booting" | "initializing") => RigDelivery::Booting,
        _ => RigDelivery::NotRunning,
    }
}

/// The Hetzner label selector for this box's servers of one event.
pub(crate) fn server_selector(client_uuid: &str, event_id: i64) -> String {
    format!("app=restreamer,client_uuid={client_uuid},event_id={event_id}")
}

/// True while an event receives or delivers.
pub(crate) fn is_active(e: &StreamingEvent) -> bool {
    e.receiving_activated || e.delivering_activated
}

#[async_trait]
impl AvGateRig for AppRig {
    /// Find the CI event by name, BEFORE any YouTube write. Refuses while ANY
    /// event is active, the CI event included: another event means a real
    /// service may be streaming, and an active CI event belongs to another run
    /// (restreamer's own CI E2E uses the same event). Also refuses when no
    /// delivery can start, so a refused session leaves no broadcast behind.
    async fn resolve_event(&self, name: &str) -> Result<RigEvent, String> {
        if self.state.delivery_orchestrator.is_none() {
            return Err("delivery is not configured (no Hetzner token)".to_string());
        }
        let events = db::list_streaming_events(&self.state.pool)
            .await
            .map_err(|e| format!("listing events failed: {e}"))?;
        if let Some(live) = events.iter().find(|e| is_active(e)) {
            return Err(format!(
                "event {:?} is active; the gate runs only when no event is",
                live.name
            ));
        }
        let event = events
            .iter()
            .find(|e| e.name == name)
            .ok_or_else(|| format!("no event named {name:?}"))?;
        let drain_secs = event
            .cache_delay_secs
            .map(|s| s.max(0) as u64)
            .unwrap_or(self.state.config.delivery.delivery_delay_secs);
        Ok(RigEvent {
            id: event.id,
            drain: Duration::from_secs(drain_secs),
        })
    }

    async fn start_event(&self, event_id: i64) -> Result<(), StartEventError> {
        if self.state.delivery_orchestrator.is_none() {
            return Err(StartEventError::Refused(
                "delivery is not configured (no Hetzner token)".to_string(),
            ));
        }
        // Re-checked right before the start: YouTube calls ran since
        // `resolve_event`, and another run may have taken the event meanwhile.
        if self
            .event_active(event_id)
            .await
            .map_err(StartEventError::Refused)?
        {
            return Err(StartEventError::Refused(format!(
                "event {event_id} became active (another run took it)"
            )));
        }
        let started = start_stream(
            State(self.state.clone()),
            UrlPath(event_id),
            Query(StartStreamQuery::default()),
        )
        .await;
        match started {
            Ok(_) => {}
            Err(StatusCode::CONFLICT) => {
                return Err(StartEventError::Refused(
                    "another event is active".to_string(),
                ));
            }
            Err(StatusCode::NOT_FOUND) => {
                return Err(StartEventError::Refused(format!(
                    "event {event_id} does not exist"
                )));
            }
            Err(status) => {
                return Err(StartEventError::Failed(format!(
                    "starting event {event_id} failed: HTTP {}",
                    status.as_u16()
                )));
            }
        }
        // `start_stream` reports a failed VPS start only to the activity
        // feed. A started delivery always has its row by the time it returns.
        match db::get_delivery_instance_by_event(&self.state.pool, event_id).await {
            Ok(Some(_)) => Ok(()),
            Ok(None) => Err(StartEventError::Failed(
                "the delivery VPS did not start (see the activity feed)".to_string(),
            )),
            Err(e) => Err(StartEventError::Failed(format!(
                "reading the delivery state failed: {e}"
            ))),
        }
    }

    async fn delivery(&self, event_id: i64) -> Result<RigDelivery, String> {
        let row = db::get_delivery_instance_by_event(&self.state.pool, event_id)
            .await
            .map_err(|e| format!("reading the delivery state failed: {e}"))?;
        Ok(delivery_phase(row.as_ref().map(|r| r.status.as_str())))
    }

    async fn stop_event(&self, event_id: i64) -> Result<(), String> {
        stop_stream(State(self.state.clone()), UrlPath(event_id))
            .await
            .map(|_| ())
            .map_err(|s| format!("stopping event {event_id} failed: HTTP {}", s.as_u16()))
    }

    async fn event_active(&self, event_id: i64) -> Result<bool, String> {
        db::get_streaming_event_by_id(&self.state.pool, event_id)
            .await
            .map_err(|e| format!("reading event {event_id} failed: {e}"))
            .map(|e| e.as_ref().is_some_and(is_active))
    }

    async fn server_count(&self, event_id: i64) -> Result<usize, String> {
        let Some(orch) = &self.state.delivery_orchestrator else {
            // No Hetzner token: this box cannot have created a server.
            return Ok(0);
        };
        let selector = server_selector(&self.state.config.client_uuid, event_id);
        orch.hetzner()
            .list_servers(Some(&selector))
            .await
            .map(|s| s.len())
            .map_err(|e| e.to_string())
    }
}

/// Build the YouTube manage client from the configured oauth file and the
/// device-flow client credentials. It draws from the same project quota
/// bucket as the YouTube health polling (`youtube_quota_tracker`).
pub(crate) fn manage_client(state: &AppState) -> Result<ManageClient, String> {
    let tracker = crate::delivery_status::youtube_quota_tracker();
    let cfg = &state.config;
    let creds = ManageCredentials::from_oauth_file(
        Path::new(&cfg.av_gate.oauth_file),
        &cfg.youtube.device_flow.client_id,
        &cfg.youtube.device_flow.client_secret,
    )
    .map_err(|e| e.to_string())?;
    #[cfg(test)]
    if let Some(seam) = state.av_gate.seam.lock().expect("seam").clone() {
        let client = ManageClient::with_endpoints(creds, &seam.api_base, &seam.token_uri);
        return Ok(match seam.quota_bucket {
            Some(bucket) => client.with_quota_tracker(bucket),
            None => client,
        });
    }
    Ok(ManageClient::new(creds).with_quota_tracker(tracker))
}

/// The session context for `state`: the production rig and timings.
pub(crate) fn session_ctx(state: &AppState) -> Arc<SessionCtx> {
    let cfg = &state.config.av_gate;
    let rig: Arc<dyn AvGateRig> = Arc::new(AppRig::new(state.clone()));
    let timings = AvGateTimings::from_config(cfg);
    let quota_bucket = Some(crate::delivery_status::youtube_quota_tracker());
    #[cfg(test)]
    let (rig, timings, quota_bucket) = match state.av_gate.seam.lock().expect("seam").clone() {
        Some(seam) => (seam.rig, seam.timings, seam.quota_bucket),
        None => (rig, timings, quota_bucket),
    };
    Arc::new(SessionCtx {
        pool: state.pool.clone(),
        audit_tx: state.audit_tx.clone(),
        registry: Arc::clone(&state.av_gate.registry),
        rig,
        timings,
        event_name: cfg.event_name.clone(),
        stream_title: cfg.stream_title.clone(),
        daily_quota_budget: cfg.daily_quota_budget,
        quota_bucket,
    })
}

/// The av-gate maintenance task (#357), spawned by the runtime AFTER the
/// delivery boot reconcile, so the delivery it re-attached for a session's
/// event is the one stopped here: the boot reconcile (tear down `starting`/
/// `ready` sessions a crash left, resume `processing` ones, then open the API),
/// followed by the retry loop for failed teardowns. Runs until shutdown.
pub async fn run_av_gate_maintenance(state: AppState) {
    let ctx = session_ctx(&state);
    let clients: ClientFactory = Arc::new(move || manage_client(&state));
    supervise_maintenance(ctx, clients).await;
}

#[cfg(test)]
#[path = "av_gate_rig_tests.rs"]
mod tests;
