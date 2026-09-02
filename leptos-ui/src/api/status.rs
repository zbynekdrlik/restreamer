//! `StatusResponse` (the combined `/status`-equivalent payload) + `get_status()`.
//! Split from `api/mod.rs` to keep it under the 1000-line CI cap (#354 review).

use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::JsValue;

use super::{ChunkStats, CommandResult, StreamingEvent, http_get, invoke, is_tauri};

/// Combined status response.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct StatusResponse {
    pub streaming_event: Option<StreamingEvent>,
    pub chunk_stats: ChunkStats,
    pub inpoint_connected: bool,
    /// Seconds the RTMP publisher has been stably connected. Used by the
    /// dashboard to gate Start-Delivery until the ingest has been up for
    /// at least 15 seconds.
    #[serde(default)]
    pub rtmp_stable_secs: u64,
    /// Local chunk-store disk-pressure level: "ok" | "warn" | "critical".
    /// Drives the DiskPressureBanner (#231). Empty/"ok" in Tauri IPC mode
    /// (the IPC StatusResponse does not carry it) -> banner stays hidden.
    #[serde(default)]
    pub disk_pressure: String,
    /// Whether the configured S3 region matches the project standard
    /// (`fsn1`). Drives the S3RegionBanner (#278). Defaults to true
    /// (assume standard) so a missing field never false-alarms.
    #[serde(default = "default_true")]
    pub s3_region_standard: bool,
    /// Live ingest A/V skew (ms, signed; positive = audio behind video).
    /// Drives the IngestSkewBanner's "~N s" text (#354).
    #[serde(default)]
    pub ingest_skew_ms: i64,
    /// Latched "ingest skew over threshold" flag — source (OBS) desynced.
    /// Drives IngestSkewBanner visibility + the Start-Delivering client gate
    /// (#354). Defaults false so a missing field never false-alarms.
    #[serde(default)]
    pub ingest_skew_active: bool,
    /// Number of orphaned delivery VPS still billing on Hetzner (#352). Drives
    /// the VpsOrphanBanner. Defaults 0 (assume none) so a missing field never
    /// false-alarms.
    #[serde(default)]
    pub vps_orphan_count: u8,
}

fn default_true() -> bool {
    true
}

/// Get the current service status.
/// In Tauri mode, uses IPC invoke. In browser mode, fetches from HTTP API.
pub async fn get_status() -> Result<StatusResponse, String> {
    if is_tauri() {
        let result: CommandResult<StatusResponse> = invoke("get_status", JsValue::NULL).await?;
        if result.success {
            return result.data.ok_or_else(|| "No data returned".to_string());
        }
        return Err(result.error.unwrap_or_else(|| "Unknown error".to_string()));
    }
    // Browser mode: fetch full /status for inpoint state, plus chunk stats
    let status: serde_json::Value = http_get("/status").await.unwrap_or_default();
    let event: Option<StreamingEvent> = serde_json::from_value(status["streaming_event"].clone())
        .ok()
        .flatten();
    let chunk_stats: ChunkStats = http_get("/chunks/stats").await.unwrap_or_default();
    let inpoint_connected = status["inpoint"]["details"]["rtmp_connected"]
        .as_bool()
        .unwrap_or(false);
    let rtmp_stable_secs = status["inpoint"]["details"]["rtmp_stable_secs"]
        .as_u64()
        .unwrap_or(0);
    let disk_pressure = status["disk_pressure"]
        .as_str()
        .unwrap_or("ok")
        .to_string();
    let s3_region_standard = status["s3_region_standard"].as_bool().unwrap_or(true);
    let ingest_skew_ms = status["inpoint"]["details"]["ingest_skew_ms"]
        .as_i64()
        .unwrap_or(0);
    let ingest_skew_active = status["inpoint"]["details"]["ingest_skew_active"]
        .as_bool()
        .unwrap_or(false);
    let vps_orphan_count = status["vps_orphan_count"].as_u64().unwrap_or(0) as u8;
    Ok(StatusResponse {
        streaming_event: event,
        chunk_stats,
        inpoint_connected,
        rtmp_stable_secs,
        disk_pressure,
        s3_region_standard,
        ingest_skew_ms,
        ingest_skew_active,
        vps_orphan_count,
    })
}
