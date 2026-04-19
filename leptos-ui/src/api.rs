//! Tauri invoke API wrappers for the frontend.

use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = ["window", "__TAURI__", "core"], js_name = invoke)]
    fn tauri_invoke(cmd: &str, args: JsValue) -> js_sys::Promise;
}

/// Command result wrapper matching the Rust backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResult<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

/// Streaming event from the backend.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct StreamingEvent {
    pub id: i64,
    pub name: String,
    pub received_bytes: i64,
    pub receiving_activated: bool,
    pub delivering_activated: bool,
    #[serde(default)]
    pub cache_delay_secs: Option<i64>,
    #[serde(default)]
    pub created_from: Option<String>,
    #[serde(default)]
    pub rescue_video_url: Option<String>,
}

/// Event template (reusable preset).
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct EventTemplate {
    pub id: i64,
    pub name: String,
    #[serde(default)]
    pub cache_delay_secs: Option<i64>,
    #[serde(default)]
    pub rescue_video_url: Option<String>,
}

/// Chunk statistics from the backend.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ChunkStats {
    pub total_chunks: i64,
    pub pending_chunks: i64,
    pub sent_chunks: i64,
    pub in_process_chunks: i64,
    pub total_bytes: i64,
    pub buffer_duration_secs: f64,
}

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
}

/// Log entry from the backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub level: String,
    pub target: String,
    pub message: String,
}

/// Invoke a Tauri command and parse the result.
async fn invoke<T: for<'de> Deserialize<'de>>(
    cmd: &str,
    args: JsValue,
) -> Result<CommandResult<T>, String> {
    let promise = tauri_invoke(cmd, args);
    let result = JsFuture::from(promise)
        .await
        .map_err(|e| format!("Invoke failed: {:?}", e))?;

    serde_wasm_bindgen::from_value(result).map_err(|e| format!("Parse error: {e}"))
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
    Ok(StatusResponse {
        streaming_event: event,
        chunk_stats,
        inpoint_connected,
        rtmp_stable_secs,
    })
}

/// Get chunk statistics.
pub async fn get_chunk_stats() -> Result<ChunkStats, String> {
    if is_tauri() {
        let result: CommandResult<ChunkStats> = invoke("get_chunk_stats", JsValue::NULL).await?;
        if result.success {
            return result.data.ok_or_else(|| "No data returned".to_string());
        }
        return Err(result.error.unwrap_or_else(|| "Unknown error".to_string()));
    }
    http_get("/chunks/stats").await
}

/// Get the current streaming event.
pub async fn get_streaming_event() -> Result<Option<StreamingEvent>, String> {
    if is_tauri() {
        let result: CommandResult<Option<StreamingEvent>> =
            invoke("get_streaming_event", JsValue::NULL).await?;
        if result.success {
            return Ok(result.data.flatten());
        }
        return Err(result.error.unwrap_or_else(|| "Unknown error".to_string()));
    }
    // HTTP endpoint returns null when no event, which deserializes as None
    Ok(http_get("/streaming-event").await.ok())
}

/// Logs response from the HTTP API.
#[derive(Debug, Clone, Deserialize)]
struct LogsResponse {
    entries: Vec<LogEntry>,
}

/// Get recent log entries for a component.
pub async fn get_logs(component: &str, _limit: usize) -> Result<Vec<LogEntry>, String> {
    if is_tauri() {
        #[derive(Serialize)]
        struct Args {
            component: String,
            limit: Option<usize>,
        }
        let args = serde_wasm_bindgen::to_value(&Args {
            component: component.to_string(),
            limit: Some(_limit),
        })
        .map_err(|e| e.to_string())?;
        let result: CommandResult<Vec<LogEntry>> = invoke("get_logs", args).await?;
        if result.success {
            return result.data.ok_or_else(|| "No data returned".to_string());
        }
        return Err(result.error.unwrap_or_else(|| "Unknown error".to_string()));
    }
    // Browser mode: use HTTP logs endpoint (returns {entries: [...]})
    // The API routes are /logs/inpoint and /logs/endpoint, but the component
    // filter values are "rs_inpoint", "rs_endpoint", "rs_runtime". Strip "rs_".
    let route = component.strip_prefix("rs_").unwrap_or(component);
    let resp: LogsResponse = http_get(&format!("/logs/{route}")).await?;
    Ok(resp.entries)
}

/// Format bytes as human-readable string.
pub fn format_bytes(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = KB * 1024;
    const GB: i64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format duration as HH:MM:SS.
pub fn format_duration(secs: f64) -> String {
    let total = secs as u64;
    let h = total / 3600;
    let m = (total % 3600) / 60;
    let s = total % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

// --- HTTP-based API calls to local Axum server ---

// Detect whether we're running inside Tauri or a regular browser and return
// the appropriate API base URL. Inside Tauri the origin is tauri://localhost,
// so we must use an absolute URL. In a LAN browser the origin already points
// at the Axum server, so a relative path works.
#[wasm_bindgen(inline_js = "
export function compute_api_base() {
    if (window.__TAURI__) {
        return 'http://127.0.0.1:8910/api/v1';
    }
    return window.location.origin + '/api/v1';
}
export function js_is_tauri() {
    return !!(window.__TAURI__);
}
")]
extern "C" {
    #[wasm_bindgen(js_name = compute_api_base)]
    fn compute_api_base() -> String;
    #[wasm_bindgen(js_name = js_is_tauri)]
    fn js_is_tauri() -> bool;
}

fn api_base() -> String {
    compute_api_base()
}

fn is_tauri() -> bool {
    js_is_tauri()
}

/// Endpoint configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EndpointConfig {
    pub id: i64,
    pub alias: String,
    pub service_type: String,
    pub stream_key: String,
    pub enabled: bool,
    pub position_last: i64,
    pub delivered_bytes: i64,
    pub is_fast: bool,
    pub created_at: String,
    pub updated_at: String,
}

async fn http_get<T: for<'de> Deserialize<'de>>(path: &str) -> Result<T, String> {
    let url = format!("{}{path}", api_base());
    let resp = gloo_net::http::Request::get(&url)
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?;
    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }
    resp.json().await.map_err(|e| format!("Parse error: {e}"))
}

async fn http_post(path: &str) -> Result<(), String> {
    let url = format!("{}{path}", api_base());
    let resp = gloo_net::http::Request::post(&url)
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?;
    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }
    Ok(())
}

async fn http_post_json<T: Serialize>(path: &str, body: &T) -> Result<serde_json::Value, String> {
    let url = format!("{}{path}", api_base());
    let resp = gloo_net::http::Request::post(&url)
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(body).map_err(|e| e.to_string())?)
        .map_err(|e| format!("Request error: {e}"))?
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?;
    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }
    resp.json().await.map_err(|e| format!("Parse error: {e}"))
}

async fn http_delete(path: &str) -> Result<(), String> {
    let url = format!("{}{path}", api_base());
    let resp = gloo_net::http::Request::delete(&url)
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?;
    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }
    Ok(())
}

async fn http_put_json<T: Serialize>(path: &str, body: &T) -> Result<(), String> {
    let url = format!("{}{path}", api_base());
    let resp = gloo_net::http::Request::put(&url)
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(body).map_err(|e| e.to_string())?)
        .map_err(|e| format!("Request error: {e}"))?
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?;
    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }
    Ok(())
}

async fn http_patch_json<T: Serialize>(path: &str, body: &T) -> Result<(), String> {
    let url = format!("{}{path}", api_base());
    let resp = gloo_net::http::Request::patch(&url)
        .header("Content-Type", "application/json")
        .body(serde_json::to_string(body).map_err(|e| e.to_string())?)
        .map_err(|e| format!("Request error: {e}"))?
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?;
    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }
    Ok(())
}

// Events API
pub async fn list_events() -> Result<Vec<StreamingEvent>, String> {
    http_get("/events").await
}

pub async fn create_event(name: &str) -> Result<serde_json::Value, String> {
    #[derive(Serialize)]
    struct Body {
        name: String,
    }
    http_post_json(
        "/events",
        &Body {
            name: name.to_string(),
        },
    )
    .await
}

pub async fn activate_event(id: i64) -> Result<(), String> {
    http_post(&format!("/events/{id}/activate")).await
}

pub async fn start_delivering(id: i64) -> Result<(), String> {
    http_post(&format!("/events/{id}/start-delivering")).await?;
    // Also start the Hetzner delivery VPS
    let body = serde_json::json!({ "event_id": id });
    http_post_json("/delivery/start", &body)
        .await
        .map_err(|e| format!("Delivery VPS start failed: {e}"))?;
    Ok(())
}

pub async fn deactivate_event(id: i64) -> Result<(), String> {
    http_post(&format!("/events/{id}/deactivate")).await
}

pub async fn delete_event(id: i64) -> Result<(), String> {
    http_delete(&format!("/events/{id}")).await
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClearChunksResponse {
    pub deleted: u64,
}

pub async fn clear_event_s3_chunks(id: i64) -> Result<ClearChunksResponse, String> {
    let path = format!("/events/{id}/clear-s3");
    let url = format!("{}{path}", api_base());
    let resp = gloo_net::http::Request::post(&url)
        .send()
        .await
        .map_err(|e| format!("HTTP error: {e}"))?;
    if !resp.ok() {
        return Err(format!("HTTP {}", resp.status()));
    }
    resp.json().await.map_err(|e| format!("Parse error: {e}"))
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3UsageEntry {
    pub event_name: String,
    pub bytes: u64,
    pub objects: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3UsageResponse {
    pub total_bytes: u64,
    pub total_objects: u64,
    pub by_event: Vec<S3UsageEntry>,
}

pub async fn get_s3_usage() -> Result<S3UsageResponse, String> {
    http_get("/s3/usage").await
}

#[derive(Clone, Debug, Deserialize, PartialEq, Default)]
pub struct UploadStats {
    pub chunks_per_sec: f64,
    pub median_ms: u32,
    pub p95_ms: u32,
    pub error_rate: f64,
    pub in_flight: usize,
    pub adaptive_target: usize,
}

/// Get current upload telemetry snapshot (1-minute window) from backend.
pub async fn fetch_upload_stats() -> Result<UploadStats, String> {
    http_get("/uploads/stats").await
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct UploadRow {
    pub chunk_id: i64,
    pub event_identifier: String,
    pub sequence_number: i64,
    pub size_bytes: i64,
    pub attempts: i64,
    pub duration_ms: Option<i64>,
    pub status: String,
    pub last_error: Option<String>,
    pub first_attempt_at: Option<i64>,
    pub completed_at: Option<i64>,
}

/// Fetch recent chunk upload history (newest first).
pub async fn fetch_recent_uploads(limit: u32) -> Result<Vec<UploadRow>, String> {
    http_get(&format!("/uploads/recent?limit={limit}")).await
}

// Templates API
pub async fn list_templates() -> Result<Vec<EventTemplate>, String> {
    http_get("/templates").await
}

pub async fn create_template(
    name: &str,
    cache_delay_secs: Option<i64>,
    rescue_video_url: Option<String>,
) -> Result<serde_json::Value, String> {
    #[derive(Serialize)]
    struct Body {
        name: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        cache_delay_secs: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        rescue_video_url: Option<String>,
    }
    http_post_json(
        "/templates",
        &Body {
            name: name.to_string(),
            cache_delay_secs,
            rescue_video_url,
        },
    )
    .await
}

pub async fn update_template(
    id: i64,
    name: Option<&str>,
    cache_delay_secs: Option<i64>,
    rescue_video_url: Option<String>,
) -> Result<(), String> {
    #[derive(Serialize)]
    struct Body {
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cache_delay_secs: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        rescue_video_url: Option<String>,
    }
    http_patch_json(
        &format!("/templates/{id}"),
        &Body {
            name: name.map(|s| s.to_string()),
            cache_delay_secs,
            rescue_video_url,
        },
    )
    .await
}

pub async fn delete_template(id: i64) -> Result<(), String> {
    http_delete(&format!("/templates/{id}")).await
}

/// Upload a rescue video file to the backend. Returns the public URL of
/// the uploaded video on success. The file is sent as multipart/form-data
/// with a "file" field; the server stores it to S3 with public-read ACL
/// and returns the URL.
pub async fn upload_rescue_video(file: web_sys::File) -> Result<String, String> {
    use wasm_bindgen::JsCast;

    let form_data = web_sys::FormData::new().map_err(|e| format!("FormData: {e:?}"))?;
    form_data
        .append_with_blob("file", file.as_ref())
        .map_err(|e| format!("FormData append: {e:?}"))?;

    let opts = web_sys::RequestInit::new();
    opts.set_method("POST");
    opts.set_body(&form_data);

    let url = format!("{}/rescue-video/upload", api_base());
    let request =
        web_sys::Request::new_with_str_and_init(&url, &opts).map_err(|e| format!("req: {e:?}"))?;

    let window = web_sys::window().ok_or("no window")?;
    let resp_promise = window.fetch_with_request(&request);
    let resp_js = JsFuture::from(resp_promise)
        .await
        .map_err(|e| format!("fetch: {e:?}"))?;
    let resp: web_sys::Response = resp_js.dyn_into().map_err(|e| format!("cast: {e:?}"))?;

    if !resp.ok() {
        return Err(format!("upload failed with status {}", resp.status()));
    }

    let json_promise = resp.json().map_err(|e| format!("json promise: {e:?}"))?;
    let json_js = JsFuture::from(json_promise)
        .await
        .map_err(|e| format!("json: {e:?}"))?;
    let parsed: serde_json::Value =
        serde_wasm_bindgen::from_value(json_js).map_err(|e| format!("parse: {e:?}"))?;
    parsed["url"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| "response missing 'url' field".to_string())
}

pub async fn get_template_endpoints(template_id: i64) -> Result<Vec<EndpointConfig>, String> {
    http_get(&format!("/templates/{template_id}/endpoints")).await
}

pub async fn attach_template_endpoint(template_id: i64, endpoint_id: i64) -> Result<(), String> {
    http_post(&format!("/templates/{template_id}/endpoints/{endpoint_id}")).await
}

pub async fn detach_template_endpoint(template_id: i64, endpoint_id: i64) -> Result<(), String> {
    http_delete(&format!("/templates/{template_id}/endpoints/{endpoint_id}")).await
}

pub async fn create_event_from_template(template_id: i64) -> Result<serde_json::Value, String> {
    #[derive(Serialize)]
    struct Body {
        template_id: i64,
    }
    http_post_json("/events", &Body { template_id }).await
}

// Endpoints API
pub async fn list_endpoints() -> Result<Vec<EndpointConfig>, String> {
    http_get("/endpoints").await
}

pub async fn create_endpoint(
    alias: &str,
    service_type: &str,
    stream_key: &str,
) -> Result<serde_json::Value, String> {
    #[derive(Serialize)]
    struct Body {
        alias: String,
        service_type: String,
        stream_key: String,
    }
    http_post_json(
        "/endpoints",
        &Body {
            alias: alias.to_string(),
            service_type: service_type.to_string(),
            stream_key: stream_key.to_string(),
        },
    )
    .await
}

pub async fn delete_endpoint(id: i64) -> Result<(), String> {
    http_delete(&format!("/endpoints/{id}")).await
}

/// Request body for updating an endpoint (all fields optional).
#[derive(Debug, Clone, Serialize, Default)]
pub struct UpdateEndpointRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_fast: Option<bool>,
}

pub async fn update_endpoint(id: i64, req: &UpdateEndpointRequest) -> Result<(), String> {
    http_put_json(&format!("/endpoints/{id}"), req).await
}

// Event-Endpoint Assignment API
pub async fn get_event_endpoints(event_id: i64) -> Result<Vec<EndpointConfig>, String> {
    http_get(&format!("/events/{event_id}/endpoints")).await
}

pub async fn attach_endpoint(event_id: i64, endpoint_id: i64) -> Result<(), String> {
    http_post(&format!("/events/{event_id}/endpoints/{endpoint_id}")).await
}

pub async fn detach_endpoint(event_id: i64, endpoint_id: i64) -> Result<(), String> {
    http_delete(&format!("/events/{event_id}/endpoints/{endpoint_id}")).await
}

// Stream control API

pub async fn start_stream(event_id: i64) -> Result<(), String> {
    http_post(&format!("/events/{event_id}/start-stream")).await
}

pub async fn stop_stream(event_id: i64) -> Result<(), String> {
    http_post(&format!("/events/{event_id}/stop-stream")).await
}

/// Request body for updating an event (all fields optional).
#[derive(Debug, Clone, Serialize, Default)]
pub struct UpdateEventRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_delay_secs: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rescue_video_url: Option<String>,
}

pub async fn update_event(id: i64, req: &UpdateEventRequest) -> Result<(), String> {
    http_patch_json(&format!("/events/{id}"), req).await
}

// Delivery status API

/// Delivery status response from the HTTP API.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct DeliveryStatusResponse {
    pub instance_status: Option<String>,
    pub server_ip: Option<String>,
    pub server_ready: bool,
    pub endpoints_alive: bool,
    pub endpoint_details: Vec<DeliveryEndpointDetail>,
    pub instance: Option<DeliveryInstanceInfo>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DeliveryEndpointDetail {
    pub alias: String,
    pub alive: bool,
    pub current_chunk_id: i64,
    pub bytes_processed_total: i64,
    pub chunks_processed: i64,
    pub chunk_delay_secs: f64,
    #[serde(default)]
    pub stall_reason: Option<String>,
    #[serde(default)]
    pub ffmpeg_restart_count: u32,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub delivery_mode: Option<String>,
    #[serde(default)]
    pub rescue_eta_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct DeliveryInstanceInfo {
    pub name: String,
    pub status: String,
    pub ipv4: String,
}

pub async fn get_delivery_status(event_id: i64) -> Result<DeliveryStatusResponse, String> {
    http_get(&format!("/delivery/status?event_id={event_id}")).await
}

/// Cached delivery status response (matches WsEvent::DeliveryStatus shape).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct CachedDeliveryStatus {
    pub instance_name: String,
    pub status: String,
    pub server_ip: Option<String>,
    pub endpoint_count: u32,
    pub endpoints: Vec<CachedDeliveryEndpoint>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct CachedDeliveryEndpoint {
    pub alias: String,
    pub alive: bool,
    pub current_chunk_id: i64,
    pub bytes_processed_total: i64,
    pub chunks_processed: i64,
    pub chunk_delay_secs: f64,
    #[serde(default)]
    pub stall_reason: Option<String>,
    #[serde(default)]
    pub ffmpeg_restart_count: u32,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub is_fast: bool,
    #[serde(default)]
    pub delivery_mode: Option<String>,
    #[serde(default)]
    pub rescue_eta_secs: Option<u64>,
}

/// Get cached delivery status (instant, no VPS round-trip).
pub async fn get_delivery_status_cached() -> Result<CachedDeliveryStatus, String> {
    http_get("/delivery/status/cached").await
}

// YouTube health API

/// YouTube stream health info for dashboard display.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct YouTubeStatusResponse {
    pub authenticated: bool,
    #[serde(default)]
    pub stream_receiving: Option<bool>,
    #[serde(default)]
    pub streams: Vec<YouTubeStreamHealth>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct YouTubeStreamHealth {
    #[serde(default)]
    pub health_status: Option<String>,
    #[serde(default)]
    pub stream_status: String,
}

/// Fetch YouTube health status. Returns None on any error (non-critical polling).
pub async fn get_youtube_health() -> Option<YouTubeStatusResponse> {
    http_get::<YouTubeStatusResponse>("/youtube/status")
        .await
        .ok()
}

// OBS API

/// OBS status response from the HTTP API.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ObsStatusResponse {
    pub connected: bool,
    pub streaming: bool,
    pub recording: bool,
    #[serde(default)]
    pub stream_timecode: Option<String>,
}

/// Fetch OBS status. Returns None if OBS integration is disabled (503).
pub async fn get_obs_status() -> Option<ObsStatusResponse> {
    http_get::<ObsStatusResponse>("/obs/status").await.ok()
}

/// Tell OBS to start streaming.
pub async fn obs_start_stream() -> Result<(), String> {
    http_post("/obs/start-stream").await
}

/// Tell OBS to stop streaming.
pub async fn obs_stop_stream() -> Result<(), String> {
    http_post("/obs/stop-stream").await
}

// --- Config API ---

/// Fetch the current config (credentials redacted).
pub async fn get_config() -> Result<serde_json::Value, String> {
    http_get("/config").await
}

/// Patch the config with a partial JSON update.
pub async fn patch_config(body: &serde_json::Value) -> Result<(), String> {
    http_patch_json("/config", body).await
}

/// Add an endpoint to a running delivery VPS mid-stream.
pub async fn delivery_add_endpoint(
    event_id: i64,
    endpoint_id: i64,
    start_position: &str,
) -> Result<(), String> {
    let body = serde_json::json!({
        "event_id": event_id,
        "endpoint_id": endpoint_id,
        "start_position": { "strategy": start_position },
    });
    http_post_json("/delivery/endpoints/add", &body)
        .await
        .map(|_| ())
}

/// Remove an endpoint from a running delivery VPS mid-stream.
pub async fn delivery_remove_endpoint(event_id: i64, alias: &str) -> Result<(), String> {
    let body = serde_json::json!({
        "event_id": event_id,
        "alias": alias,
    });
    http_post_json("/delivery/endpoints/remove", &body)
        .await
        .map(|_| ())
}
