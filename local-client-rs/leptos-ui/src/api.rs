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
    Ok(StatusResponse {
        streaming_event: event,
        chunk_stats,
        inpoint_connected,
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
    let _ = http_post_json("/delivery/start", &body).await;
    Ok(())
}

pub async fn deactivate_event(id: i64) -> Result<(), String> {
    http_post(&format!("/events/{id}/deactivate")).await
}

pub async fn delete_event(id: i64) -> Result<(), String> {
    http_delete(&format!("/events/{id}")).await
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
