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
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StreamingEvent {
    pub id: i64,
    pub identifier: Option<String>,
    pub short_description: Option<String>,
    pub date_of_event: String,
    pub server_ip: String,
    pub received_bytes: i64,
    pub receiving_activated: bool,
    pub delivering_activated: bool,
}

/// Chunk statistics from the backend.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChunkStats {
    pub total_chunks: i64,
    pub pending_chunks: i64,
    pub sent_chunks: i64,
    pub in_process_chunks: i64,
    pub total_bytes: i64,
    pub buffer_duration_secs: f64,
}

/// Combined status response.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatusResponse {
    pub streaming_event: Option<StreamingEvent>,
    pub chunk_stats: ChunkStats,
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
pub async fn get_status() -> Result<StatusResponse, String> {
    let result: CommandResult<StatusResponse> = invoke("get_status", JsValue::NULL).await?;

    if result.success {
        result.data.ok_or_else(|| "No data returned".to_string())
    } else {
        Err(result.error.unwrap_or_else(|| "Unknown error".to_string()))
    }
}

/// Get chunk statistics.
pub async fn get_chunk_stats() -> Result<ChunkStats, String> {
    let result: CommandResult<ChunkStats> = invoke("get_chunk_stats", JsValue::NULL).await?;

    if result.success {
        result.data.ok_or_else(|| "No data returned".to_string())
    } else {
        Err(result.error.unwrap_or_else(|| "Unknown error".to_string()))
    }
}

/// Get the current streaming event.
pub async fn get_streaming_event() -> Result<Option<StreamingEvent>, String> {
    let result: CommandResult<Option<StreamingEvent>> =
        invoke("get_streaming_event", JsValue::NULL).await?;

    if result.success {
        Ok(result.data.flatten())
    } else {
        Err(result.error.unwrap_or_else(|| "Unknown error".to_string()))
    }
}

/// Get recent log entries for a component.
pub async fn get_logs(component: &str, limit: usize) -> Result<Vec<LogEntry>, String> {
    #[derive(Serialize)]
    struct Args {
        component: String,
        limit: Option<usize>,
    }

    let args = serde_wasm_bindgen::to_value(&Args {
        component: component.to_string(),
        limit: Some(limit),
    })
    .map_err(|e| e.to_string())?;

    let result: CommandResult<Vec<LogEntry>> = invoke("get_logs", args).await?;

    if result.success {
        result.data.ok_or_else(|| "No data returned".to_string())
    } else {
        Err(result.error.unwrap_or_else(|| "Unknown error".to_string()))
    }
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

const API_BASE: &str = "http://127.0.0.1:8910/api/v1";

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

/// Scheduled stream.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ScheduledStream {
    pub id: i64,
    pub event_id: i64,
    pub start_time: String,
    pub repeat_interval: Option<String>,
    pub last_run_at: Option<String>,
    pub next_run_at: Option<String>,
    pub enabled: bool,
}

async fn http_get<T: for<'de> Deserialize<'de>>(path: &str) -> Result<T, String> {
    let url = format!("{API_BASE}{path}");
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
    let url = format!("{API_BASE}{path}");
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
    let url = format!("{API_BASE}{path}");
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
    let url = format!("{API_BASE}{path}");
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

pub async fn create_event(identifier: &str) -> Result<serde_json::Value, String> {
    #[derive(Serialize)]
    struct Body {
        identifier: String,
    }
    http_post_json(
        "/events",
        &Body {
            identifier: identifier.to_string(),
        },
    )
    .await
}

pub async fn activate_event(id: i64) -> Result<(), String> {
    http_post(&format!("/events/{id}/activate")).await
}

pub async fn start_delivering(id: i64) -> Result<(), String> {
    http_post(&format!("/events/{id}/start-delivering")).await
}

pub async fn deactivate_event(id: i64) -> Result<(), String> {
    http_post(&format!("/events/{id}/deactivate")).await
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

// Schedules API
pub async fn list_schedules() -> Result<Vec<ScheduledStream>, String> {
    http_get("/schedules").await
}

pub async fn delete_schedule(id: i64) -> Result<(), String> {
    http_delete(&format!("/schedules/{id}")).await
}
