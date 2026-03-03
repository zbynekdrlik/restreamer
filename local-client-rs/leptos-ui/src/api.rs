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
