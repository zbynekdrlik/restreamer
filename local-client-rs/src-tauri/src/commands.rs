//! Tauri commands for the embedded service.
//!
//! These commands provide direct access to the service state without HTTP.

use std::sync::Arc;

use serde::Serialize;
use tauri::State;

use rs_core::log_buffer::LogEntry;
use rs_core::models::{ChunkStats, StreamingEvent};

use crate::state::AppState;

#[derive(Debug, Serialize)]
pub struct CommandResult<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T> CommandResult<T> {
    fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    fn err(error: String) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(error),
        }
    }
}

/// Combined status response for the dashboard.
#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub streaming_event: Option<StreamingEvent>,
    pub chunk_stats: ChunkStats,
}

/// Get the current service status including streaming event and chunk stats.
#[tauri::command]
pub async fn get_status(
    state: State<'_, Arc<AppState>>,
) -> Result<CommandResult<StatusResponse>, ()> {
    let streaming_event = match state.get_streaming_event().await {
        Ok(e) => e,
        Err(e) => return Ok(CommandResult::err(e)),
    };

    let chunk_stats = match state.get_chunk_stats().await {
        Ok(s) => s,
        Err(e) => return Ok(CommandResult::err(e)),
    };

    Ok(CommandResult::ok(StatusResponse {
        streaming_event,
        chunk_stats,
    }))
}

/// Get chunk statistics.
#[tauri::command]
pub async fn get_chunk_stats(
    state: State<'_, Arc<AppState>>,
) -> Result<CommandResult<ChunkStats>, ()> {
    match state.get_chunk_stats().await {
        Ok(stats) => Ok(CommandResult::ok(stats)),
        Err(e) => Ok(CommandResult::err(e)),
    }
}

/// Get the current streaming event.
#[tauri::command]
pub async fn get_streaming_event(
    state: State<'_, Arc<AppState>>,
) -> Result<CommandResult<Option<StreamingEvent>>, ()> {
    match state.get_streaming_event().await {
        Ok(event) => Ok(CommandResult::ok(event)),
        Err(e) => Ok(CommandResult::err(e)),
    }
}

/// Get recent log entries for a component (inpoint or endpoint).
#[tauri::command]
pub fn get_logs(
    state: State<'_, Arc<AppState>>,
    component: String,
    limit: Option<usize>,
) -> CommandResult<Vec<LogEntry>> {
    let limit = limit.unwrap_or(100);
    let logs = state.get_logs(&component, limit);
    CommandResult::ok(logs)
}

/// Get the current configuration.
#[tauri::command]
pub fn get_config(state: State<'_, Arc<AppState>>) -> CommandResult<serde_json::Value> {
    match serde_json::to_value(state.config()) {
        Ok(config) => CommandResult::ok(config),
        Err(e) => CommandResult::err(e.to_string()),
    }
}
