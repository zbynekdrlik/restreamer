//! HTTP handler for GET /api/v1/delivery/metrics.

use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use rs_core::db::metrics::{self, Filter};
use serde::Deserialize;

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub event_id: i64,
    #[serde(default)]
    pub alias: Option<String>,
    #[serde(default)]
    pub since_ms: Option<i64>,
    #[serde(default)]
    pub until_ms: Option<i64>,
    #[serde(default)]
    pub limit: Option<i64>,
}

pub async fn list(State(state): State<AppState>, Query(q): Query<ListQuery>) -> impl IntoResponse {
    let f = Filter {
        event_id: Some(q.event_id),
        alias: q.alias,
        since_ms: q.since_ms,
        until_ms: q.until_ms,
        limit: q.limit,
    };
    match metrics::query(&state.pool, f).await {
        Ok(rows) => Json(serde_json::json!({ "rows": rows })).into_response(),
        Err(e) => {
            tracing::error!("metrics query failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "metrics query failed").into_response()
        }
    }
}
