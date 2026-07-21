//! HTTP handlers for audit log queries.

use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use rs_core::db::audit::{self, Filter};
use serde::Deserialize;

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    #[serde(default)]
    pub event_id: Option<i64>,
    #[serde(default)]
    pub instance_id: Option<i64>,
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Comma-separated: "info,warn,error,critical"
    #[serde(default)]
    pub severity: Option<String>,
    /// Comma-separated sources
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub since: Option<String>,
    #[serde(default)]
    pub until: Option<String>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub offset: Option<i64>,
    /// #169: when true, collapse consecutive same-`(source, action, endpoint)`
    /// bursts into one row (count + first→last span) via
    /// [`audit::group_audit_rows`]. Response then carries `grouped` instead of
    /// `rows`. The live dashboard groups client-side (so live WS rows collapse
    /// too); this server surface is for API consumers / non-live queries.
    #[serde(default)]
    pub group: Option<bool>,
    /// Grouping window in seconds (default 60). Ignored unless `group=true`.
    #[serde(default)]
    pub group_window_secs: Option<i64>,
}

pub async fn list(State(state): State<AppState>, Query(q): Query<ListQuery>) -> impl IntoResponse {
    let severities: Vec<String> = q
        .severity
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let sources: Vec<String> = q
        .source
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    let filter = Filter {
        event_id: q.event_id,
        instance_id: q.instance_id,
        endpoint: q.endpoint,
        action: q.action,
        severities,
        sources,
        since: q.since,
        until: q.until,
        limit: q.limit,
        offset: q.offset,
    };

    let want_group = q.group.unwrap_or(false);
    let group_window = q.group_window_secs.unwrap_or(60);

    match audit::query(&state.pool, filter).await {
        Ok(rows) => {
            let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM audit_log")
                .fetch_one(&state.pool)
                .await
                .unwrap_or(0);
            if want_group {
                let grouped = audit::group_audit_rows(rows, group_window);
                Json(serde_json::json!({ "grouped": grouped, "total": total })).into_response()
            } else {
                Json(serde_json::json!({ "rows": rows, "total": total })).into_response()
            }
        }
        Err(e) => {
            tracing::error!("audit list failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "audit list failed").into_response()
        }
    }
}

pub async fn get_one(Path(id): Path<i64>, State(state): State<AppState>) -> impl IntoResponse {
    match audit::get_by_id(&state.pool, id).await {
        Ok(Some(row)) => Json(row).into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "not found").into_response(),
        Err(e) => {
            tracing::error!("audit get_by_id failed: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "audit get failed").into_response()
        }
    }
}
