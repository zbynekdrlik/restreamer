//! `audit_log` DB access.

use crate::audit::AuditRow;
use crate::error::Result;
use crate::models::WsEvent;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use tokio::sync::broadcast;

/// Row as returned from the DB. `detail` is the parsed JSON, not raw text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogRow {
    pub id: i64,
    pub ts: String,
    pub severity: String,
    pub source: String,
    pub event_id: Option<i64>,
    pub instance_id: Option<i64>,
    pub endpoint: Option<String>,
    pub action: String,
    pub detail: serde_json::Value,
}

/// Filter for `query`. All fields optional.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    pub event_id: Option<i64>,
    pub instance_id: Option<i64>,
    pub endpoint: Option<String>,
    pub severities: Vec<String>,
    pub sources: Vec<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

pub async fn insert_batch(
    pool: &SqlitePool,
    rows: &[AuditRow],
    ws_tx: &broadcast::Sender<WsEvent>,
) -> Result<()> {
    let mut tx = pool.begin().await?;
    let mut inserted: Vec<(i64, &AuditRow)> = Vec::with_capacity(rows.len());

    for row in rows {
        let severity = serde_json::to_string(&row.severity)
            .unwrap_or_else(|_| "\"info\"".into())
            .trim_matches('"')
            .to_string();
        let source = serde_json::to_string(&row.source)
            .unwrap_or_else(|_| "\"system\"".into())
            .trim_matches('"')
            .to_string();
        let action = serde_json::to_string(&row.action)
            .unwrap_or_else(|_| "\"unknown\"".into())
            .trim_matches('"')
            .to_string();
        let detail = row.detail.to_string();

        let id: i64 = if let Some(ts) = &row.ts_override {
            sqlx::query_scalar(
                "INSERT INTO audit_log (ts, severity, source, event_id, instance_id, endpoint, action, detail)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) RETURNING id"
            )
            .bind(ts)
            .bind(&severity).bind(&source)
            .bind(row.event_id).bind(row.instance_id).bind(row.endpoint.as_deref())
            .bind(&action).bind(&detail)
            .fetch_one(&mut *tx).await?
        } else {
            sqlx::query_scalar(
                "INSERT INTO audit_log (severity, source, event_id, instance_id, endpoint, action, detail)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) RETURNING id"
            )
            .bind(&severity).bind(&source)
            .bind(row.event_id).bind(row.instance_id).bind(row.endpoint.as_deref())
            .bind(&action).bind(&detail)
            .fetch_one(&mut *tx).await?
        };
        inserted.push((id, row));
    }
    tx.commit().await?;

    // Broadcast post-commit so subscribers see durable state.
    for (id, row) in inserted {
        if let Ok(ts) = sqlx::query_scalar::<_, String>("SELECT ts FROM audit_log WHERE id = ?1")
            .bind(id)
            .fetch_one(pool)
            .await
        {
            let severity = serde_json::to_string(&row.severity)
                .unwrap_or_default()
                .trim_matches('"')
                .to_string();
            let source = serde_json::to_string(&row.source)
                .unwrap_or_default()
                .trim_matches('"')
                .to_string();
            let action = serde_json::to_string(&row.action)
                .unwrap_or_default()
                .trim_matches('"')
                .to_string();
            let _ = ws_tx.send(WsEvent::AuditAppended {
                id,
                ts,
                severity,
                source,
                event_id: row.event_id,
                instance_id: row.instance_id,
                endpoint: row.endpoint.clone(),
                action,
                detail: row.detail.clone(),
            });
        }
    }
    Ok(())
}

pub async fn query(pool: &SqlitePool, f: Filter) -> Result<Vec<AuditLogRow>> {
    let mut sql = String::from(
        "SELECT id, ts, severity, source, event_id, instance_id, endpoint, action, detail FROM audit_log WHERE 1=1",
    );
    let mut binds: Vec<String> = Vec::new();

    if let Some(ev) = f.event_id {
        sql.push_str(&format!(" AND event_id = ?{}", binds.len() + 1));
        binds.push(ev.to_string());
    }
    if let Some(inst) = f.instance_id {
        sql.push_str(&format!(" AND instance_id = ?{}", binds.len() + 1));
        binds.push(inst.to_string());
    }
    if let Some(ep) = &f.endpoint {
        sql.push_str(&format!(" AND endpoint = ?{}", binds.len() + 1));
        binds.push(ep.clone());
    }
    if !f.severities.is_empty() {
        let placeholders: Vec<String> = f
            .severities
            .iter()
            .enumerate()
            .map(|(i, _)| format!("?{}", binds.len() + i + 1))
            .collect();
        sql.push_str(&format!(" AND severity IN ({})", placeholders.join(",")));
        binds.extend(f.severities.iter().cloned());
    }
    if !f.sources.is_empty() {
        let placeholders: Vec<String> = f
            .sources
            .iter()
            .enumerate()
            .map(|(i, _)| format!("?{}", binds.len() + i + 1))
            .collect();
        sql.push_str(&format!(" AND source IN ({})", placeholders.join(",")));
        binds.extend(f.sources.iter().cloned());
    }
    if let Some(s) = &f.since {
        sql.push_str(&format!(" AND ts >= ?{}", binds.len() + 1));
        binds.push(s.clone());
    }
    if let Some(u) = &f.until {
        sql.push_str(&format!(" AND ts <= ?{}", binds.len() + 1));
        binds.push(u.clone());
    }

    sql.push_str(" ORDER BY id DESC");
    sql.push_str(&format!(" LIMIT {}", f.limit.unwrap_or(200).clamp(1, 5000)));
    if let Some(off) = f.offset {
        sql.push_str(&format!(" OFFSET {}", off.max(0)));
    }

    let mut q = sqlx::query(&sql);
    for b in &binds {
        q = q.bind(b);
    }
    let rows = q.fetch_all(pool).await?;

    Ok(rows
        .into_iter()
        .map(|r| AuditLogRow {
            id: r.get("id"),
            ts: r.get("ts"),
            severity: r.get("severity"),
            source: r.get("source"),
            event_id: r.get("event_id"),
            instance_id: r.get("instance_id"),
            endpoint: r.get("endpoint"),
            action: r.get("action"),
            detail: serde_json::from_str(&r.get::<String, _>("detail"))
                .unwrap_or(serde_json::json!({})),
        })
        .collect())
}

pub async fn get_by_id(pool: &SqlitePool, id: i64) -> Result<Option<AuditLogRow>> {
    let row = sqlx::query(
        "SELECT id, ts, severity, source, event_id, instance_id, endpoint, action, detail FROM audit_log WHERE id = ?1"
    ).bind(id).fetch_optional(pool).await?;
    Ok(row.map(|r| AuditLogRow {
        id: r.get("id"),
        ts: r.get("ts"),
        severity: r.get("severity"),
        source: r.get("source"),
        event_id: r.get("event_id"),
        instance_id: r.get("instance_id"),
        endpoint: r.get("endpoint"),
        action: r.get("action"),
        detail: serde_json::from_str(&r.get::<String, _>("detail"))
            .unwrap_or(serde_json::json!({})),
    }))
}

pub async fn rotate(pool: &SqlitePool, keep_days: i64) -> Result<i64> {
    let res = sqlx::query("DELETE FROM audit_log WHERE ts < datetime('now', ?1)")
        .bind(format!("-{keep_days} days"))
        .execute(pool)
        .await?;
    Ok(res.rows_affected() as i64)
}
