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
    // Capture `(id, ts, &row)` in one INSERT RETURNING per row so the
    // post-commit broadcast doesn't need to SELECT ts separately — that
    // used to be an N+1 read after the transaction committed.
    let mut inserted: Vec<(i64, String, &AuditRow)> = Vec::with_capacity(rows.len());

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

        let (id, ts): (i64, String) = if let Some(ts_override) = &row.ts_override {
            sqlx::query_as(
                "INSERT INTO audit_log (ts, severity, source, event_id, instance_id, endpoint, action, detail)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) RETURNING id, ts"
            )
            .bind(ts_override)
            .bind(&severity).bind(&source)
            .bind(row.event_id).bind(row.instance_id).bind(row.endpoint.as_deref())
            .bind(&action).bind(&detail)
            .fetch_one(&mut *tx).await?
        } else {
            sqlx::query_as(
                "INSERT INTO audit_log (severity, source, event_id, instance_id, endpoint, action, detail)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) RETURNING id, ts"
            )
            .bind(&severity).bind(&source)
            .bind(row.event_id).bind(row.instance_id).bind(row.endpoint.as_deref())
            .bind(&action).bind(&detail)
            .fetch_one(&mut *tx).await?
        };
        inserted.push((id, ts, row));
    }
    tx.commit().await?;

    // Broadcast post-commit so subscribers see durable state. `ts` was
    // captured by the INSERT RETURNING above, so no extra SELECT round-trips.
    for (id, ts, row) in inserted {
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
    Ok(())
}

/// Typed bind value — INTEGER columns must bind as i64 so SQLite uses
/// integer indexes (e.g. `idx_audit_event ON audit_log(event_id, ts DESC)`).
/// Binding an integer column as TEXT silently bypasses those indexes and
/// degrades to a full table scan on larger retention windows.
enum BindValue {
    Int(i64),
    Str(String),
}

pub async fn query(pool: &SqlitePool, f: Filter) -> Result<Vec<AuditLogRow>> {
    let mut sql = String::from(
        "SELECT id, ts, severity, source, event_id, instance_id, endpoint, action, detail FROM audit_log WHERE 1=1",
    );
    let mut binds: Vec<BindValue> = Vec::new();

    if let Some(ev) = f.event_id {
        sql.push_str(&format!(" AND event_id = ?{}", binds.len() + 1));
        binds.push(BindValue::Int(ev));
    }
    if let Some(inst) = f.instance_id {
        sql.push_str(&format!(" AND instance_id = ?{}", binds.len() + 1));
        binds.push(BindValue::Int(inst));
    }
    if let Some(ep) = &f.endpoint {
        sql.push_str(&format!(" AND endpoint = ?{}", binds.len() + 1));
        binds.push(BindValue::Str(ep.clone()));
    }
    if !f.severities.is_empty() {
        let placeholders: Vec<String> = f
            .severities
            .iter()
            .enumerate()
            .map(|(i, _)| format!("?{}", binds.len() + i + 1))
            .collect();
        sql.push_str(&format!(" AND severity IN ({})", placeholders.join(",")));
        binds.extend(f.severities.iter().cloned().map(BindValue::Str));
    }
    if !f.sources.is_empty() {
        let placeholders: Vec<String> = f
            .sources
            .iter()
            .enumerate()
            .map(|(i, _)| format!("?{}", binds.len() + i + 1))
            .collect();
        sql.push_str(&format!(" AND source IN ({})", placeholders.join(",")));
        binds.extend(f.sources.iter().cloned().map(BindValue::Str));
    }
    if let Some(s) = &f.since {
        sql.push_str(&format!(" AND ts >= ?{}", binds.len() + 1));
        binds.push(BindValue::Str(s.clone()));
    }
    if let Some(u) = &f.until {
        sql.push_str(&format!(" AND ts <= ?{}", binds.len() + 1));
        binds.push(BindValue::Str(u.clone()));
    }

    sql.push_str(" ORDER BY id DESC");
    sql.push_str(&format!(" LIMIT {}", f.limit.unwrap_or(200).clamp(1, 5000)));
    if let Some(off) = f.offset {
        sql.push_str(&format!(" OFFSET {}", off.max(0)));
    }

    let mut q = sqlx::query(&sql);
    for b in binds {
        q = match b {
            BindValue::Int(i) => q.bind(i),
            BindValue::Str(s) => q.bind(s),
        };
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

/// One grouped audit-log entry (issue #169). Collapses consecutive
/// rows that share `(source, action, endpoint)` and fall within
/// `window_secs` of each other. Dashboard activity feed uses this to
/// hide repeat-burst noise (e.g. 25 `endpoint_rtmp_push_died` rows in
/// 5 min becomes one row showing `count=25`, span first_ts → last_ts,
/// with one representative `sample_detail` for drill-down).
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct GroupedAuditRow {
    pub source: String,
    pub action: String,
    pub endpoint: Option<String>,
    pub severity: String,
    pub count: u32,
    pub first_ts: String,
    pub last_ts: String,
    /// Representative detail JSON (the LAST row in the group). Operator
    /// drills into this for forensic detail; the older rows in the
    /// group are reachable via the ungrouped `/api/v1/audit` endpoint.
    pub sample_detail: serde_json::Value,
    pub sample_id: i64,
}

/// Pure fn — group consecutive same-key audit rows within `window_secs`.
///
/// Input rows must be sorted DESCENDING by ts (newest first), the same
/// order returned by [`query`]. Output preserves that order: newest
/// group first.
///
/// Two rows belong to the same group iff:
/// - `(source, action, endpoint)` match exactly, AND
/// - the more-recent row's ts is at most `window_secs` after the
///   currently-open group's earliest row.
///
/// `window_secs == 0` disables grouping entirely (every row becomes a
/// singleton group). The dashboard typically uses 60.
pub fn group_audit_rows(rows: Vec<AuditLogRow>, window_secs: i64) -> Vec<GroupedAuditRow> {
    if rows.is_empty() {
        return Vec::new();
    }
    if window_secs == 0 {
        return rows
            .into_iter()
            .map(|r| GroupedAuditRow {
                source: r.source.clone(),
                action: r.action.clone(),
                endpoint: r.endpoint.clone(),
                severity: r.severity.clone(),
                count: 1,
                first_ts: r.ts.clone(),
                last_ts: r.ts.clone(),
                sample_detail: r.detail,
                sample_id: r.id,
            })
            .collect();
    }
    let parse = |s: &str| -> Option<chrono::DateTime<chrono::Utc>> {
        chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|d| d.with_timezone(&chrono::Utc))
            .or_else(|| {
                chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    .ok()
                    .map(|n| n.and_utc())
                    .or_else(|| {
                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ")
                            .ok()
                            .map(|n| n.and_utc())
                    })
            })
    };

    let mut out: Vec<GroupedAuditRow> = Vec::new();
    for row in rows {
        // Match against the LAST open group of the same key, which (for
        // newest-first input) is the most recently appended group.
        let key_match = out.iter_mut().rev().find(|g| {
            g.source == row.source && g.action == row.action && g.endpoint == row.endpoint
        });
        if let Some(g) = key_match {
            // Compare row.ts against g.first_ts (the EARLIEST = oldest
            // ts in the group when input is newest-first).
            let group_oldest_ts = parse(&g.first_ts);
            let row_ts = parse(&row.ts);
            if let (Some(go), Some(rt)) = (group_oldest_ts, row_ts) {
                let delta = (go - rt).num_seconds().abs();
                if delta <= window_secs {
                    g.count = g.count.saturating_add(1);
                    g.first_ts = row.ts.clone();
                    continue;
                }
            }
        }
        out.push(GroupedAuditRow {
            source: row.source.clone(),
            action: row.action.clone(),
            endpoint: row.endpoint.clone(),
            severity: row.severity.clone(),
            count: 1,
            first_ts: row.ts.clone(),
            last_ts: row.ts.clone(),
            sample_detail: row.detail,
            sample_id: row.id,
        });
    }
    out
}
