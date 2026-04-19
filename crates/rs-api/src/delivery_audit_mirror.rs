//! VPS audit mirroring: pull `/api/status?since=<cursor>` rows from the
//! delivery VPS and insert them into the host `audit_log` table, advancing
//! `delivery_instances.last_audit_cursor` on success.
//!
//! Split from `delivery.rs` to keep that file under the 1000-line file-size gate.

use std::time::Duration;

use sqlx::SqlitePool;

/// Pull audit rows from the VPS (`/api/status?since=<cursor>`) and mirror them
/// into the host `audit_log` preserving source/action/endpoint/ts.
/// Advances `delivery_instances.last_audit_cursor` on success.
///
/// Errors are best-effort: caller should swallow with `.ok()` because the
/// VPS may be unreachable for reasons outside our control, and the next
/// poll tick will retry. Successful mirrors fire `WsEvent::AuditAppended`
/// for each inserted row when `audit_tx` is `Some` (preferred path).
pub async fn mirror_vps_audit(
    pool: &SqlitePool,
    instance_id: i64,
    audit_tx: Option<&tokio::sync::mpsc::Sender<rs_core::audit::AuditRow>>,
) -> anyhow::Result<()> {
    use rs_core::audit::{Action, AuditRow, Severity, Source};

    let instance = rs_core::db::get_delivery_instance(pool, instance_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("instance {instance_id} not found"))?;
    let cursor: i64 =
        sqlx::query_scalar("SELECT last_audit_cursor FROM delivery_instances WHERE id = ?1")
            .bind(instance_id)
            .fetch_one(pool)
            .await?;

    let url = format!("http://{}:8000/api/status?since={cursor}", instance.ipv4);
    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .bearer_auth(&instance.auth_token)
        .timeout(Duration::from_secs(5))
        .send()
        .await?;
    if !resp.status().is_success() {
        return Ok(());
    }

    #[derive(serde::Deserialize)]
    struct StatusBody {
        #[serde(default)]
        recent_audit: Vec<serde_json::Value>,
        #[serde(default)]
        next_audit_cursor: i64,
    }
    let body: StatusBody = resp.json().await?;
    if body.recent_audit.is_empty() {
        return Ok(());
    }

    for r in &body.recent_audit {
        let ts = r["ts"].as_str().unwrap_or("").to_string();
        let severity: Severity =
            serde_json::from_value(r["severity"].clone()).unwrap_or(Severity::Info);
        let source: Source = serde_json::from_value(r["source"].clone()).unwrap_or(Source::Vps);
        let action: Action =
            serde_json::from_value(r["action"].clone()).unwrap_or(Action::EndpointStarted);
        let endpoint = r["endpoint"].as_str().map(|s| s.to_string());
        let detail = r["detail"].clone();

        if let Some(tx) = audit_tx {
            rs_core::audit::record(
                tx,
                AuditRow {
                    severity,
                    source,
                    event_id: instance.event_id,
                    instance_id: Some(instance_id),
                    endpoint,
                    action,
                    detail,
                    ts_override: if ts.is_empty() { None } else { Some(ts) },
                },
            );
        } else {
            // Synchronous insert fallback (used when AppState does not yet
            // carry an audit channel — see Task 27 wire-up).
            let (ws_tx, _rx) = tokio::sync::broadcast::channel::<rs_core::models::WsEvent>(16);
            let rows = vec![AuditRow {
                severity,
                source,
                event_id: instance.event_id,
                instance_id: Some(instance_id),
                endpoint,
                action,
                detail,
                ts_override: if ts.is_empty() { None } else { Some(ts) },
            }];
            rs_core::db::audit::insert_batch(pool, &rows, &ws_tx).await?;
        }
    }

    sqlx::query("UPDATE delivery_instances SET last_audit_cursor = ?1 WHERE id = ?2")
        .bind(body.next_audit_cursor)
        .bind(instance_id)
        .execute(pool)
        .await?;
    Ok(())
}
