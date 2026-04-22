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
/// poll tick will retry. Rows are sent through the provided `audit_tx` so
/// they land in the `audit_log` table AND broadcast live via
/// `WsEvent::AuditAppended` through the shared writer pipeline.
///
/// On unreachable VPS a single `VpsAuditMirrorFailed` diagnostic row is
/// emitted (rate-limited by the writer's dedup key) so operators see why
/// the VPS panel is not updating, instead of a silent `Ok(())`.
pub async fn mirror_vps_audit(
    pool: &SqlitePool,
    instance_id: i64,
    audit_tx: &tokio::sync::mpsc::Sender<rs_core::audit::AuditRow>,
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
    let resp = match client
        .get(&url)
        .bearer_auth(&instance.auth_token)
        .timeout(Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            // Emit one diagnostic row per unreachable instance so the
            // operator sees WHY the VPS panel is frozen. The writer's
            // rate-limiter dedups by (instance_id, action) so a persistent
            // outage does not flood the audit log.
            rs_core::audit::record(
                audit_tx,
                AuditRow {
                    severity: Severity::Warn,
                    source: Source::System,
                    event_id: instance.event_id,
                    instance_id: Some(instance_id),
                    endpoint: None,
                    action: Action::VpsUnreachable,
                    detail: serde_json::json!({
                        "phase": "mirror",
                        "error": e.to_string(),
                    }),
                    ts_override: None,
                },
            );
            return Ok(());
        }
    };
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
        // Strict parse: skip rows the host can't interpret rather than
        // silently coercing to default variants (which would corrupt the
        // audit log with rows that look legitimate but misclassify the
        // source event).
        let Ok(severity) = serde_json::from_value::<Severity>(r["severity"].clone()) else {
            tracing::warn!(row = ?r, "mirror_vps_audit: unknown severity, skipping row");
            continue;
        };
        let Ok(source) = serde_json::from_value::<Source>(r["source"].clone()) else {
            tracing::warn!(row = ?r, "mirror_vps_audit: unknown source, skipping row");
            continue;
        };
        let Ok(action) = serde_json::from_value::<Action>(r["action"].clone()) else {
            tracing::warn!(row = ?r, "mirror_vps_audit: unknown action, skipping row");
            continue;
        };
        let endpoint = r["endpoint"].as_str().map(|s| s.to_string());
        let detail = r["detail"].clone();

        rs_core::audit::record(
            audit_tx,
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
    }

    sqlx::query("UPDATE delivery_instances SET last_audit_cursor = ?1 WHERE id = ?2")
        .bind(body.next_audit_cursor)
        .bind(instance_id)
        .execute(pool)
        .await?;
    Ok(())
}
