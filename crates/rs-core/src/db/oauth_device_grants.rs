//! CRUD for `oauth_device_grants` — transient state for pending Device Code
//! Flow grants. A row exists from `device-start` until the operator either
//! authorizes (row deleted, tokens persisted to `youtube_oauth`) or the flow
//! terminates (`status` set to `denied` / `expired` / `error`).

use crate::error::Result;
use sqlx::Row;
use sqlx::sqlite::SqlitePool;

#[derive(Debug, Clone)]
pub struct DeviceGrant {
    pub label: String,
    pub device_code: String,
    pub user_code: String,
    pub verification_url: String,
    pub interval_secs: i64,
    pub expires_at: String,
    pub status: String,
    pub error: Option<String>,
    pub started_at: String,
}

fn row_to_grant(r: sqlx::sqlite::SqliteRow) -> DeviceGrant {
    DeviceGrant {
        label: r.get("label"),
        device_code: r.get("device_code"),
        user_code: r.get("user_code"),
        verification_url: r.get("verification_url"),
        interval_secs: r.get("interval_secs"),
        expires_at: r.get("expires_at"),
        status: r.get("status"),
        error: r.get("error"),
        started_at: r.get("started_at"),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn insert(
    pool: &SqlitePool,
    label: &str,
    device_code: &str,
    user_code: &str,
    verification_url: &str,
    interval_secs: i64,
    expires_at: &str,
    started_at: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO oauth_device_grants
            (label, device_code, user_code, verification_url, interval_secs, expires_at, status, error, started_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', NULL, ?7)
         ON CONFLICT(label) DO UPDATE SET
            device_code = excluded.device_code,
            user_code = excluded.user_code,
            verification_url = excluded.verification_url,
            interval_secs = excluded.interval_secs,
            expires_at = excluded.expires_at,
            status = 'pending',
            error = NULL,
            started_at = excluded.started_at",
    )
    .bind(label)
    .bind(device_code)
    .bind(user_code)
    .bind(verification_url)
    .bind(interval_secs)
    .bind(expires_at)
    .bind(started_at)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_by_label(pool: &SqlitePool, label: &str) -> Result<Option<DeviceGrant>> {
    let row = sqlx::query("SELECT * FROM oauth_device_grants WHERE label = ?1")
        .bind(label)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(row_to_grant))
}

pub async fn list_pending(pool: &SqlitePool) -> Result<Vec<DeviceGrant>> {
    let rows = sqlx::query("SELECT * FROM oauth_device_grants WHERE status = 'pending' ORDER BY started_at")
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(row_to_grant).collect())
}

pub async fn update_status(
    pool: &SqlitePool,
    label: &str,
    status: &str,
    error: Option<&str>,
) -> Result<()> {
    sqlx::query("UPDATE oauth_device_grants SET status = ?1, error = ?2 WHERE label = ?3")
        .bind(status)
        .bind(error)
        .bind(label)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete(pool: &SqlitePool, label: &str) -> Result<()> {
    sqlx::query("DELETE FROM oauth_device_grants WHERE label = ?1")
        .bind(label)
        .execute(pool)
        .await?;
    Ok(())
}
