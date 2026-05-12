use chrono::Utc;
use sqlx::Row;
use sqlx::sqlite::SqlitePool;

use crate::error::{CoreError, Result};
use crate::models::{
    DeliveryEndpointStatus, DeliveryInstance, EndpointConfig, PusherKind, StreamingEvent,
    YouTubeOAuth,
};

/// Parse a `pusher` TEXT column value from the database into `PusherKind`.
/// Unknown values default to `Ffmpeg` so existing rows are never broken.
fn parse_pusher_kind(s: String) -> PusherKind {
    match s.as_str() {
        "rust" => PusherKind::Rust,
        _ => PusherKind::Ffmpeg,
    }
}

// --- Endpoint Configs ---

pub async fn list_endpoint_configs(pool: &SqlitePool) -> Result<Vec<EndpointConfig>> {
    let rows = sqlx::query(
        "SELECT id, alias, service_type, stream_key, enabled, position_last,
         delivered_bytes, is_fast, pusher, youtube_oauth_id, created_at, updated_at
         FROM endpoint_configs ORDER BY id",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| EndpointConfig {
            id: r.get("id"),
            alias: r.get("alias"),
            service_type: r.get("service_type"),
            stream_key: r.get("stream_key"),
            enabled: r.get::<i32, _>("enabled") != 0,
            position_last: r.get("position_last"),
            delivered_bytes: r.get("delivered_bytes"),
            is_fast: r.get::<i32, _>("is_fast") != 0,
            pusher: parse_pusher_kind(r.get("pusher")),
            prefetch_chunks: None,
            youtube_oauth_id: r.get("youtube_oauth_id"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        })
        .collect())
}

pub async fn get_endpoint_config(pool: &SqlitePool, id: i64) -> Result<Option<EndpointConfig>> {
    let row = sqlx::query(
        "SELECT id, alias, service_type, stream_key, enabled, position_last,
         delivered_bytes, is_fast, pusher, youtube_oauth_id, created_at, updated_at
         FROM endpoint_configs WHERE id = ?1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| EndpointConfig {
        id: r.get("id"),
        alias: r.get("alias"),
        service_type: r.get("service_type"),
        stream_key: r.get("stream_key"),
        enabled: r.get::<i32, _>("enabled") != 0,
        position_last: r.get("position_last"),
        delivered_bytes: r.get("delivered_bytes"),
        is_fast: r.get::<i32, _>("is_fast") != 0,
        pusher: parse_pusher_kind(r.get("pusher")),
        prefetch_chunks: None,
        youtube_oauth_id: r.get("youtube_oauth_id"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
    }))
}

pub async fn create_endpoint_config(
    pool: &SqlitePool,
    alias: &str,
    service_type: &str,
    stream_key: &str,
    is_fast: bool,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key, is_fast)
         VALUES (?1, ?2, ?3, ?4) RETURNING id",
    )
    .bind(alias)
    .bind(service_type)
    .bind(stream_key)
    .bind(is_fast as i32)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

#[allow(clippy::too_many_arguments)]
pub async fn update_endpoint_config(
    pool: &SqlitePool,
    id: i64,
    alias: &str,
    service_type: &str,
    stream_key: &str,
    enabled: bool,
    is_fast: bool,
) -> Result<()> {
    sqlx::query(
        "UPDATE endpoint_configs SET alias = ?1, service_type = ?2, stream_key = ?3,
         enabled = ?4, is_fast = ?5, updated_at = datetime('now') WHERE id = ?6",
    )
    .bind(alias)
    .bind(service_type)
    .bind(stream_key)
    .bind(enabled as i32)
    .bind(is_fast as i32)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn delete_endpoint_config(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query("DELETE FROM endpoint_configs WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Link or unlink an endpoint's YouTube OAuth grant.
pub async fn set_endpoint_youtube_oauth_id(
    pool: &SqlitePool,
    endpoint_id: i64,
    oauth_id: Option<i64>,
) -> Result<()> {
    sqlx::query(
        "UPDATE endpoint_configs SET youtube_oauth_id = ?1, updated_at = datetime('now')
         WHERE id = ?2",
    )
    .bind(oauth_id)
    .bind(endpoint_id)
    .execute(pool)
    .await?;
    Ok(())
}

// --- Event Endpoints (M2M) ---

pub async fn attach_endpoint_to_event(
    pool: &SqlitePool,
    event_id: i64,
    endpoint_id: i64,
) -> Result<()> {
    sqlx::query("INSERT OR IGNORE INTO event_endpoints (event_id, endpoint_id) VALUES (?1, ?2)")
        .bind(event_id)
        .bind(endpoint_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn detach_endpoint_from_event(
    pool: &SqlitePool,
    event_id: i64,
    endpoint_id: i64,
) -> Result<()> {
    sqlx::query("DELETE FROM event_endpoints WHERE event_id = ?1 AND endpoint_id = ?2")
        .bind(event_id)
        .bind(endpoint_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn get_event_endpoints(pool: &SqlitePool, event_id: i64) -> Result<Vec<EndpointConfig>> {
    let rows = sqlx::query(
        "SELECT e.id, e.alias, e.service_type, e.stream_key, e.enabled, e.position_last,
         e.delivered_bytes, e.is_fast, e.pusher, e.youtube_oauth_id, e.created_at, e.updated_at
         FROM endpoint_configs e
         INNER JOIN event_endpoints ee ON ee.endpoint_id = e.id
         WHERE ee.event_id = ?1 AND e.enabled = 1
         ORDER BY e.id",
    )
    .bind(event_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| EndpointConfig {
            id: r.get("id"),
            alias: r.get("alias"),
            service_type: r.get("service_type"),
            stream_key: r.get("stream_key"),
            enabled: r.get::<i32, _>("enabled") != 0,
            position_last: r.get("position_last"),
            delivered_bytes: r.get("delivered_bytes"),
            is_fast: r.get::<i32, _>("is_fast") != 0,
            pusher: parse_pusher_kind(r.get("pusher")),
            prefetch_chunks: None,
            youtube_oauth_id: r.get("youtube_oauth_id"),
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        })
        .collect())
}

// --- Delivery Instances ---

#[allow(clippy::too_many_arguments)]
pub async fn create_delivery_instance(
    pool: &SqlitePool,
    hetzner_id: i64,
    name: &str,
    ipv4: &str,
    server_type: &str,
    event_id: Option<i64>,
    auth_token: &str,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO delivery_instances (hetzner_id, name, ipv4, server_type, event_id, auth_token)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6) RETURNING id",
    )
    .bind(hetzner_id)
    .bind(name)
    .bind(ipv4)
    .bind(server_type)
    .bind(event_id)
    .bind(auth_token)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

pub async fn get_delivery_instance(pool: &SqlitePool, id: i64) -> Result<Option<DeliveryInstance>> {
    let row = sqlx::query(
        "SELECT id, hetzner_id, name, ipv4, status, server_type, event_id, created_at, last_health_at, auth_token
         FROM delivery_instances WHERE id = ?1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| DeliveryInstance {
        id: r.get("id"),
        hetzner_id: r.get("hetzner_id"),
        name: r.get("name"),
        ipv4: r.get("ipv4"),
        status: r.get("status"),
        server_type: r.get("server_type"),
        event_id: r.get("event_id"),
        created_at: r.get("created_at"),
        last_health_at: r.get("last_health_at"),
        auth_token: r.get("auth_token"),
    }))
}

pub async fn update_delivery_instance_status(
    pool: &SqlitePool,
    id: i64,
    status: &str,
) -> Result<()> {
    sqlx::query("UPDATE delivery_instances SET status = ?1 WHERE id = ?2")
        .bind(status)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn update_delivery_instance_health(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query("UPDATE delivery_instances SET last_health_at = datetime('now') WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_delivery_instance(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query("DELETE FROM delivery_instances WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn list_delivery_instances(pool: &SqlitePool) -> Result<Vec<DeliveryInstance>> {
    let rows = sqlx::query(
        "SELECT id, hetzner_id, name, ipv4, status, server_type, event_id, created_at, last_health_at, auth_token
         FROM delivery_instances WHERE status != 'deleted' ORDER BY id",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| DeliveryInstance {
            id: r.get("id"),
            hetzner_id: r.get("hetzner_id"),
            name: r.get("name"),
            ipv4: r.get("ipv4"),
            status: r.get("status"),
            server_type: r.get("server_type"),
            event_id: r.get("event_id"),
            created_at: r.get("created_at"),
            last_health_at: r.get("last_health_at"),
            auth_token: r.get("auth_token"),
        })
        .collect())
}

pub async fn get_delivery_instance_by_event(
    pool: &SqlitePool,
    event_id: i64,
) -> Result<Option<DeliveryInstance>> {
    let row = sqlx::query(
        "SELECT id, hetzner_id, name, ipv4, status, server_type, event_id, created_at, last_health_at, auth_token
         FROM delivery_instances WHERE event_id = ?1 AND status != 'deleted'
         ORDER BY id DESC LIMIT 1",
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| DeliveryInstance {
        id: r.get("id"),
        hetzner_id: r.get("hetzner_id"),
        name: r.get("name"),
        ipv4: r.get("ipv4"),
        status: r.get("status"),
        server_type: r.get("server_type"),
        event_id: r.get("event_id"),
        created_at: r.get("created_at"),
        last_health_at: r.get("last_health_at"),
        auth_token: r.get("auth_token"),
    }))
}

// --- Delivery Endpoint Status ---

#[allow(clippy::too_many_arguments)]
pub async fn upsert_delivery_endpoint_status(
    pool: &SqlitePool,
    instance_id: i64,
    alias: &str,
    alive: bool,
    chunks_processed: i64,
    current_chunk_id: i64,
    bytes_processed_total: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO delivery_endpoint_status (instance_id, alias, alive, chunks_processed, current_chunk_id, bytes_processed_total, last_check_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, datetime('now'))
         ON CONFLICT(instance_id, alias) DO UPDATE SET
             alive = ?3, chunks_processed = ?4, current_chunk_id = ?5, bytes_processed_total = ?6, last_check_at = datetime('now')",
    )
    .bind(instance_id)
    .bind(alias)
    .bind(alive as i32)
    .bind(chunks_processed)
    .bind(current_chunk_id)
    .bind(bytes_processed_total)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_delivery_endpoint_statuses(
    pool: &SqlitePool,
    instance_id: i64,
) -> Result<Vec<DeliveryEndpointStatus>> {
    let rows = sqlx::query(
        "SELECT id, instance_id, alias, alive, chunks_processed, current_chunk_id, bytes_processed_total, last_check_at
         FROM delivery_endpoint_status WHERE instance_id = ?1 ORDER BY alias",
    )
    .bind(instance_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| DeliveryEndpointStatus {
            id: r.get("id"),
            instance_id: r.get("instance_id"),
            alias: r.get("alias"),
            alive: r.get::<i32, _>("alive") != 0,
            chunks_processed: r.get("chunks_processed"),
            current_chunk_id: r.get("current_chunk_id"),
            bytes_processed_total: r.get("bytes_processed_total"),
            last_check_at: r.get("last_check_at"),
        })
        .collect())
}

// --- YouTube OAuth ---

pub async fn get_youtube_oauth(pool: &SqlitePool) -> Result<Option<YouTubeOAuth>> {
    let row = sqlx::query(
        "SELECT id, access_token, refresh_token, token_uri, client_id, client_secret, scopes, expires_at
         FROM youtube_oauth WHERE id = 1",
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| YouTubeOAuth {
        id: r.get("id"),
        access_token: r.get("access_token"),
        refresh_token: r.get("refresh_token"),
        token_uri: r.get("token_uri"),
        client_id: r.get("client_id"),
        client_secret: r.get("client_secret"),
        scopes: r.get("scopes"),
        expires_at: r.get("expires_at"),
    }))
}

#[allow(clippy::too_many_arguments)]
pub async fn upsert_youtube_oauth(
    pool: &SqlitePool,
    access_token: &str,
    refresh_token: &str,
    token_uri: &str,
    client_id: &str,
    client_secret: &str,
    scopes: &str,
    expires_at: Option<&str>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO youtube_oauth (id, access_token, refresh_token, token_uri, client_id, client_secret, scopes, expires_at)
         VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(id) DO UPDATE SET
             access_token = ?1, refresh_token = ?2, token_uri = ?3,
             client_id = ?4, client_secret = ?5, scopes = ?6, expires_at = ?7",
    )
    .bind(access_token)
    .bind(refresh_token)
    .bind(token_uri)
    .bind(client_id)
    .bind(client_secret)
    .bind(scopes)
    .bind(expires_at)
    .execute(pool)
    .await?;
    Ok(())
}

// --- Streaming Events (extended) ---

pub async fn list_streaming_events(pool: &SqlitePool) -> Result<Vec<StreamingEvent>> {
    let rows = sqlx::query(
        "SELECT id, name, received_bytes, receiving_activated, delivering_activated, cache_delay_secs, created_from, rescue_video_url
         FROM streaming_events ORDER BY id DESC",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| StreamingEvent {
            id: r.get("id"),
            name: r.get("name"),
            received_bytes: r.get("received_bytes"),
            receiving_activated: r.get::<i32, _>("receiving_activated") != 0,
            delivering_activated: r.get::<i32, _>("delivering_activated") != 0,
            cache_delay_secs: r.get("cache_delay_secs"),
            created_from: r.get("created_from"),
            rescue_video_url: r.get("rescue_video_url"),
        })
        .collect())
}

pub async fn create_streaming_event(pool: &SqlitePool, name: &str) -> Result<i64> {
    let row = sqlx::query("INSERT INTO streaming_events (name) VALUES (?1) RETURNING id")
        .bind(name)
        .fetch_one(pool)
        .await?;
    Ok(row.get("id"))
}

pub async fn update_streaming_event(
    pool: &SqlitePool,
    id: i64,
    name: &str,
    cache_delay_secs: Option<i64>,
    rescue_video_url: Option<String>,
) -> Result<()> {
    sqlx::query(
        "UPDATE streaming_events SET name = ?1, cache_delay_secs = ?2, rescue_video_url = ?3 WHERE id = ?4",
    )
    .bind(name)
    .bind(cache_delay_secs)
    .bind(&rescue_video_url)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

// --- Create Event from Template ---

/// Find the next unused name in the sequence `base_name`, `base_name-2`,
/// `base_name-3`, … up to `base_name-100`.
///
/// Single query: fetches every existing event name that starts with
/// `base_name` into a HashSet, then walks the candidate sequence locally.
/// This avoids the up-to-100-round-trips behaviour of the previous loop.
async fn find_unique_event_name(pool: &SqlitePool, base_name: &str) -> Result<String> {
    use std::collections::HashSet;

    // Escape SQLite LIKE wildcards so a literal % or _ in the template name
    // doesn't accidentally match unrelated rows. The escape character is
    // declared in the LIKE clause via `ESCAPE '\'`.
    let escaped = base_name
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    let pattern = format!("{escaped}%");

    let rows = sqlx::query(r#"SELECT name FROM streaming_events WHERE name LIKE ?1 ESCAPE '\'"#)
        .bind(&pattern)
        .fetch_all(pool)
        .await?;

    let existing: HashSet<String> = rows
        .into_iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();

    if !existing.contains(base_name) {
        return Ok(base_name.to_string());
    }

    for suffix in 2..=100i32 {
        let candidate = format!("{base_name}-{suffix}");
        if !existing.contains(&candidate) {
            return Ok(candidate);
        }
    }

    Err(CoreError::Other(format!(
        "Could not find a unique name for base '{base_name}' after 100 attempts"
    )))
}

/// Create a new streaming event from a template.
///
/// Generates a name `{template.name}-{YYYY-MM-DD}` (UTC). If that name already
/// exists, appends `-2`, `-3`, … up to `-100`. Copies the template's
/// `cache_delay_secs` and all its endpoints to the new event.
///
/// Returns `(event_id, event_name)`.
pub async fn create_event_from_template(
    pool: &SqlitePool,
    template_id: i64,
) -> Result<(i64, String)> {
    let template = super::templates::get_template_by_id_required(pool, template_id).await?;

    let today = Utc::now().format("%Y-%m-%d").to_string();
    let base_name = format!("{}-{}", template.name, today);
    let event_name = find_unique_event_name(pool, &base_name).await?;

    let row = sqlx::query(
        "INSERT INTO streaming_events (name, cache_delay_secs, created_from, rescue_video_url) VALUES (?1, ?2, ?3, ?4) RETURNING id",
    )
    .bind(&event_name)
    .bind(template.cache_delay_secs)
    .bind(&template.name)
    .bind(&template.rescue_video_url)
    .fetch_one(pool)
    .await?;
    let event_id: i64 = row.get("id");

    // Copy template endpoints to the new event
    let template_eps = super::templates::get_template_endpoints(pool, template_id).await?;
    for ep in template_eps {
        attach_endpoint_to_event(pool, event_id, ep.id).await?;
    }

    Ok((event_id, event_name))
}

/// Row from the delivery_restart_log table.
#[derive(Debug, serde::Serialize)]
pub struct DeliveryRestartRow {
    pub alias: String,
    pub timestamp_ms: i64,
    pub chunk_id: i64,
    pub lifetime_secs: i64,
    pub reason: String,
    pub stderr_tail: Option<String>,
    pub backoff_secs: i64,
}

// --- Delivery Log Capture ---

/// Insert a single ffmpeg restart record. Deduplicates on (instance_id, alias, timestamp_ms).
#[allow(clippy::too_many_arguments)]
pub async fn insert_delivery_restart_record(
    pool: &SqlitePool,
    instance_id: i64,
    event_id: Option<i64>,
    alias: &str,
    timestamp_ms: i64,
    chunk_id: i64,
    lifetime_secs: i64,
    reason: &str,
    stderr_tail: Option<&str>,
    backoff_secs: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO delivery_restart_log
             (instance_id, event_id, alias, timestamp_ms, chunk_id, lifetime_secs, reason, stderr_tail, backoff_secs)
         SELECT ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9
         WHERE NOT EXISTS (
             SELECT 1 FROM delivery_restart_log
             WHERE instance_id = ?1 AND alias = ?3 AND timestamp_ms = ?4
         )",
    )
    .bind(instance_id)
    .bind(event_id)
    .bind(alias)
    .bind(timestamp_ms)
    .bind(chunk_id)
    .bind(lifetime_secs)
    .bind(reason)
    .bind(stderr_tail)
    .bind(backoff_secs)
    .execute(pool)
    .await?;
    Ok(())
}

/// Get all restart records for a delivery instance, ordered by timestamp.
pub async fn get_delivery_restart_log(
    pool: &SqlitePool,
    instance_id: i64,
) -> Result<Vec<DeliveryRestartRow>> {
    let rows = sqlx::query(
        "SELECT alias, timestamp_ms, chunk_id, lifetime_secs, reason, stderr_tail, backoff_secs
         FROM delivery_restart_log
         WHERE instance_id = ?1
         ORDER BY timestamp_ms ASC",
    )
    .bind(instance_id)
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| DeliveryRestartRow {
            alias: r.get("alias"),
            timestamp_ms: r.get("timestamp_ms"),
            chunk_id: r.get("chunk_id"),
            lifetime_secs: r.get("lifetime_secs"),
            reason: r.get("reason"),
            stderr_tail: r.get("stderr_tail"),
            backoff_secs: r.get("backoff_secs"),
        })
        .collect())
}

/// Store captured VPS log text for a delivery instance.
pub async fn insert_delivery_log(
    pool: &SqlitePool,
    instance_id: i64,
    event_id: Option<i64>,
    log_text: &str,
) -> Result<()> {
    sqlx::query("INSERT INTO delivery_logs (instance_id, event_id, log_text) VALUES (?1, ?2, ?3)")
        .bind(instance_id)
        .bind(event_id)
        .bind(log_text)
        .execute(pool)
        .await?;
    Ok(())
}

/// Get the captured log text for a delivery instance (most recent capture).
pub async fn get_delivery_log(pool: &SqlitePool, instance_id: i64) -> Result<Option<String>> {
    let row = sqlx::query(
        "SELECT log_text FROM delivery_logs WHERE instance_id = ?1 ORDER BY id DESC LIMIT 1",
    )
    .bind(instance_id)
    .fetch_optional(pool)
    .await?;
    Ok(row.map(|r| r.get("log_text")))
}
