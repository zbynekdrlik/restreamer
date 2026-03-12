use sqlx::Row;
use sqlx::sqlite::SqlitePool;

use crate::error::Result;
use crate::models::{
    DeliveryEndpointStatus, DeliveryInstance, EndpointConfig, StreamingEvent, YouTubeOAuth,
};

// --- Endpoint Configs ---

pub async fn list_endpoint_configs(pool: &SqlitePool) -> Result<Vec<EndpointConfig>> {
    let rows = sqlx::query(
        "SELECT id, alias, service_type, stream_key, enabled, position_last,
         delivered_bytes, is_fast, created_at, updated_at
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
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        })
        .collect())
}

pub async fn get_endpoint_config(pool: &SqlitePool, id: i64) -> Result<Option<EndpointConfig>> {
    let row = sqlx::query(
        "SELECT id, alias, service_type, stream_key, enabled, position_last,
         delivered_bytes, is_fast, created_at, updated_at
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
         e.delivered_bytes, e.is_fast, e.created_at, e.updated_at
         FROM endpoint_configs e
         INNER JOIN event_endpoints ee ON ee.endpoint_id = e.id
         WHERE ee.event_id = ?1
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
            created_at: r.get("created_at"),
            updated_at: r.get("updated_at"),
        })
        .collect())
}

// --- Delivery Instances ---

pub async fn create_delivery_instance(
    pool: &SqlitePool,
    hetzner_id: i64,
    name: &str,
    ipv4: &str,
    server_type: &str,
    event_id: Option<i64>,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO delivery_instances (hetzner_id, name, ipv4, server_type, event_id)
         VALUES (?1, ?2, ?3, ?4, ?5) RETURNING id",
    )
    .bind(hetzner_id)
    .bind(name)
    .bind(ipv4)
    .bind(server_type)
    .bind(event_id)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

pub async fn get_delivery_instance(pool: &SqlitePool, id: i64) -> Result<Option<DeliveryInstance>> {
    let row = sqlx::query(
        "SELECT id, hetzner_id, name, ipv4, status, server_type, event_id, created_at, last_health_at
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
        "SELECT id, hetzner_id, name, ipv4, status, server_type, event_id, created_at, last_health_at
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
        })
        .collect())
}

pub async fn get_delivery_instance_by_event(
    pool: &SqlitePool,
    event_id: i64,
) -> Result<Option<DeliveryInstance>> {
    let row = sqlx::query(
        "SELECT id, hetzner_id, name, ipv4, status, server_type, event_id, created_at, last_health_at
         FROM delivery_instances WHERE event_id = ?1 AND status != 'deleted' LIMIT 1",
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
    }))
}

// --- Delivery Endpoint Status ---

pub async fn upsert_delivery_endpoint_status(
    pool: &SqlitePool,
    instance_id: i64,
    alias: &str,
    alive: bool,
    buff_size_bytes: i64,
    current_chunk_id: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO delivery_endpoint_status (instance_id, alias, alive, buff_size_bytes, current_chunk_id, last_check_at)
         VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'))
         ON CONFLICT(instance_id, alias) DO UPDATE SET
             alive = ?3, buff_size_bytes = ?4, current_chunk_id = ?5, last_check_at = datetime('now')",
    )
    .bind(instance_id)
    .bind(alias)
    .bind(alive as i32)
    .bind(buff_size_bytes)
    .bind(current_chunk_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn get_delivery_endpoint_statuses(
    pool: &SqlitePool,
    instance_id: i64,
) -> Result<Vec<DeliveryEndpointStatus>> {
    let rows = sqlx::query(
        "SELECT id, instance_id, alias, alive, buff_size_bytes, current_chunk_id, last_check_at
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
            buff_size_bytes: r.get("buff_size_bytes"),
            current_chunk_id: r.get("current_chunk_id"),
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
        "SELECT id, name, received_bytes, receiving_activated, delivering_activated
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
