use sqlx::Row;
use sqlx::sqlite::SqlitePool;

use crate::error::{CoreError, Result};
use crate::models::{EndpointConfig, EventTemplate};

pub async fn create_template(
    pool: &SqlitePool,
    name: &str,
    cache_delay_secs: Option<i64>,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO event_templates (name, cache_delay_secs) VALUES (?1, ?2) RETURNING id",
    )
    .bind(name)
    .bind(cache_delay_secs)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

pub async fn list_templates(pool: &SqlitePool) -> Result<Vec<EventTemplate>> {
    let rows = sqlx::query("SELECT id, name, cache_delay_secs FROM event_templates ORDER BY id")
        .fetch_all(pool)
        .await?;

    Ok(rows
        .into_iter()
        .map(|r| EventTemplate {
            id: r.get("id"),
            name: r.get("name"),
            cache_delay_secs: r.get("cache_delay_secs"),
        })
        .collect())
}

pub async fn get_template_by_id(pool: &SqlitePool, id: i64) -> Result<Option<EventTemplate>> {
    let row = sqlx::query("SELECT id, name, cache_delay_secs FROM event_templates WHERE id = ?1")
        .bind(id)
        .fetch_optional(pool)
        .await?;

    Ok(row.map(|r| EventTemplate {
        id: r.get("id"),
        name: r.get("name"),
        cache_delay_secs: r.get("cache_delay_secs"),
    }))
}

pub async fn update_template(
    pool: &SqlitePool,
    id: i64,
    name: &str,
    cache_delay_secs: Option<i64>,
) -> Result<()> {
    sqlx::query("UPDATE event_templates SET name = ?1, cache_delay_secs = ?2 WHERE id = ?3")
        .bind(name)
        .bind(cache_delay_secs)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn delete_template(pool: &SqlitePool, id: i64) -> Result<()> {
    sqlx::query("DELETE FROM event_templates WHERE id = ?1")
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn attach_endpoint_to_template(
    pool: &SqlitePool,
    template_id: i64,
    endpoint_id: i64,
) -> Result<()> {
    sqlx::query(
        "INSERT OR IGNORE INTO template_endpoints (template_id, endpoint_id) VALUES (?1, ?2)",
    )
    .bind(template_id)
    .bind(endpoint_id)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn detach_endpoint_from_template(
    pool: &SqlitePool,
    template_id: i64,
    endpoint_id: i64,
) -> Result<()> {
    sqlx::query("DELETE FROM template_endpoints WHERE template_id = ?1 AND endpoint_id = ?2")
        .bind(template_id)
        .bind(endpoint_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub async fn get_template_endpoints(
    pool: &SqlitePool,
    template_id: i64,
) -> Result<Vec<EndpointConfig>> {
    let rows = sqlx::query(
        "SELECT e.id, e.alias, e.service_type, e.stream_key, e.enabled, e.position_last,
         e.delivered_bytes, e.is_fast, e.created_at, e.updated_at
         FROM endpoint_configs e
         INNER JOIN template_endpoints te ON te.endpoint_id = e.id
         WHERE te.template_id = ?1 AND e.enabled = 1
         ORDER BY e.id",
    )
    .bind(template_id)
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

pub async fn get_template_by_id_required(pool: &SqlitePool, id: i64) -> Result<EventTemplate> {
    get_template_by_id(pool, id)
        .await?
        .ok_or_else(|| CoreError::Other(format!("template {id} not found")))
}

/// Seed templates from existing streaming events. One-shot startup helper.
///
/// Idempotency: runs only when `event_templates` is empty. If a user has any
/// templates already, this function is a no-op (returns 0).
///
/// Behavior when seeding:
/// - For each event in `streaming_events`, create a matching template (same
///   name, same cache_delay_secs).
/// - Copy the event's endpoint assignments to `template_endpoints`.
/// - Delete events that are not currently streaming
///   (`receiving_activated = 0 AND delivering_activated = 0`). Streaming
///   events are preserved so we don't disrupt active live sessions.
///
/// Returns the number of templates created.
pub async fn seed_templates_from_events(pool: &SqlitePool) -> Result<usize> {
    // Idempotency check
    let template_count: i64 = sqlx::query("SELECT COUNT(*) as c FROM event_templates")
        .fetch_one(pool)
        .await?
        .get("c");
    if template_count > 0 {
        return Ok(0);
    }

    // Fetch all events
    let events = super::list_streaming_events(pool).await?;
    if events.is_empty() {
        return Ok(0);
    }

    let mut created = 0usize;
    for event in &events {
        // Create template with same name + cache_delay
        let template_id = create_template(pool, &event.name, event.cache_delay_secs).await?;

        // Copy endpoint assignments
        let endpoints = super::get_event_endpoints(pool, event.id).await?;
        for ep in &endpoints {
            attach_endpoint_to_template(pool, template_id, ep.id).await?;
        }
        created += 1;
    }

    // Delete non-streaming events. Cascade removes chunk_records and event_endpoints.
    sqlx::query(
        "DELETE FROM streaming_events WHERE receiving_activated = 0 AND delivering_activated = 0",
    )
    .execute(pool)
    .await?;

    tracing::info!("Seeded {created} templates from existing streaming events");
    Ok(created)
}
