use sqlx::Row;
use sqlx::sqlite::SqlitePool;

use crate::error::{CoreError, Result};
use crate::models::{EndpointConfig, EventTemplate};

pub async fn create_template(
    pool: &SqlitePool,
    name: &str,
    cache_delay_secs: Option<i64>,
    rescue_video_url: Option<String>,
) -> Result<i64> {
    let row = sqlx::query(
        "INSERT INTO event_templates (name, cache_delay_secs, rescue_video_url) VALUES (?1, ?2, ?3) RETURNING id",
    )
    .bind(name)
    .bind(cache_delay_secs)
    .bind(&rescue_video_url)
    .fetch_one(pool)
    .await?;
    Ok(row.get("id"))
}

pub async fn list_templates(pool: &SqlitePool) -> Result<Vec<EventTemplate>> {
    let rows = sqlx::query(
        "SELECT id, name, cache_delay_secs, rescue_video_url FROM event_templates ORDER BY id",
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| EventTemplate {
            id: r.get("id"),
            name: r.get("name"),
            cache_delay_secs: r.get("cache_delay_secs"),
            rescue_video_url: r.get("rescue_video_url"),
        })
        .collect())
}

pub async fn get_template_by_id(pool: &SqlitePool, id: i64) -> Result<Option<EventTemplate>> {
    let row = sqlx::query(
        "SELECT id, name, cache_delay_secs, rescue_video_url FROM event_templates WHERE id = ?1",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| EventTemplate {
        id: r.get("id"),
        name: r.get("name"),
        cache_delay_secs: r.get("cache_delay_secs"),
        rescue_video_url: r.get("rescue_video_url"),
    }))
}

pub async fn update_template(
    pool: &SqlitePool,
    id: i64,
    name: &str,
    cache_delay_secs: Option<i64>,
    rescue_video_url: Option<String>,
) -> Result<()> {
    sqlx::query(
        "UPDATE event_templates SET name = ?1, cache_delay_secs = ?2, rescue_video_url = ?3 WHERE id = ?4",
    )
    .bind(name)
    .bind(cache_delay_secs)
    .bind(&rescue_video_url)
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
/// - Existing events are preserved as-is (not deleted) — this avoids losing
///   endpoint assignments and keeps all current event configurations
///   functional. Templates and events coexist; templates serve as presets for
///   new instances, while existing events continue to work.
///
/// All writes happen inside a single transaction so a partial failure rolls
/// back cleanly; the idempotency check on the next startup will then retry
/// the entire seed instead of leaving the DB in a half-seeded state.
///
/// Returns the number of templates created.
pub async fn seed_templates_from_events(pool: &SqlitePool) -> Result<usize> {
    // Idempotency check (outside the transaction — read-only)
    let template_count: i64 = sqlx::query("SELECT COUNT(*) as c FROM event_templates")
        .fetch_one(pool)
        .await?
        .get("c");
    if template_count > 0 {
        return Ok(0);
    }

    // Fetch all events (read-only — outside the transaction is fine)
    let events = super::list_streaming_events(pool).await?;
    if events.is_empty() {
        return Ok(0);
    }

    // Collect endpoint assignments up front so the transaction only writes.
    let mut event_plans: Vec<(String, Option<i64>, Option<String>, Vec<i64>)> =
        Vec::with_capacity(events.len());
    for event in &events {
        let endpoints = super::get_event_endpoints(pool, event.id).await?;
        let endpoint_ids: Vec<i64> = endpoints.iter().map(|e| e.id).collect();
        event_plans.push((
            event.name.clone(),
            event.cache_delay_secs,
            event.rescue_video_url.clone(),
            endpoint_ids,
        ));
    }

    // Wrap all writes in one transaction so a failure mid-seed rolls back.
    let mut tx = pool.begin().await?;
    let mut created = 0usize;
    for (name, cache_delay, rescue_video_url, endpoint_ids) in &event_plans {
        let row = sqlx::query(
            "INSERT INTO event_templates (name, cache_delay_secs, rescue_video_url) VALUES (?1, ?2, ?3) RETURNING id",
        )
        .bind(name)
        .bind(cache_delay)
        .bind(rescue_video_url)
        .fetch_one(&mut *tx)
        .await?;
        let template_id: i64 = row.get("id");

        for endpoint_id in endpoint_ids {
            sqlx::query(
                "INSERT OR IGNORE INTO template_endpoints (template_id, endpoint_id) VALUES (?1, ?2)",
            )
            .bind(template_id)
            .bind(endpoint_id)
            .execute(&mut *tx)
            .await?;
        }
        created += 1;
    }
    tx.commit().await?;

    tracing::info!("Seeded {created} templates from existing streaming events");
    Ok(created)
}
