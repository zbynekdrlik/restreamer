use super::*;
use chrono::Utc;

async fn setup_db() -> sqlx::sqlite::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn migration_v12_creates_template_tables() {
    let pool = setup_db().await;

    // event_templates table exists and is writable
    let id: i64 = sqlx::query("INSERT INTO event_templates (name) VALUES ('test') RETURNING id")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("id");
    assert!(id > 0);

    // template_endpoints table exists (FK to event_templates)
    let ep_id: i64 = sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key) VALUES ('yt', 'YT_HLS', 'k') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap()
    .get("id");

    sqlx::query("INSERT INTO template_endpoints (template_id, endpoint_id) VALUES (?1, ?2)")
        .bind(id)
        .bind(ep_id)
        .execute(&pool)
        .await
        .unwrap();

    // created_from column exists on streaming_events
    let evt_id = create_streaming_event(&pool, "test-evt").await.unwrap();
    sqlx::query("UPDATE streaming_events SET created_from = 'test-template' WHERE id = ?1")
        .bind(evt_id)
        .execute(&pool)
        .await
        .unwrap();

    let val: Option<String> =
        sqlx::query("SELECT created_from FROM streaming_events WHERE id = ?1")
            .bind(evt_id)
            .fetch_one(&pool)
            .await
            .unwrap()
            .get("created_from");
    assert_eq!(val.as_deref(), Some("test-template"));
}

// --- Template DB tests (Task 3) ---

#[tokio::test]
async fn template_crud() {
    let pool = setup_db().await;
    let list = list_templates(&pool).await.unwrap();
    assert!(list.is_empty());

    let id = create_template(&pool, "Sunday Service", Some(30))
        .await
        .unwrap();
    assert!(id > 0);

    let list = list_templates(&pool).await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].name, "Sunday Service");
    assert_eq!(list[0].cache_delay_secs, Some(30));

    let tmpl = get_template_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(tmpl.id, id);
    assert_eq!(tmpl.name, "Sunday Service");
    assert_eq!(tmpl.cache_delay_secs, Some(30));

    update_template(&pool, id, "Sunday Service Updated", Some(60))
        .await
        .unwrap();
    let tmpl = get_template_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(tmpl.name, "Sunday Service Updated");
    assert_eq!(tmpl.cache_delay_secs, Some(60));

    update_template(&pool, id, "No Delay", None).await.unwrap();
    let tmpl = get_template_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(tmpl.cache_delay_secs, None);

    delete_template(&pool, id).await.unwrap();
    assert!(get_template_by_id(&pool, id).await.unwrap().is_none());
    assert!(list_templates(&pool).await.unwrap().is_empty());
}

#[tokio::test]
async fn template_duplicate_name_fails() {
    let pool = setup_db().await;

    create_template(&pool, "Weekend Service", None)
        .await
        .unwrap();
    let result = create_template(&pool, "Weekend Service", Some(15)).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn template_endpoint_linking() {
    let pool = setup_db().await;

    let tmpl_id = create_template(&pool, "Multi-endpoint Template", Some(45))
        .await
        .unwrap();
    let ep1 = create_endpoint_config(&pool, "YT-tmpl", "YT_HLS", "key-yt", false)
        .await
        .unwrap();
    let ep2 = create_endpoint_config(&pool, "FB-tmpl", "FB", "key-fb", false)
        .await
        .unwrap();

    // Initially no endpoints
    let eps = get_template_endpoints(&pool, tmpl_id).await.unwrap();
    assert!(eps.is_empty());

    // Attach both endpoints
    attach_endpoint_to_template(&pool, tmpl_id, ep1)
        .await
        .unwrap();
    attach_endpoint_to_template(&pool, tmpl_id, ep2)
        .await
        .unwrap();

    let eps = get_template_endpoints(&pool, tmpl_id).await.unwrap();
    assert_eq!(eps.len(), 2);

    // Duplicate attach is idempotent (INSERT OR IGNORE)
    attach_endpoint_to_template(&pool, tmpl_id, ep1)
        .await
        .unwrap();
    let eps = get_template_endpoints(&pool, tmpl_id).await.unwrap();
    assert_eq!(eps.len(), 2);

    // Detach one endpoint
    detach_endpoint_from_template(&pool, tmpl_id, ep1)
        .await
        .unwrap();
    let eps = get_template_endpoints(&pool, tmpl_id).await.unwrap();
    assert_eq!(eps.len(), 1);
    assert_eq!(eps[0].id, ep2);
}

#[tokio::test]
async fn template_cascade_deletes_endpoints() {
    let pool = setup_db().await;

    let tmpl_id = create_template(&pool, "Cascade Test", None).await.unwrap();
    let ep_id = create_endpoint_config(&pool, "YT-cascade", "YT_HLS", "key", false)
        .await
        .unwrap();
    attach_endpoint_to_template(&pool, tmpl_id, ep_id)
        .await
        .unwrap();

    // Verify link exists
    let eps = get_template_endpoints(&pool, tmpl_id).await.unwrap();
    assert_eq!(eps.len(), 1);

    // Delete the template — should cascade to template_endpoints
    delete_template(&pool, tmpl_id).await.unwrap();

    // endpoint_config itself should still exist (only the join row is deleted)
    let ep = get_endpoint_config(&pool, ep_id).await.unwrap();
    assert!(ep.is_some());

    // The template is gone
    assert!(get_template_by_id(&pool, tmpl_id).await.unwrap().is_none());
}

// --- Create event from template tests (Task 4) ---

#[tokio::test]
async fn create_event_from_template_basic() {
    let pool = setup_db().await;

    // Create a template with an endpoint
    let tmpl_id = create_template(&pool, "Morning Service", Some(45))
        .await
        .unwrap();
    let ep_id = create_endpoint_config(&pool, "YT-from-tmpl", "YT_HLS", "stream-key", false)
        .await
        .unwrap();
    attach_endpoint_to_template(&pool, tmpl_id, ep_id)
        .await
        .unwrap();

    // Create event from template
    let (event_id, event_name) = create_event_from_template(&pool, tmpl_id).await.unwrap();
    assert!(event_id > 0);

    // Name must contain today's date in YYYY-MM-DD format
    let today = Utc::now().format("%Y-%m-%d").to_string();
    assert!(
        event_name.contains(&today),
        "Expected name to contain {today}, got {event_name}"
    );
    assert!(
        event_name.starts_with("Morning Service-"),
        "Expected name to start with 'Morning Service-', got {event_name}"
    );

    // Verify event properties
    let event = get_streaming_event_by_id(&pool, event_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.name, event_name);
    assert_eq!(event.cache_delay_secs, Some(45));
    assert_eq!(event.created_from.as_deref(), Some("Morning Service"));

    // Verify endpoints were copied
    let eps = get_event_endpoints(&pool, event_id).await.unwrap();
    assert_eq!(eps.len(), 1);
    assert_eq!(eps[0].id, ep_id);
}

#[tokio::test]
async fn create_event_from_template_duplicate_date() {
    let pool = setup_db().await;

    let tmpl_id = create_template(&pool, "Evening Service", None)
        .await
        .unwrap();

    // Create 3 events on the same day
    let (_, name1) = create_event_from_template(&pool, tmpl_id).await.unwrap();
    let (_, name2) = create_event_from_template(&pool, tmpl_id).await.unwrap();
    let (_, name3) = create_event_from_template(&pool, tmpl_id).await.unwrap();

    let today = Utc::now().format("%Y-%m-%d").to_string();

    // First event: Evening Service-YYYY-MM-DD
    assert_eq!(name1, format!("Evening Service-{today}"));

    // Second event: Evening Service-YYYY-MM-DD-2
    assert_eq!(name2, format!("Evening Service-{today}-2"));

    // Third event: Evening Service-YYYY-MM-DD-3
    assert_eq!(name3, format!("Evening Service-{today}-3"));
}

// --- seed_templates_from_events tests (Task 2) ---

#[tokio::test]
async fn seed_templates_converts_events() {
    let pool = setup_db().await;

    // Wipe templates so the seed has work to do
    sqlx::query("DELETE FROM template_endpoints")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM event_templates")
        .execute(&pool)
        .await
        .unwrap();

    // Insert two non-streaming events
    let evt1: i64 = sqlx::query(
        "INSERT INTO streaming_events (name, cache_delay_secs, receiving_activated, delivering_activated)
         VALUES ('sunday-service', 120, 0, 0) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap()
    .get("id");

    let _evt2: i64 = sqlx::query(
        "INSERT INTO streaming_events (name, cache_delay_secs, receiving_activated, delivering_activated)
         VALUES ('wednesday-study', NULL, 0, 0) RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap()
    .get("id");

    // Create endpoint and assign to evt1
    let ep_id: i64 = sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key) VALUES ('yt', 'YT_HLS', 'k') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap()
    .get("id");

    attach_endpoint_to_event(&pool, evt1, ep_id).await.unwrap();

    // Run the seed function
    let created = seed_templates_from_events(&pool).await.unwrap();
    assert_eq!(created, 2);

    // Verify: 2 templates created with correct fields
    let templates = list_templates(&pool).await.unwrap();
    assert_eq!(templates.len(), 2);

    let sunday = templates
        .iter()
        .find(|t| t.name == "sunday-service")
        .unwrap();
    assert_eq!(sunday.cache_delay_secs, Some(120));

    let wed = templates
        .iter()
        .find(|t| t.name == "wednesday-study")
        .unwrap();
    assert_eq!(wed.cache_delay_secs, None);

    // Verify: sunday-service has its endpoint
    let sunday_eps = get_template_endpoints(&pool, sunday.id).await.unwrap();
    assert_eq!(sunday_eps.len(), 1);
    assert_eq!(sunday_eps[0].alias, "yt");

    // Verify: events still exist (seed does not delete events)
    let remaining: i64 = sqlx::query("SELECT COUNT(*) as c FROM streaming_events")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("c");
    assert_eq!(remaining, 2);

    // Verify: event_endpoints assignments still exist (cascade not triggered)
    let evt1_eps = get_event_endpoints(&pool, evt1).await.unwrap();
    assert_eq!(evt1_eps.len(), 1);
    assert_eq!(evt1_eps[0].alias, "yt");
}

#[tokio::test]
async fn seed_templates_idempotent() {
    let pool = setup_db().await;

    // Pre-create a template — this makes the templates table non-empty
    create_template(&pool, "existing-template", Some(60))
        .await
        .unwrap();

    // Insert an event with a different name
    sqlx::query(
        "INSERT INTO streaming_events (name, cache_delay_secs, receiving_activated, delivering_activated)
         VALUES ('orphan-event', 120, 0, 0)",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Run seed — should be no-op because templates table is non-empty
    let created = seed_templates_from_events(&pool).await.unwrap();
    assert_eq!(created, 0);

    // Verify: still only the original template
    let templates = list_templates(&pool).await.unwrap();
    assert_eq!(templates.len(), 1);
    assert_eq!(templates[0].name, "existing-template");

    // Verify: event was NOT deleted (seed didn't run)
    let remaining: i64 = sqlx::query("SELECT COUNT(*) as c FROM streaming_events")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get("c");
    assert_eq!(remaining, 1);
}

#[tokio::test]
async fn seed_templates_preserves_streaming() {
    let pool = setup_db().await;

    // Wipe templates
    sqlx::query("DELETE FROM template_endpoints")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM event_templates")
        .execute(&pool)
        .await
        .unwrap();

    // Insert one streaming event and one idle event
    sqlx::query(
        "INSERT INTO streaming_events (name, cache_delay_secs, receiving_activated, delivering_activated)
         VALUES ('live-stream', 60, 1, 1)",
    )
    .execute(&pool)
    .await
    .unwrap();

    sqlx::query(
        "INSERT INTO streaming_events (name, cache_delay_secs, receiving_activated, delivering_activated)
         VALUES ('idle-stream', 60, 0, 0)",
    )
    .execute(&pool)
    .await
    .unwrap();

    // Run seed
    let created = seed_templates_from_events(&pool).await.unwrap();
    assert_eq!(created, 2);

    // Verify: BOTH templates created (both event names)
    let templates = list_templates(&pool).await.unwrap();
    assert_eq!(templates.len(), 2);

    // Verify: BOTH events still exist (seed does not delete events)
    let remaining: Vec<String> = sqlx::query("SELECT name FROM streaming_events ORDER BY name")
        .fetch_all(&pool)
        .await
        .unwrap()
        .iter()
        .map(|r| r.get::<String, _>("name"))
        .collect();
    assert_eq!(remaining.len(), 2);
    assert!(remaining.contains(&"live-stream".to_string()));
    assert!(remaining.contains(&"idle-stream".to_string()));
}

#[tokio::test]
async fn seed_templates_no_events() {
    let pool = setup_db().await;

    // Wipe templates
    sqlx::query("DELETE FROM template_endpoints")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("DELETE FROM event_templates")
        .execute(&pool)
        .await
        .unwrap();

    // No events to seed from
    let created = seed_templates_from_events(&pool).await.unwrap();
    assert_eq!(created, 0);

    // Verify: still no templates
    let templates = list_templates(&pool).await.unwrap();
    assert_eq!(templates.len(), 0);
}
