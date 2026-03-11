use super::*;

async fn setup_db() -> sqlx::sqlite::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn client_profile_crud() {
    let pool = setup_db().await;
    assert!(get_client_profile(&pool).await.unwrap().is_none());

    upsert_client_profile(&pool, "test-uuid").await.unwrap();
    let profile = get_client_profile(&pool).await.unwrap().unwrap();
    assert_eq!(profile.user_uuid, "test-uuid");

    upsert_client_profile(&pool, "updated-uuid").await.unwrap();
    let profile = get_client_profile(&pool).await.unwrap().unwrap();
    assert_eq!(profile.user_uuid, "updated-uuid");
}

#[tokio::test]
async fn streaming_event_crud() {
    let pool = setup_db().await;
    assert!(get_streaming_event(&pool).await.unwrap().is_none());

    let id = upsert_streaming_event(&pool, "evt-1").await.unwrap();
    assert!(id > 0);

    let event = get_streaming_event(&pool).await.unwrap().unwrap();
    assert_eq!(event.name, "evt-1");
    assert!(event.receiving_activated);
    assert!(event.delivering_activated);

    update_streaming_event_flags(&pool, id, false, false)
        .await
        .unwrap();
    let event = get_streaming_event(&pool).await.unwrap().unwrap();
    assert!(!event.receiving_activated);
    assert!(!event.delivering_activated);

    update_received_bytes(&pool, id, 1024).await.unwrap();
    let event = get_streaming_event(&pool).await.unwrap().unwrap();
    assert_eq!(event.received_bytes, 1024);

    delete_streaming_event(&pool, id).await.unwrap();
    assert!(get_streaming_event(&pool).await.unwrap().is_none());
}

#[tokio::test]
async fn chunk_record_crud() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-1").await.unwrap();

    let chunk_id = insert_chunk(&pool, event_id, "/tmp/chunk1.bin", 512, "abc123")
        .await
        .unwrap();
    assert!(chunk_id > 0);

    let unsent = get_unsent_chunks(&pool, 10).await.unwrap();
    assert_eq!(unsent.len(), 1);
    assert_eq!(unsent[0].md5, "abc123");
    assert!(!unsent[0].in_process);
    assert!(!unsent[0].sent);

    set_chunk_in_process(&pool, chunk_id, true).await.unwrap();
    let unsent = get_unsent_chunks(&pool, 10).await.unwrap();
    assert_eq!(unsent.len(), 0);

    set_chunk_sent(&pool, chunk_id).await.unwrap();
    let stats = get_chunk_stats(&pool, 1000).await.unwrap();
    assert_eq!(stats.total_chunks, 1);
    assert_eq!(stats.sent_chunks, 1);
    assert_eq!(stats.pending_chunks, 0);
}

#[tokio::test]
async fn chunk_stats_and_pagination() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-1").await.unwrap();

    for i in 0..5 {
        insert_chunk(
            &pool,
            event_id,
            &format!("/tmp/chunk{i}.bin"),
            100 * (i + 1),
            &format!("md5_{i}"),
        )
        .await
        .unwrap();
    }

    let stats = get_chunk_stats(&pool, 1000).await.unwrap();
    assert_eq!(stats.total_chunks, 5);
    assert_eq!(stats.pending_chunks, 5);
    assert_eq!(stats.total_bytes, 100 + 200 + 300 + 400 + 500);

    let page = get_chunks_paginated(&pool, 0, 3).await.unwrap();
    assert_eq!(page.len(), 3);

    let page2 = get_chunks_paginated(&pool, 3, 3).await.unwrap();
    assert_eq!(page2.len(), 2);
}

#[tokio::test]
async fn get_first_chunk_id_for_event_works() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-1").await.unwrap();
    let event_id2 = upsert_streaming_event(&pool, "evt-2").await.unwrap();

    // No chunks yet — returns None
    let first = get_first_chunk_id_for_event(&pool, event_id).await.unwrap();
    assert_eq!(first, None);

    // Insert chunks for event 1
    let c1 = insert_chunk(&pool, event_id, "/tmp/c1.bin", 100, "md5a")
        .await
        .unwrap();
    let _c2 = insert_chunk(&pool, event_id, "/tmp/c2.bin", 100, "md5b")
        .await
        .unwrap();

    // Insert chunk for event 2 (should not affect event 1)
    let _c3 = insert_chunk(&pool, event_id2, "/tmp/c3.bin", 100, "md5c")
        .await
        .unwrap();

    let first = get_first_chunk_id_for_event(&pool, event_id).await.unwrap();
    assert_eq!(first, Some(c1));

    // Event 2 should return its own first chunk
    let first2 = get_first_chunk_id_for_event(&pool, event_id2)
        .await
        .unwrap();
    assert!(first2.is_some());
    assert_ne!(first2.unwrap(), c1);
}

#[tokio::test]
async fn delete_all_chunks_works() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-1").await.unwrap();

    for i in 0..3 {
        insert_chunk(&pool, event_id, &format!("/tmp/c{i}.bin"), 100, "md5")
            .await
            .unwrap();
    }

    let deleted = delete_all_chunks(&pool).await.unwrap();
    assert_eq!(deleted, 3);

    let stats = get_chunk_stats(&pool, 1000).await.unwrap();
    assert_eq!(stats.total_chunks, 0);
}

#[tokio::test]
async fn cascade_delete() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-1").await.unwrap();
    insert_chunk(&pool, event_id, "/tmp/c.bin", 100, "md5")
        .await
        .unwrap();

    delete_streaming_event(&pool, event_id).await.unwrap();
    let stats = get_chunk_stats(&pool, 1000).await.unwrap();
    assert_eq!(stats.total_chunks, 0);
}

#[tokio::test]
async fn delete_other_streaming_events_keeps_only_target() {
    let pool = setup_db().await;

    let id1 = upsert_streaming_event(&pool, "evt-1").await.unwrap();
    let id2 = upsert_streaming_event(&pool, "evt-2").await.unwrap();
    let id3 = upsert_streaming_event(&pool, "evt-3").await.unwrap();
    assert_ne!(id1, id2);
    assert_ne!(id2, id3);

    let deleted = delete_other_streaming_events(&pool, id2).await.unwrap();
    assert_eq!(deleted, 2);

    let remaining = get_streaming_event(&pool).await.unwrap().unwrap();
    assert_eq!(remaining.id, id2);
    assert_eq!(remaining.name, "evt-2");

    assert!(
        get_streaming_event_by_id(&pool, id1)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        get_streaming_event_by_id(&pool, id3)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn delete_other_streaming_events_noop_when_only_one() {
    let pool = setup_db().await;

    let id = upsert_streaming_event(&pool, "evt-1").await.unwrap();

    let deleted = delete_other_streaming_events(&pool, id).await.unwrap();
    assert_eq!(deleted, 0);

    let event = get_streaming_event(&pool).await.unwrap().unwrap();
    assert_eq!(event.id, id);
}

#[tokio::test]
async fn delete_other_streaming_events_cascades_chunks() {
    let pool = setup_db().await;

    let id1 = upsert_streaming_event(&pool, "stale").await.unwrap();
    let id2 = upsert_streaming_event(&pool, "active").await.unwrap();

    insert_chunk(&pool, id1, "/tmp/stale.bin", 100, "md5_stale")
        .await
        .unwrap();
    insert_chunk(&pool, id2, "/tmp/active.bin", 200, "md5_active")
        .await
        .unwrap();

    let deleted = delete_other_streaming_events(&pool, id2).await.unwrap();
    assert_eq!(deleted, 1);

    let stats = get_chunk_stats(&pool, 1000).await.unwrap();
    assert_eq!(stats.total_chunks, 1);
    assert_eq!(stats.total_bytes, 200);
}

#[tokio::test]
async fn migration_is_idempotent() {
    let pool = setup_db().await;
    run_migrations(&pool).await.unwrap();
    upsert_client_profile(&pool, "test").await.unwrap();
    let profile = get_client_profile(&pool).await.unwrap().unwrap();
    assert_eq!(profile.user_uuid, "test");
}

// --- V2 table tests ---

#[tokio::test]
async fn endpoint_config_crud() {
    let pool = setup_db().await;
    let list = list_endpoint_configs(&pool).await.unwrap();
    assert!(list.is_empty());

    let id = create_endpoint_config(&pool, "YouTube", "YT_HLS", "yt-key-123", false)
        .await
        .unwrap();
    assert!(id > 0);

    let ep = get_endpoint_config(&pool, id).await.unwrap().unwrap();
    assert_eq!(ep.alias, "YouTube");
    assert_eq!(ep.service_type, "YT_HLS");
    assert!(ep.enabled);
    assert!(!ep.is_fast);

    update_endpoint_config(&pool, id, "YouTube HLS", "YT_HLS", "new-key", true, true)
        .await
        .unwrap();
    let ep = get_endpoint_config(&pool, id).await.unwrap().unwrap();
    assert_eq!(ep.alias, "YouTube HLS");
    assert!(ep.is_fast);

    delete_endpoint_config(&pool, id).await.unwrap();
    assert!(get_endpoint_config(&pool, id).await.unwrap().is_none());
}

#[tokio::test]
async fn event_endpoint_attachment() {
    let pool = setup_db().await;

    let event_id = upsert_streaming_event(&pool, "evt-1").await.unwrap();
    let ep1 = create_endpoint_config(&pool, "YT", "YT_HLS", "key1", false)
        .await
        .unwrap();
    let ep2 = create_endpoint_config(&pool, "FB", "FB", "key2", false)
        .await
        .unwrap();

    attach_endpoint_to_event(&pool, event_id, ep1)
        .await
        .unwrap();
    attach_endpoint_to_event(&pool, event_id, ep2)
        .await
        .unwrap();

    let eps = get_event_endpoints(&pool, event_id).await.unwrap();
    assert_eq!(eps.len(), 2);

    detach_endpoint_from_event(&pool, event_id, ep1)
        .await
        .unwrap();
    let eps = get_event_endpoints(&pool, event_id).await.unwrap();
    assert_eq!(eps.len(), 1);
    assert_eq!(eps[0].alias, "FB");
}

#[tokio::test]
async fn event_endpoint_cascade_on_event_delete() {
    let pool = setup_db().await;

    let event_id = upsert_streaming_event(&pool, "evt-1").await.unwrap();
    let ep_id = create_endpoint_config(&pool, "YT", "YT_HLS", "key1", false)
        .await
        .unwrap();
    attach_endpoint_to_event(&pool, event_id, ep_id)
        .await
        .unwrap();

    delete_streaming_event(&pool, event_id).await.unwrap();
    assert!(get_endpoint_config(&pool, ep_id).await.unwrap().is_some());
}

#[tokio::test]
async fn delivery_instance_crud() {
    let pool = setup_db().await;

    let id = create_delivery_instance(&pool, 12345, "rs-delivery-1", "1.2.3.4", "cx23", None)
        .await
        .unwrap();
    assert!(id > 0);

    let inst = get_delivery_instance(&pool, id).await.unwrap().unwrap();
    assert_eq!(inst.hetzner_id, 12345);
    assert_eq!(inst.name, "rs-delivery-1");
    assert_eq!(inst.status, "creating");

    update_delivery_instance_status(&pool, id, "running")
        .await
        .unwrap();
    let inst = get_delivery_instance(&pool, id).await.unwrap().unwrap();
    assert_eq!(inst.status, "running");

    update_delivery_instance_health(&pool, id).await.unwrap();
    let inst = get_delivery_instance(&pool, id).await.unwrap().unwrap();
    assert!(inst.last_health_at.is_some());

    let list = list_delivery_instances(&pool).await.unwrap();
    assert_eq!(list.len(), 1);

    delete_delivery_instance(&pool, id).await.unwrap();
    assert!(get_delivery_instance(&pool, id).await.unwrap().is_none());
}

#[tokio::test]
async fn youtube_oauth_crud() {
    let pool = setup_db().await;

    assert!(get_youtube_oauth(&pool).await.unwrap().is_none());

    upsert_youtube_oauth(
        &pool,
        "access-tok",
        "refresh-tok",
        "https://oauth2.googleapis.com/token",
        "client-id",
        "client-val",
        "youtube.readonly",
        Some("2099-01-01T00:00:00Z"),
    )
    .await
    .unwrap();

    let oauth = get_youtube_oauth(&pool).await.unwrap().unwrap();
    assert_eq!(oauth.access_token, "access-tok");
    assert_eq!(oauth.refresh_token, "refresh-tok");
    assert_eq!(oauth.scopes, "youtube.readonly");

    upsert_youtube_oauth(
        &pool,
        "new-access",
        "refresh-tok",
        "https://oauth2.googleapis.com/token",
        "client-id",
        "client-val",
        "youtube.readonly",
        None,
    )
    .await
    .unwrap();

    let oauth = get_youtube_oauth(&pool).await.unwrap().unwrap();
    assert_eq!(oauth.access_token, "new-access");
    assert!(oauth.expires_at.is_none());
}

#[tokio::test]
async fn list_streaming_events_and_create() {
    let pool = setup_db().await;
    assert!(list_streaming_events(&pool).await.unwrap().is_empty());

    let id = create_streaming_event(&pool, "Test Event").await.unwrap();
    assert!(id > 0);

    let events = list_streaming_events(&pool).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].name, "Test Event");
    assert!(!events[0].receiving_activated);
}

#[tokio::test]
async fn endpoint_unique_alias_constraint() {
    let pool = setup_db().await;
    create_endpoint_config(&pool, "YouTube", "YT_HLS", "key1", false)
        .await
        .unwrap();
    let result = create_endpoint_config(&pool, "YouTube", "FB", "key2", false).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn delivery_instance_by_event() {
    let pool = setup_db().await;

    let event_id = upsert_streaming_event(&pool, "evt-1").await.unwrap();

    // No instance yet
    assert!(
        get_delivery_instance_by_event(&pool, event_id)
            .await
            .unwrap()
            .is_none()
    );

    let id = create_delivery_instance(&pool, 99999, "rs-del-1", "5.6.7.8", "cx23", Some(event_id))
        .await
        .unwrap();

    let inst = get_delivery_instance_by_event(&pool, event_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inst.id, id);
    assert_eq!(inst.hetzner_id, 99999);
    assert_eq!(inst.event_id, Some(event_id));

    // Deleted instances should not be returned
    update_delivery_instance_status(&pool, id, "deleted")
        .await
        .unwrap();
    assert!(
        get_delivery_instance_by_event(&pool, event_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn delivery_endpoint_status_crud() {
    let pool = setup_db().await;

    let inst_id = create_delivery_instance(&pool, 11111, "rs-del-1", "1.2.3.4", "cx23", None)
        .await
        .unwrap();

    // Initially empty
    let statuses = get_delivery_endpoint_statuses(&pool, inst_id)
        .await
        .unwrap();
    assert!(statuses.is_empty());

    // Insert status for two endpoints
    upsert_delivery_endpoint_status(&pool, inst_id, "YouTube", true, 4096, 42)
        .await
        .unwrap();
    upsert_delivery_endpoint_status(&pool, inst_id, "Facebook", false, 0, 10)
        .await
        .unwrap();

    let statuses = get_delivery_endpoint_statuses(&pool, inst_id)
        .await
        .unwrap();
    assert_eq!(statuses.len(), 2);
    // Ordered by alias
    assert_eq!(statuses[0].alias, "Facebook");
    assert!(!statuses[0].alive);
    assert_eq!(statuses[1].alias, "YouTube");
    assert!(statuses[1].alive);
    assert_eq!(statuses[1].buff_size_bytes, 4096);
    assert_eq!(statuses[1].current_chunk_id, 42);

    // Upsert updates existing
    upsert_delivery_endpoint_status(&pool, inst_id, "YouTube", true, 8192, 99)
        .await
        .unwrap();
    let statuses = get_delivery_endpoint_statuses(&pool, inst_id)
        .await
        .unwrap();
    assert_eq!(statuses.len(), 2);
    let yt = statuses.iter().find(|s| s.alias == "YouTube").unwrap();
    assert_eq!(yt.buff_size_bytes, 8192);
    assert_eq!(yt.current_chunk_id, 99);
}

#[tokio::test]
async fn delivery_endpoint_status_cascade_on_instance_delete() {
    let pool = setup_db().await;

    let inst_id = create_delivery_instance(&pool, 22222, "rs-del-2", "2.3.4.5", "cx23", None)
        .await
        .unwrap();
    upsert_delivery_endpoint_status(&pool, inst_id, "YT", true, 100, 5)
        .await
        .unwrap();

    delete_delivery_instance(&pool, inst_id).await.unwrap();
    let statuses = get_delivery_endpoint_statuses(&pool, inst_id)
        .await
        .unwrap();
    assert!(statuses.is_empty());
}
