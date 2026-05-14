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

    let chunk_id = insert_chunk(&pool, event_id, "/tmp/chunk1.bin", 512, "abc123", 0)
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
            0,
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
    let c1 = insert_chunk(&pool, event_id, "/tmp/c1.bin", 100, "md5a", 0)
        .await
        .unwrap();
    let _c2 = insert_chunk(&pool, event_id, "/tmp/c2.bin", 100, "md5b", 0)
        .await
        .unwrap();

    // Insert chunk for event 2 (should not affect event 1)
    let _c3 = insert_chunk(&pool, event_id2, "/tmp/c3.bin", 100, "md5c", 0)
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
async fn delete_chunks_for_event_works() {
    let pool = setup_db().await;
    let evt1 = upsert_streaming_event(&pool, "evt-1").await.unwrap();
    let evt2 = upsert_streaming_event(&pool, "evt-2").await.unwrap();

    // Insert chunks for both events
    for i in 0..3 {
        insert_chunk(&pool, evt1, &format!("/tmp/e1c{i}.bin"), 100, "md5", 0)
            .await
            .unwrap();
    }
    for i in 0..2 {
        insert_chunk(&pool, evt2, &format!("/tmp/e2c{i}.bin"), 200, "md5", 0)
            .await
            .unwrap();
    }

    // Delete only evt1 chunks
    let deleted = delete_chunks_for_event(&pool, evt1).await.unwrap();
    assert_eq!(deleted, 3);

    // evt1 should have 0 chunks
    let count1 = get_sent_chunk_count_for_event(&pool, evt1).await.unwrap();
    let chunks1 = get_chunks_for_event(&pool, evt1).await.unwrap();
    assert_eq!(count1, 0);
    assert!(chunks1.is_empty());

    // evt2 should still have its chunks
    let chunks2 = get_chunks_for_event(&pool, evt2).await.unwrap();
    assert_eq!(chunks2.len(), 2);

    // Deleting again should return 0
    let deleted_again = delete_chunks_for_event(&pool, evt1).await.unwrap();
    assert_eq!(deleted_again, 0);
}

#[tokio::test]
async fn delete_all_chunks_works() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-1").await.unwrap();

    for i in 0..3 {
        insert_chunk(&pool, event_id, &format!("/tmp/c{i}.bin"), 100, "md5", 0)
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
    insert_chunk(&pool, event_id, "/tmp/c.bin", 100, "md5", 0)
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

    insert_chunk(&pool, id1, "/tmp/stale.bin", 100, "md5_stale", 0)
        .await
        .unwrap();
    insert_chunk(&pool, id2, "/tmp/active.bin", 200, "md5_active", 0)
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

    let id = create_endpoint_config(&pool, "YouTube", "YT_RTMP", "yt-key-123", false)
        .await
        .unwrap();
    assert!(id > 0);

    let ep = get_endpoint_config(&pool, id).await.unwrap().unwrap();
    assert_eq!(ep.alias, "YouTube");
    assert_eq!(ep.service_type, "YT_RTMP");
    assert!(ep.enabled);
    assert!(!ep.is_fast);

    update_endpoint_config(&pool, id, "YouTube HLS", "YT_RTMP", "new-key", true, true)
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
    let ep1 = create_endpoint_config(&pool, "YT", "YT_RTMP", "key1", false)
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
async fn get_event_endpoints_filters_disabled() {
    let pool = setup_db().await;

    let event_id = upsert_streaming_event(&pool, "evt-1").await.unwrap();
    let ep1 = create_endpoint_config(&pool, "YouTube", "YT_RTMP", "key1", false)
        .await
        .unwrap();
    let ep2 = create_endpoint_config(&pool, "Facebook", "FB", "key2", false)
        .await
        .unwrap();

    attach_endpoint_to_event(&pool, event_id, ep1)
        .await
        .unwrap();
    attach_endpoint_to_event(&pool, event_id, ep2)
        .await
        .unwrap();

    // Both enabled — should return 2
    let eps = get_event_endpoints(&pool, event_id).await.unwrap();
    assert_eq!(eps.len(), 2);

    // Disable ep2 — should only return ep1
    update_endpoint_config(&pool, ep2, "Facebook", "FB", "key2", false, false)
        .await
        .unwrap();
    let eps = get_event_endpoints(&pool, event_id).await.unwrap();
    assert_eq!(eps.len(), 1);
    assert_eq!(eps[0].alias, "YouTube");
}

#[tokio::test]
async fn event_endpoint_cascade_on_event_delete() {
    let pool = setup_db().await;

    let event_id = upsert_streaming_event(&pool, "evt-1").await.unwrap();
    let ep_id = create_endpoint_config(&pool, "YT", "YT_RTMP", "key1", false)
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

    let id = create_delivery_instance(
        &pool,
        12345,
        "rs-delivery-1",
        "1.2.3.4",
        "cx23",
        None,
        "test-token-123",
    )
    .await
    .unwrap();
    assert!(id > 0);

    let inst = get_delivery_instance(&pool, id).await.unwrap().unwrap();
    assert_eq!(inst.hetzner_id, 12345);
    assert_eq!(inst.name, "rs-delivery-1");
    assert_eq!(inst.status, "creating");
    assert_eq!(inst.auth_token, "test-token-123");

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
    create_endpoint_config(&pool, "YouTube", "YT_RTMP", "key1", false)
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

    let id = create_delivery_instance(
        &pool,
        99999,
        "rs-del-1",
        "5.6.7.8",
        "cx23",
        Some(event_id),
        "token-evt",
    )
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

    let inst_id = create_delivery_instance(&pool, 11111, "rs-del-1", "1.2.3.4", "cx23", None, "")
        .await
        .unwrap();

    // Initially empty
    let statuses = get_delivery_endpoint_statuses(&pool, inst_id)
        .await
        .unwrap();
    assert!(statuses.is_empty());

    // Insert status for two endpoints
    upsert_delivery_endpoint_status(&pool, inst_id, "YouTube", true, 100, 42, 1048576)
        .await
        .unwrap();
    upsert_delivery_endpoint_status(&pool, inst_id, "Facebook", false, 0, 10, 0)
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
    assert_eq!(statuses[1].chunks_processed, 100);
    assert_eq!(statuses[1].current_chunk_id, 42);

    // Upsert updates existing
    upsert_delivery_endpoint_status(&pool, inst_id, "YouTube", true, 200, 99, 2097152)
        .await
        .unwrap();
    let statuses = get_delivery_endpoint_statuses(&pool, inst_id)
        .await
        .unwrap();
    assert_eq!(statuses.len(), 2);
    let yt = statuses.iter().find(|s| s.alias == "YouTube").unwrap();
    assert_eq!(yt.chunks_processed, 200);
    assert_eq!(yt.current_chunk_id, 99);
}

#[tokio::test]
async fn delivery_endpoint_status_cascade_on_instance_delete() {
    let pool = setup_db().await;

    let inst_id = create_delivery_instance(&pool, 22222, "rs-del-2", "2.3.4.5", "cx23", None, "")
        .await
        .unwrap();
    upsert_delivery_endpoint_status(&pool, inst_id, "YT", true, 10, 5, 500)
        .await
        .unwrap();

    delete_delivery_instance(&pool, inst_id).await.unwrap();
    let statuses = get_delivery_endpoint_statuses(&pool, inst_id)
        .await
        .unwrap();
    assert!(statuses.is_empty());
}

#[tokio::test]
async fn migration_v6_sent_at_column_exists() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-v6").await.unwrap();
    let chunk_id = insert_chunk(&pool, event_id, "/tmp/v6.bin", 100, "md5v6", 0)
        .await
        .unwrap();

    // Before marking sent, sent_at should be NULL
    let row = sqlx::query("SELECT sent_at FROM chunk_records WHERE id = ?1")
        .bind(chunk_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    let sent_at: Option<String> = row.get("sent_at");
    assert!(sent_at.is_none());

    // Mark as sent — should populate sent_at
    set_chunk_sent(&pool, chunk_id).await.unwrap();
    let row = sqlx::query("SELECT sent_at FROM chunk_records WHERE id = ?1")
        .bind(chunk_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    let sent_at: Option<String> = row.get("sent_at");
    assert!(sent_at.is_some());
}

#[tokio::test]
async fn migration_v6_bytes_processed_total_column_exists() {
    let pool = setup_db().await;
    let inst_id = create_delivery_instance(&pool, 33333, "rs-del-v6", "3.3.3.3", "cx23", None, "")
        .await
        .unwrap();

    upsert_delivery_endpoint_status(&pool, inst_id, "YT", true, 50, 10, 999999)
        .await
        .unwrap();

    let statuses = get_delivery_endpoint_statuses(&pool, inst_id)
        .await
        .unwrap();
    assert_eq!(statuses.len(), 1);
    assert_eq!(statuses[0].bytes_processed_total, 999999);
    assert_eq!(statuses[0].chunks_processed, 50);
}

#[tokio::test]
async fn interleaved_events_have_independent_sequence_numbers() {
    let pool = setup_db().await;
    let evt1 = upsert_streaming_event(&pool, "evt-1").await.unwrap();
    let evt2 = upsert_streaming_event(&pool, "evt-2").await.unwrap();

    // Interleave chunks: evt1, evt2, evt1, evt2, evt1
    let _c1 = insert_chunk(&pool, evt1, "/tmp/c1.bin", 100, "md5a", 0)
        .await
        .unwrap();
    let _c2 = insert_chunk(&pool, evt2, "/tmp/c2.bin", 100, "md5b", 0)
        .await
        .unwrap();
    let _c3 = insert_chunk(&pool, evt1, "/tmp/c3.bin", 100, "md5c", 0)
        .await
        .unwrap();
    let _c4 = insert_chunk(&pool, evt2, "/tmp/c4.bin", 100, "md5d", 0)
        .await
        .unwrap();
    let _c5 = insert_chunk(&pool, evt1, "/tmp/c5.bin", 100, "md5e", 0)
        .await
        .unwrap();

    // evt1 chunks should have sequence 1, 2, 3 (regardless of global ID gaps)
    let chunks_evt1 = get_chunks_for_event(&pool, evt1).await.unwrap();
    assert_eq!(chunks_evt1.len(), 3);
    assert_eq!(chunks_evt1[0].sequence_number, 1);
    assert_eq!(chunks_evt1[1].sequence_number, 2);
    assert_eq!(chunks_evt1[2].sequence_number, 3);

    // evt2 chunks should have sequence 1, 2
    let chunks_evt2 = get_chunks_for_event(&pool, evt2).await.unwrap();
    assert_eq!(chunks_evt2.len(), 2);
    assert_eq!(chunks_evt2[0].sequence_number, 1);
    assert_eq!(chunks_evt2[1].sequence_number, 2);

    // Verify first/last sequence number queries
    let first1 = get_first_sequence_number_for_event(&pool, evt1)
        .await
        .unwrap();
    assert_eq!(first1, Some(1));
    let last1 = get_latest_sequence_number_for_event(&pool, evt1)
        .await
        .unwrap();
    assert_eq!(last1, Some(3));

    let first2 = get_first_sequence_number_for_event(&pool, evt2)
        .await
        .unwrap();
    assert_eq!(first2, Some(1));
    let last2 = get_latest_sequence_number_for_event(&pool, evt2)
        .await
        .unwrap();
    assert_eq!(last2, Some(2));
}

#[tokio::test]
async fn sequence_number_starts_at_one_for_new_event() {
    let pool = setup_db().await;
    let evt = upsert_streaming_event(&pool, "evt-seq").await.unwrap();

    let _c1 = insert_chunk(&pool, evt, "/tmp/s1.bin", 50, "md5x", 0)
        .await
        .unwrap();
    let chunks = get_chunks_for_event(&pool, evt).await.unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].sequence_number, 1);
}

#[tokio::test]
async fn sequence_numbers_are_contiguous_with_many_events() {
    let pool = setup_db().await;
    let evt1 = upsert_streaming_event(&pool, "busy-1").await.unwrap();
    let evt2 = upsert_streaming_event(&pool, "busy-2").await.unwrap();
    let evt3 = upsert_streaming_event(&pool, "busy-3").await.unwrap();

    // Insert 10 chunks each, interleaved across 3 events
    for i in 0..10 {
        insert_chunk(
            &pool,
            evt1,
            &format!("/tmp/e1c{i}.bin"),
            100,
            &format!("e1m{i}"),
            0,
        )
        .await
        .unwrap();
        insert_chunk(
            &pool,
            evt2,
            &format!("/tmp/e2c{i}.bin"),
            100,
            &format!("e2m{i}"),
            0,
        )
        .await
        .unwrap();
        insert_chunk(
            &pool,
            evt3,
            &format!("/tmp/e3c{i}.bin"),
            100,
            &format!("e3m{i}"),
            0,
        )
        .await
        .unwrap();
    }

    // Each event should have sequence numbers 1..10 with no gaps
    for (evt_id, evt_name) in [(evt1, "busy-1"), (evt2, "busy-2"), (evt3, "busy-3")] {
        let chunks = get_chunks_for_event(&pool, evt_id).await.unwrap();
        assert_eq!(chunks.len(), 10, "Event {evt_name} should have 10 chunks");
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(
                chunk.sequence_number,
                (i + 1) as i64,
                "Event {evt_name} chunk {i} should have sequence {}",
                i + 1
            );
        }
    }
}

#[tokio::test]
async fn get_sent_chunk_count_for_event_works() {
    let pool = setup_db().await;
    let evt1 = upsert_streaming_event(&pool, "evt-1").await.unwrap();
    let evt2 = upsert_streaming_event(&pool, "evt-2").await.unwrap();

    // No chunks — count is 0
    let count = get_sent_chunk_count_for_event(&pool, evt1).await.unwrap();
    assert_eq!(count, 0);

    // Insert 3 chunks for evt1, mark 2 as sent
    let c1 = insert_chunk(&pool, evt1, "/tmp/s1.bin", 100, "md5a", 0)
        .await
        .unwrap();
    let c2 = insert_chunk(&pool, evt1, "/tmp/s2.bin", 100, "md5b", 0)
        .await
        .unwrap();
    let _c3 = insert_chunk(&pool, evt1, "/tmp/s3.bin", 100, "md5c", 0)
        .await
        .unwrap();

    set_chunk_sent(&pool, c1).await.unwrap();
    set_chunk_sent(&pool, c2).await.unwrap();

    let count = get_sent_chunk_count_for_event(&pool, evt1).await.unwrap();
    assert_eq!(count, 2);

    // evt2 should have 0 sent chunks
    let _c4 = insert_chunk(&pool, evt2, "/tmp/s4.bin", 100, "md5d", 0)
        .await
        .unwrap();
    let count2 = get_sent_chunk_count_for_event(&pool, evt2).await.unwrap();
    assert_eq!(count2, 0);
}

#[tokio::test]
async fn get_latest_chunk_id_for_event_works() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-latest").await.unwrap();

    // No chunks — returns None
    let latest = get_latest_chunk_id_for_event(&pool, event_id)
        .await
        .unwrap();
    assert_eq!(latest, None);

    // Insert chunks
    let _c1 = insert_chunk(&pool, event_id, "/tmp/l1.bin", 100, "md5a", 0)
        .await
        .unwrap();
    let c2 = insert_chunk(&pool, event_id, "/tmp/l2.bin", 100, "md5b", 0)
        .await
        .unwrap();

    let latest = get_latest_chunk_id_for_event(&pool, event_id)
        .await
        .unwrap();
    assert_eq!(latest, Some(c2));
}

#[tokio::test]
async fn cache_duration_sums_undelivered_sent_chunks() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "cache-dur-test")
        .await
        .unwrap();

    // No chunks → 0 duration. (Function returns the raw uncapped sum; the
    // earlier `target_secs` cap parameter was removed because per-event
    // cache_delay overrides need the raw measurement.)
    let dur = get_cache_duration_secs(&pool, event_id, 0).await.unwrap();
    assert!((dur - 0.0).abs() < 0.001);

    // Insert 3 chunks with known durations
    let c1 = insert_chunk(&pool, event_id, "/tmp/c1.ts", 1000, "md5a", 500)
        .await
        .unwrap();
    let c2 = insert_chunk(&pool, event_id, "/tmp/c2.ts", 1000, "md5b", 1000)
        .await
        .unwrap();
    let c3 = insert_chunk(&pool, event_id, "/tmp/c3.ts", 1000, "md5c", 1500)
        .await
        .unwrap();

    // Unsent chunks → 0 duration
    let dur = get_cache_duration_secs(&pool, event_id, 0).await.unwrap();
    assert!((dur - 0.0).abs() < 0.001);

    // Mark all as sent
    set_chunk_sent(&pool, c1).await.unwrap();
    set_chunk_sent(&pool, c2).await.unwrap();
    set_chunk_sent(&pool, c3).await.unwrap();

    // All sent, none delivered → sum of all durations (500+1000+1500 = 3000ms = 3.0s)
    let dur = get_cache_duration_secs(&pool, event_id, 0).await.unwrap();
    assert!((dur - 3.0).abs() < 0.001);

    // VPS delivered up to sequence 1 → only chunks 2 and 3 count (1000+1500 = 2500ms = 2.5s)
    let dur = get_cache_duration_secs(&pool, event_id, 1).await.unwrap();
    assert!((dur - 2.5).abs() < 0.001);

    // VPS delivered up to sequence 2 → only chunk 3 counts (1500ms = 1.5s)
    let dur = get_cache_duration_secs(&pool, event_id, 2).await.unwrap();
    assert!((dur - 1.5).abs() < 0.001);

    // VPS delivered all → 0 duration
    let dur = get_cache_duration_secs(&pool, event_id, 3).await.unwrap();
    assert!((dur - 0.0).abs() < 0.001);
}

#[tokio::test]
async fn sent_duration_ms_only_counts_uploaded_chunks() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "sent-dur-test")
        .await
        .unwrap();

    // Insert 3 chunks: two will be sent to S3, one local only
    let c1 = insert_chunk(&pool, event_id, "/tmp/c1.bin", 1000, "aaa", 2000)
        .await
        .unwrap();
    let c2 = insert_chunk(&pool, event_id, "/tmp/c2.bin", 1000, "bbb", 1800)
        .await
        .unwrap();
    let _c3 = insert_chunk(&pool, event_id, "/tmp/c3.bin", 1000, "ccc", 2200)
        .await
        .unwrap();

    // No chunks sent yet → 0
    let total = get_sent_duration_ms(&pool, event_id).await.unwrap();
    assert_eq!(total, 0);

    // Mark chunks 1 and 2 as sent (uploaded to S3)
    set_chunk_sent(&pool, c1).await.unwrap();
    set_chunk_sent(&pool, c2).await.unwrap();
    // Chunk 3 stays local (sent = 0)

    let total = get_sent_duration_ms(&pool, event_id).await.unwrap();
    assert_eq!(total, 3800); // 2000 + 1800, NOT 6000
}

#[tokio::test]
async fn migration_v14_rescue_video_url_column_exists() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    sqlx::query("INSERT INTO streaming_events (name, received_bytes, receiving_activated, delivering_activated, rescue_video_url) VALUES ('test', 0, 0, 0, 'https://example.com/rescue.mp4')")
        .execute(&pool)
        .await
        .unwrap();
    let row = sqlx::query("SELECT rescue_video_url FROM streaming_events WHERE name = 'test'")
        .fetch_one(&pool)
        .await
        .unwrap();
    let url: Option<String> = row.get("rescue_video_url");
    assert_eq!(url, Some("https://example.com/rescue.mp4".to_string()));
}

#[tokio::test]
async fn update_event_rescue_video_url() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    create_streaming_event(&pool, "rescue-test").await.unwrap();
    let events = list_streaming_events(&pool).await.unwrap();
    let id = events[0].id;
    let evt = get_streaming_event_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(evt.rescue_video_url, None);
    update_streaming_event(
        &pool,
        id,
        "rescue-test",
        None,
        Some("https://example.com/rescue.mp4".to_string()),
    )
    .await
    .unwrap();
    let evt = get_streaming_event_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(
        evt.rescue_video_url,
        Some("https://example.com/rescue.mp4".to_string())
    );
}

// Template and create-event-from-template tests are in template_tests.rs
// Delivery log capture tests are in delivery_log_tests.rs
// Upload telemetry tests are in upload_tests.rs
// Migration idempotency tests are in migration_tests.rs
// Pool PRAGMA tests are in pool_tests.rs
