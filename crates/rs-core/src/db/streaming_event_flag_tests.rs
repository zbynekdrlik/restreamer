//! Tests for the single-flag setters on `streaming_events`.
//!
//! Kept separate from `tests.rs` because that file is near the 1000-line
//! cap enforced by the `File size check` CI job.

use super::*;

async fn setup_db() -> sqlx::sqlite::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn set_delivering_activated_preserves_receiving_flag() {
    // Regression for #130 fix path: delivery_handlers::delivery_start
    // calls set_delivering_activated(true) to unblock the dashboard
    // while leaving receiving_activated alone. A buggy shared-setter
    // that also touched receiving would silently drop live ingest from
    // under the operator. This test pins the single-flag semantics.
    let pool = setup_db().await;
    let id = upsert_streaming_event(&pool, "evt-delivering-flag")
        .await
        .unwrap();

    // Baseline: set receiving=true, delivering=false explicitly so we
    // can observe that set_delivering_activated flips only delivering.
    update_streaming_event_flags(&pool, id, true, false)
        .await
        .unwrap();
    let before = get_streaming_event(&pool).await.unwrap().unwrap();
    assert!(
        before.receiving_activated,
        "setup: receiving should be true"
    );
    assert!(
        !before.delivering_activated,
        "setup: delivering should be false"
    );

    set_delivering_activated(&pool, id, true).await.unwrap();
    let after = get_streaming_event(&pool).await.unwrap().unwrap();
    assert!(after.delivering_activated, "delivering must be flipped");
    assert!(
        after.receiving_activated,
        "receiving must be untouched by set_delivering_activated"
    );

    // Mirror path: /delivery/stop clears the flag without touching receiving.
    set_delivering_activated(&pool, id, false).await.unwrap();
    let after_stop = get_streaming_event(&pool).await.unwrap().unwrap();
    assert!(!after_stop.delivering_activated);
    assert!(
        after_stop.receiving_activated,
        "receiving must remain true after delivery stop"
    );
}
