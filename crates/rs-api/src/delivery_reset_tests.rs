//! Tests for the `received_bytes` reset inside `DeliveryOrchestrator::start_delivery`.
//!
//! The reset clears the cumulative byte counter at the start of every delivery
//! cycle so the dashboard reflects current-cycle bytes, not cross-session totals
//! (which reached 57GB on a 3-minute event during operator soak 2026-05-10).

use sqlx::{Row, SqlitePool};

async fn setup_pool() -> SqlitePool {
    let pool = rs_core::db::create_memory_pool().await.unwrap();
    rs_core::db::run_migrations(&pool).await.unwrap();
    pool
}

async fn insert_event_with_bytes(pool: &SqlitePool, name: &str, bytes: i64) -> i64 {
    let row = sqlx::query(
        "INSERT INTO streaming_events (name, received_bytes) VALUES (?1, ?2) RETURNING id",
    )
    .bind(name)
    .bind(bytes)
    .fetch_one(pool)
    .await
    .unwrap();
    row.get::<i64, _>("id")
}

async fn read_received_bytes(pool: &SqlitePool, event_id: i64) -> i64 {
    let row = sqlx::query("SELECT received_bytes FROM streaming_events WHERE id = ?1")
        .bind(event_id)
        .fetch_one(pool)
        .await
        .unwrap();
    row.get::<i64, _>("received_bytes")
}

#[tokio::test]
async fn reset_received_bytes_zeroes_existing_counter() {
    let pool = setup_pool().await;
    let event_id = insert_event_with_bytes(&pool, "ev-reset-populated", 57_000_000_000).await;
    crate::delivery::reset_event_received_bytes(&pool, event_id)
        .await
        .expect("reset should succeed on populated event");
    let after = read_received_bytes(&pool, event_id).await;
    assert_eq!(after, 0, "received_bytes must be 0 after reset");
}

#[tokio::test]
async fn reset_received_bytes_is_idempotent() {
    let pool = setup_pool().await;
    let event_id = insert_event_with_bytes(&pool, "ev-reset-idempotent", 0).await;
    crate::delivery::reset_event_received_bytes(&pool, event_id)
        .await
        .expect("reset on zero counter is a no-op success");
    let after = read_received_bytes(&pool, event_id).await;
    assert_eq!(after, 0);
}

#[tokio::test]
async fn reset_received_bytes_succeeds_on_unknown_event_id() {
    // UPDATE with 0 rows affected is success — Start Delivering should not abort
    // just because the row was already deleted by a concurrent stop.
    let pool = setup_pool().await;
    crate::delivery::reset_event_received_bytes(&pool, 99_999)
        .await
        .expect("UPDATE matching 0 rows is success");
}
