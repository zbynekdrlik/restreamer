//! Regression tests for delivery_instances.status CHECK constraint.
//!
//! The orchestrator writes phase strings to track VPS startup state for
//! the operator dashboard. The DB CHECK constraint must allow every
//! string the orchestrator writes — otherwise the UPDATE fails and the
//! orchestrator falls back to "failed", breaking the dashboard.
//!
//! Extracted from db/tests.rs to keep that file under the 1000-line cap.
use super::*;

async fn setup_db() -> sqlx::sqlite::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn delivery_instance_status_accepts_all_phases() {
    let pool = setup_db().await;
    create_streaming_event(&pool, "s-evt").await.unwrap();
    let evt_id = list_streaming_events(&pool).await.unwrap()[0].id;
    let inst_id =
        create_delivery_instance(&pool, 12345, "tn", "1.2.3.4", "cpx22", Some(evt_id), "tok")
            .await
            .unwrap();

    for status in [
        "creating",
        "running",
        "stopping",
        "deleted",
        "failed",
        "booting",
        "initializing",
        "delivering",
    ] {
        update_delivery_instance_status(&pool, inst_id, status)
            .await
            .unwrap_or_else(|e| panic!("status '{status}' should be allowed: {e}"));
        let inst = get_delivery_instance(&pool, inst_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(inst.status, status, "round-trip failed for '{status}'");
    }
}
