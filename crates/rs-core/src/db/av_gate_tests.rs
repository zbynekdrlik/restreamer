//! `av_gate_sessions` persistence (#357).

use super::*;
use crate::db::{create_memory_pool, run_migrations};

async fn pool() -> SqlitePool {
    let p = create_memory_pool().await.unwrap();
    run_migrations(&p).await.unwrap();
    p
}

fn full_row(id: &str, created_at: &str, state: &str, units: i64) -> AvGateSessionRow {
    AvGateSessionRow {
        id: id.to_string(),
        requester: "camera-box".to_string(),
        title: "A/V gate".to_string(),
        state: state.to_string(),
        broadcast_id: Some("bc-1".to_string()),
        stream_id: Some("st-1".to_string()),
        event_id: Some(9278),
        went_live: true,
        vod_id: Some("bc-1".to_string()),
        reason: Some("why".to_string()),
        quota_units: units,
        created_at: created_at.to_string(),
        ready_at: Some("2026-10-06T10:01:00.000Z".to_string()),
        stop_requested_at: Some("2026-10-06T10:02:00.000Z".to_string()),
        processing_at: Some("2026-10-06T10:03:00.000Z".to_string()),
        finished_at: Some("2026-10-06T10:04:00.000Z".to_string()),
    }
}

#[tokio::test]
async fn save_then_get_round_trips_every_column() {
    let p = pool().await;
    let row = full_row("s1", "2026-10-06T10:00:00.000Z", "done", 321);
    save(&p, &row).await.unwrap();
    assert_eq!(get(&p, "s1").await.unwrap(), Some(row));
}

#[tokio::test]
async fn new_starting_row_round_trips_with_empty_optionals() {
    let p = pool().await;
    let row = AvGateSessionRow::new_starting("s2", "restreamer-ci", "t", "2026-10-06T10:00:00Z");
    assert_eq!(row.state, "starting");
    assert!(!row.went_live);
    assert_eq!(row.quota_units, 0);
    save(&p, &row).await.unwrap();
    assert_eq!(get(&p, "s2").await.unwrap(), Some(row));
}

#[tokio::test]
async fn save_overwrites_an_existing_row() {
    let p = pool().await;
    let mut row = AvGateSessionRow::new_starting("s3", "r", "t", "2026-10-06T10:00:00Z");
    save(&p, &row).await.unwrap();
    row.state = "ready".to_string();
    row.went_live = true;
    row.quota_units = 151;
    row.broadcast_id = Some("bc".to_string());
    save(&p, &row).await.unwrap();
    assert_eq!(get(&p, "s3").await.unwrap(), Some(row));
}

#[tokio::test]
async fn get_of_an_unknown_id_is_none() {
    let p = pool().await;
    assert_eq!(get(&p, "nope").await.unwrap(), None);
}

#[tokio::test]
async fn list_unfinished_skips_terminal_rows_oldest_first() {
    let p = pool().await;
    save(&p, &full_row("b", "2026-10-06T10:02:00.000Z", "ready", 0))
        .await
        .unwrap();
    save(
        &p,
        &full_row("a", "2026-10-06T10:01:00.000Z", "starting", 0),
    )
    .await
    .unwrap();
    save(
        &p,
        &full_row("c", "2026-10-06T10:03:00.000Z", "processing", 0),
    )
    .await
    .unwrap();
    save(&p, &full_row("d", "2026-10-06T10:00:00.000Z", "done", 0))
        .await
        .unwrap();
    save(&p, &full_row("e", "2026-10-06T10:00:30.000Z", "failed", 0))
        .await
        .unwrap();
    let ids: Vec<String> = list_unfinished(&p)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.id)
        .collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
}

#[tokio::test]
async fn quota_units_since_sums_only_sessions_at_or_after_the_cutoff() {
    let p = pool().await;
    save(
        &p,
        &full_row("old", "2026-10-05T09:59:59.999Z", "done", 1000),
    )
    .await
    .unwrap();
    save(
        &p,
        &full_row("edge", "2026-10-05T10:00:00.000Z", "failed", 7),
    )
    .await
    .unwrap();
    save(
        &p,
        &full_row("new", "2026-10-06T08:00:00.000Z", "ready", 300),
    )
    .await
    .unwrap();
    assert_eq!(
        quota_units_since(&p, "2026-10-05T10:00:00.000Z")
            .await
            .unwrap(),
        307
    );
    assert_eq!(
        quota_units_since(&p, "2026-10-07T00:00:00.000Z")
            .await
            .unwrap(),
        0
    );
}
