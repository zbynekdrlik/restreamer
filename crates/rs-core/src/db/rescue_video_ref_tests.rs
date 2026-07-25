//! Tests for `rescue_video_url_referenced_elsewhere` (#115). Split out of
//! `tests.rs` to keep that file under the 1000-line cap.

use super::*;

async fn setup_db() -> sqlx::sqlite::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn rescue_video_url_not_referenced_when_unused() {
    let pool = setup_db().await;
    let referenced = rescue_video_url_referenced_elsewhere(
        &pool,
        "https://s3.example.com/rescue-videos/abc.flv",
        None,
        None,
    )
    .await
    .unwrap();
    assert!(!referenced);
}

#[tokio::test]
async fn rescue_video_url_referenced_by_another_template() {
    let pool = setup_db().await;
    let url = "https://s3.example.com/rescue-videos/shared.flv".to_string();
    create_template(&pool, "tpl-a", None, Some(url.clone()))
        .await
        .unwrap();

    // No exclusion -- the template above must count as a reference.
    let referenced = rescue_video_url_referenced_elsewhere(&pool, &url, None, None)
        .await
        .unwrap();
    assert!(referenced);
}

#[tokio::test]
async fn rescue_video_url_not_referenced_when_only_match_is_excluded() {
    let pool = setup_db().await;
    let url = "https://s3.example.com/rescue-videos/mine.flv".to_string();
    let tpl_id = create_template(&pool, "tpl-b", None, Some(url.clone()))
        .await
        .unwrap();

    // Excluding the template's own id -- no OTHER row references the url.
    let referenced = rescue_video_url_referenced_elsewhere(&pool, &url, Some(tpl_id), None)
        .await
        .unwrap();
    assert!(!referenced);
}

#[tokio::test]
async fn rescue_video_url_referenced_by_event_created_from_template() {
    let pool = setup_db().await;
    let url = "https://s3.example.com/rescue-videos/inherited.flv".to_string();
    let tpl_id = create_template(&pool, "tpl-c", None, Some(url.clone()))
        .await
        .unwrap();
    create_streaming_event(&pool, "evt-from-tpl").await.unwrap();
    let events = list_streaming_events(&pool).await.unwrap();
    let event_id = events[0].id;
    update_streaming_event(&pool, event_id, "evt-from-tpl", None, Some(url.clone()))
        .await
        .unwrap();

    // Excluding only the template -- the event still holds the same url.
    let referenced = rescue_video_url_referenced_elsewhere(&pool, &url, Some(tpl_id), None)
        .await
        .unwrap();
    assert!(referenced);

    // Excluding both -- no reference remains.
    let referenced =
        rescue_video_url_referenced_elsewhere(&pool, &url, Some(tpl_id), Some(event_id))
            .await
            .unwrap();
    assert!(!referenced);
}
