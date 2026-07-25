//! Best-effort cleanup of orphaned rescue-video S3 objects (#115).
//!
//! `S3Client::upload_public_object` gives every rescue-video upload a fresh
//! UUID key under `rescue-videos/`. Nothing used to delete the OLD object
//! when a template/event's `rescue_video_url` was replaced or cleared, or
//! when the owning template/event was deleted -- so the bucket accumulated
//! orphans forever. This module is the single shared cleanup, called from
//! every place `rescue_video_url` can change or disappear:
//! `template_handlers::update_template`, `template_handlers::delete_template`,
//! `stream_handlers::update_event`, and `handlers::delete_event_by_id`.
//!
//! Deletion never fails the parent request: a stray S3 object is tech
//! debt, not a correctness problem, and an S3 hiccup here must not block
//! an unrelated template/event update or delete.

use rs_core::db;
use rs_endpoint::s3::S3Client;
use sqlx::SqlitePool;
use tracing::{info, warn};

/// If `old_url` was replaced or cleared by `new_url`, and it is one of our
/// own rescue-video uploads, and no OTHER template/event still references
/// it, delete the S3 object. No-op when `old_url` is absent, unchanged, or
/// externally hosted (something an operator pasted in manually).
///
/// `exclude_template_id` / `exclude_event_id` identify the row being
/// updated or deleted -- so a same-row change is never mistaken for a
/// still-live reference from itself.
pub async fn cleanup_orphaned_rescue_video(
    pool: &SqlitePool,
    s3_client: &S3Client,
    old_url: Option<&str>,
    new_url: Option<&str>,
    exclude_template_id: Option<i64>,
    exclude_event_id: Option<i64>,
) {
    let Some(old_url) = old_url else {
        return;
    };
    if Some(old_url) == new_url {
        return;
    }
    let Some(key) = s3_client.rescue_video_key_from_url(old_url) else {
        return;
    };

    match db::rescue_video_url_referenced_elsewhere(
        pool,
        old_url,
        exclude_template_id,
        exclude_event_id,
    )
    .await
    {
        Ok(true) => {
            info!(
                "Rescue video {old_url} still referenced by another template/event -- not deleting"
            );
        }
        Ok(false) => match s3_client.delete_object(&key).await {
            Ok(()) => info!("Deleted orphaned rescue video S3 object: {key}"),
            Err(e) => warn!("Failed to delete orphaned rescue video {key}: {e}"),
        },
        Err(e) => {
            warn!("Failed to check rescue video references for {old_url}: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_core::config::S3Config;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn s3_config(endpoint: &str) -> S3Config {
        S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: endpoint.to_string(),
            access_key_id: "key".to_string(),
            secret_access_key: "secret".to_string(),
        }
    }

    async fn test_pool() -> SqlitePool {
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn deletes_old_object_when_url_changes_and_unreferenced() {
        let mock = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/test-bucket/rescue-videos/old.flv"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock)
            .await;

        let pool = test_pool().await;
        let s3 = S3Client::new(&s3_config(&mock.uri())).unwrap();
        let old_url = format!("{}/test-bucket/rescue-videos/old.flv", mock.uri());

        cleanup_orphaned_rescue_video(&pool, &s3, Some(&old_url), Some("https://new"), None, None)
            .await;
        // mock's `.expect(1)` is verified on MockServer drop at end of test.
    }

    #[tokio::test]
    async fn deletes_old_object_when_cleared_to_none() {
        let mock = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/test-bucket/rescue-videos/cleared.flv"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&mock)
            .await;

        let pool = test_pool().await;
        let s3 = S3Client::new(&s3_config(&mock.uri())).unwrap();
        let old_url = format!("{}/test-bucket/rescue-videos/cleared.flv", mock.uri());

        cleanup_orphaned_rescue_video(&pool, &s3, Some(&old_url), None, None, None).await;
    }

    #[tokio::test]
    async fn skips_delete_when_url_unchanged() {
        let mock = MockServer::start().await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&mock)
            .await;

        let pool = test_pool().await;
        let s3 = S3Client::new(&s3_config(&mock.uri())).unwrap();
        let url = format!("{}/test-bucket/rescue-videos/same.flv", mock.uri());

        cleanup_orphaned_rescue_video(&pool, &s3, Some(&url), Some(&url), None, None).await;
    }

    #[tokio::test]
    async fn skips_delete_for_externally_hosted_url() {
        let mock = MockServer::start().await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&mock)
            .await;

        let pool = test_pool().await;
        let s3 = S3Client::new(&s3_config(&mock.uri())).unwrap();

        cleanup_orphaned_rescue_video(
            &pool,
            &s3,
            Some("https://s3.example.com/manually-typed.mp4"),
            None,
            None,
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn skips_delete_when_still_referenced_by_another_template() {
        let mock = MockServer::start().await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&mock)
            .await;

        let pool = test_pool().await;
        let s3 = S3Client::new(&s3_config(&mock.uri())).unwrap();
        let shared_url = format!("{}/test-bucket/rescue-videos/shared.flv", mock.uri());

        // A SECOND template still points at this URL.
        db::create_template(&pool, "other-template", None, Some(shared_url.clone()))
            .await
            .unwrap();

        // Simulate updating a DIFFERENT template (id 999, excluded) away
        // from shared_url -- must NOT delete, since the other template
        // above still references it.
        cleanup_orphaned_rescue_video(
            &pool,
            &s3,
            Some(&shared_url),
            Some("https://new"),
            Some(999),
            None,
        )
        .await;
    }

    #[tokio::test]
    async fn skips_delete_when_old_url_is_none() {
        let mock = MockServer::start().await;
        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&mock)
            .await;

        let pool = test_pool().await;
        let s3 = S3Client::new(&s3_config(&mock.uri())).unwrap();

        cleanup_orphaned_rescue_video(&pool, &s3, None, Some("https://new"), None, None).await;
    }
}
