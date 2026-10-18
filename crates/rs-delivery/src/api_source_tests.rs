//! #408: `POST /api/source` sets the event's end, behind the bearer token,
//! and refuses an announcement the source has already outrun.

use super::*;
use crate::disk_cache::download_service::FetchedChunk;
use crate::disk_cache::{DiskCache, DiskCacheConfig, S3Backend};
use axum::body::Body;
use axum::http::Request;
use tokio::sync::RwLock;
use tower::ServiceExt;

const TOKEN: &str = "vps-token";

struct NoS3;

#[async_trait::async_trait]
impl S3Backend for NoS3 {
    async fn fetch(&self, _chunk_id: i64) -> Result<Option<FetchedChunk>, String> {
        Ok(None)
    }
}

/// A VPS after `/api/init`: token set, the event's cache built.
async fn initialised(dir: &std::path::Path) -> (Arc<AppState>, Arc<DiskCache>) {
    let mut state = AppState::new().await;
    state.auth_token = RwLock::new(Some(TOKEN.to_string()));
    let cfg = DiskCacheConfig {
        cache_dir: dir.to_path_buf(),
        eviction_interval_secs: 3600,
        ..Default::default()
    };
    let cache = Arc::new(
        DiskCache::new(cfg, Arc::new(NoS3), "evt".to_string(), None)
            .await
            .unwrap(),
    );
    *state.disk_cache.write().await = Some(Arc::clone(&cache));
    (Arc::new(state), cache)
}

async fn post(state: Arc<AppState>, body: serde_json::Value) -> (StatusCode, serde_json::Value) {
    let req = Request::builder()
        .method("POST")
        .uri("/api/source")
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = crate::api::router(state).oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 16)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test]
async fn the_announced_end_reaches_the_events_endpoints() {
    let tmp = tempfile::tempdir().unwrap();
    let (state, cache) = initialised(tmp.path()).await;
    let (status, body) = post(state, serde_json::json!({ "ended_at_chunk_id": 5 })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({ "ended_at_chunk_id": 5 }));
    assert_eq!(cache.source_end.ended_at(), Some(5));
}

#[tokio::test]
async fn an_announcement_the_source_already_outran_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let (state, cache) = initialised(tmp.path()).await;
    cache.source_end.note_fetched(9);
    let (status, body) = post(state, serde_json::json!({ "ended_at_chunk_id": 5 })).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body, serde_json::json!({ "error": "stale" }));
    assert_eq!(cache.source_end.ended_at(), None);
}

#[tokio::test]
async fn there_is_no_end_to_announce_before_init() {
    let mut state = AppState::new().await;
    state.auth_token = RwLock::new(Some(TOKEN.to_string()));
    let (status, body) = post(
        Arc::new(state),
        serde_json::json!({ "ended_at_chunk_id": 5 }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body, serde_json::json!({ "error": "no_event" }));
}
