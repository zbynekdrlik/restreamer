//! Unit tests for the embedded-frontend serving, cache-control, and the
//! version drift self-check. Exercised against a committed fixture asset set
//! (`tests/fixtures/`) so no `trunk build` is required.

use super::*;
use axum::body::to_bytes;
use axum::http::header;
use rust_embed::RustEmbed;

/// Fixture asset set standing in for a real trunk build.
#[derive(RustEmbed)]
#[folder = "$CARGO_MANIFEST_DIR/tests/fixtures"]
struct TestAssets;

const HASHED_JS: &str = "leptos-ui-0123456789abcdef.js";

async fn body_string(resp: Response) -> String {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn root_serves_index_html() {
    let resp = serve::<TestAssets>("/");
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(body_string(resp).await.contains("FIXTURE-INDEX"));
}

#[tokio::test]
async fn unknown_route_falls_back_to_index_for_spa() {
    // A client-side route like /settings has no embedded file — SPA fallback
    // must serve index.html (this is the #107 "dashboard 404" regression).
    let resp = serve::<TestAssets>("/settings");
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(body_string(resp).await.contains("FIXTURE-INDEX"));
}

#[tokio::test]
async fn hashed_asset_is_served_and_immutable() {
    let resp = serve::<TestAssets>(&format!("/{HASHED_JS}"));
    assert_eq!(resp.status(), StatusCode::OK);
    let cc = resp
        .headers()
        .get(header::CACHE_CONTROL)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        cc.contains("immutable"),
        "hashed asset must be immutable: {cc}"
    );
    assert!(cc.contains("max-age=31536000"));
    assert!(body_string(resp).await.contains("FIXTURE-JS"));
}

#[tokio::test]
async fn index_is_no_cache() {
    let resp = serve::<TestAssets>("/");
    let cc = resp
        .headers()
        .get(header::CACHE_CONTROL)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(cc, "no-cache");
}

#[tokio::test]
async fn empty_asset_set_returns_404() {
    // No fixtures embedded → honest 404 (the non-trunk workspace-build state).
    #[derive(RustEmbed)]
    #[folder = "$CARGO_MANIFEST_DIR/tests/empty"]
    struct EmptyAssets;
    let resp = serve::<EmptyAssets>("/");
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[test]
fn cache_control_classifies_assets() {
    assert_eq!(cache_control_for("index.html"), "no-cache");
    assert_eq!(cache_control_for("version.txt"), "no-cache");
    assert_eq!(cache_control_for("manifest.json"), "no-cache");
    assert_eq!(cache_control_for("sw.js"), "no-cache"); // not hashed
    assert_eq!(
        cache_control_for("leptos-ui-0123456789abcdef.js"),
        "public, max-age=31536000, immutable"
    );
    assert_eq!(
        cache_control_for("leptos-ui-0123456789abcdef_bg.wasm"),
        "public, max-age=31536000, immutable"
    );
    assert_eq!(
        cache_control_for("style-0123456789abcdef.css"),
        "public, max-age=31536000, immutable"
    );
}

#[test]
fn version_is_read_from_embedded_file() {
    assert_eq!(version_of::<TestAssets>().as_deref(), Some("9.9.9-fixture"));
}

#[test]
fn version_drift_detected_and_clean() {
    // Mismatch → drift reported.
    assert_eq!(
        compare_versions(Some("0.29.26"), "0.29.27"),
        Some(("0.29.26".to_string(), "0.29.27".to_string()))
    );
    // Match → no drift.
    assert_eq!(compare_versions(Some("0.29.27"), "0.29.27"), None);
    // Unbuilt frontend → never a drift.
    assert_eq!(compare_versions(None, "0.29.27"), None);
}
