//! #336 — the public `GET /api/v1/config` response must never carry a
//! credential in plaintext, and a client echoing a mask back through `PATCH`
//! must never overwrite the real credential with the mask.
//!
//! The oracle in this file is deliberately INDEPENDENT of the production
//! redaction code: it re-derives "is this field name a credential?" from its
//! own marker list and its own readable-path list. Importing the production
//! predicate would make the central test vacuous — an over-broad production
//! allowlist would silently disable the very assertion this file exists to
//! enforce.
//!
//! Every value here is FAKE. No real credential belongs in a fixture.

use crate::router::build_router;
use crate::state::AppState;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use rs_core::config::Config;
use rs_core::db;
use rs_core::models::WsEvent;
use serde_json::Value;
use tokio::sync::broadcast;
use tower::ServiceExt;

/// Prefix for every fixture credential — fake, never a real secret.
const FAKE: &str = "fake-not-a-real-credential";

/// The mask the UI already renders for s3/hetzner/obs credentials.
const MASK: &str = "***";

/// Case-insensitive field-name markers that make a config field a credential.
///
/// Do NOT "sync" this list with the production one in
/// `rs_core::config_redact` — it is a deliberate second opinion. If both lists
/// are edited together, this file stops testing anything. (The production
/// module's `config_inventory_is_fully_classified` test is the mechanism that
/// catches a field neither list's markers happen to match.)
const SECRET_MARKERS: &[&str] = &[
    "secret",
    "token",
    "key",
    "password",
    "passphrase",
    "webhook",
];

/// Dotted paths that match a marker but are NOT credentials and must stay
/// readable: a Hetzner SSH key *name* and TLS *file paths*.
const READABLE_PATHS: &[&str] = &[
    "hetzner.ssh_key_name",
    "hetzner.extra_ssh_key_names",
    "api.tls_cert",
    "api.tls_key",
];

fn is_secretish(path: &str, key: &str) -> bool {
    if READABLE_PATHS.contains(&path) {
        return false;
    }
    let lower = key.to_ascii_lowercase();
    SECRET_MARKERS.iter().any(|m| lower.contains(m))
}

/// A config with EVERY credential field populated, so the walk below has
/// something to catch on each one.
fn secret_laden_config() -> Config {
    let mut c = Config::for_testing();
    c.s3.access_key_id = format!("{FAKE}-s3-access");
    c.s3.secret_access_key = format!("{FAKE}-s3-secret");
    c.hetzner.api_token = format!("{FAKE}-hetzner-api");
    c.hetzner.ssh_key_name = "restreamer-ci".to_string();
    c.hetzner.extra_ssh_key_names = vec!["debug-key".to_string()];
    c.youtube.client_id = "1234-fake.apps.googleusercontent.com".to_string();
    c.youtube.client_secret = format!("{FAKE}-yt-client");
    c.youtube.device_flow.client_id = "5678-fake.apps.googleusercontent.com".to_string();
    c.youtube.device_flow.client_secret = format!("{FAKE}-device-flow");
    c.notifications.discord_bot_token = format!("{FAKE}-discord-bot");
    c.notifications.discord_webhook_url = format!("https://discord.test/api/webhooks/{FAKE}");
    c.notifications.discord_channel_id = "1234567890".to_string();
    c.obs.ws_password = format!("{FAKE}-obs-ws");
    c.facebook.enabled = true;
    c.facebook.page_id = "163104934022649".to_string();
    c.facebook.page_access_token = format!("{FAKE}-fb-page");
    c
}

/// Collect the dotted paths of every secret-ish field returned unmasked.
/// Reports the path and the value LENGTH only — never the value.
fn unmasked_secret_paths(value: &Value) -> Vec<String> {
    let mut out = Vec::new();
    walk("", value, &mut out);
    out
}

fn walk(path: &str, value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let child_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                if is_secretish(&child_path, key) {
                    collect_unmasked(&child_path, child, out);
                } else {
                    walk(&child_path, child, out);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                walk(path, item, out);
            }
        }
        _ => {}
    }
}

fn collect_unmasked(path: &str, value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(s) => {
            if s != MASK {
                out.push(format!("{path} (unmasked, {} chars)", s.len()));
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_unmasked(path, item, out);
            }
        }
        Value::Object(map) => {
            for (key, child) in map {
                collect_unmasked(&format!("{path}.{key}"), child, out);
            }
        }
        // Numbers / bools / null carry no credential material.
        _ => {}
    }
}

async fn state_with(config: Config) -> AppState {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    AppState::new_for_tests(pool, config, ws_tx)
}

async fn get_config_json(state: AppState) -> Value {
    let app = build_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/config")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}

/// The central deny-by-default assertion: NO field whose name matches a secret
/// marker may come back unmasked. A newly added credential field is covered by
/// this test without anyone editing it.
#[tokio::test]
async fn public_config_get_masks_every_secretish_field() {
    // Positive control FIRST: prove the oracle can still fire. Without this the
    // test goes vacuously green if a field is renamed, the marker list is
    // truncated, or the fixture stops populating credentials — it would then
    // assert "nothing leaked" while inspecting nothing.
    let raw = serde_json::to_value(secret_laden_config()).unwrap();
    let would_leak = unmasked_secret_paths(&raw);
    assert!(
        would_leak.len() >= 8,
        "the oracle went blind — it flags only {} field(s) on the RAW config: {would_leak:?}",
        would_leak.len()
    );

    let state = state_with(secret_laden_config()).await;
    let json = get_config_json(state).await;
    let leaked = unmasked_secret_paths(&json);
    assert!(
        leaked.is_empty(),
        "GET /api/v1/config returned secret-ish fields unmasked: {leaked:?}"
    );
}

/// Named coverage for the two fields the live public endpoint actually leaked.
#[tokio::test]
async fn public_config_get_masks_device_flow_secret_and_discord_bot_token() {
    let state = state_with(secret_laden_config()).await;
    let json = get_config_json(state).await;
    assert_eq!(json["youtube"]["device_flow"]["client_secret"], MASK);
    assert_eq!(json["notifications"]["discord_bot_token"], MASK);
}

/// Deny-by-default must not blind the UI or CI: names, paths and ids that only
/// LOOK secret-ish stay readable.
#[tokio::test]
async fn public_config_get_keeps_names_paths_and_ids_readable() {
    let state = state_with(secret_laden_config()).await;
    let json = get_config_json(state).await;
    // A Hetzner key *name*, not key material.
    assert_eq!(json["hetzner"]["ssh_key_name"], "restreamer-ci");
    assert_eq!(json["hetzner"]["extra_ssh_key_names"][0], "debug-key");
    // TLS file *paths*, not inline material.
    assert_eq!(json["api"]["tls_cert"], "cert.pem");
    assert_eq!(json["api"]["tls_key"], "key.pem");
    // CI reads client_uuid off this endpoint (ci.yml Hetzner orphan cleanup).
    assert_eq!(json["client_uuid"], "test-uuid-00000000");
    assert_eq!(json["s3"]["bucket"], "test-bucket");
    // The Leptos settings screen reads obs.ws_url.
    assert_eq!(json["obs"]["ws_url"], "ws://127.0.0.1:4455");
    // Non-secret neighbours inside a section that also holds a credential.
    assert_eq!(json["notifications"]["discord_channel_id"], "1234567890");
    assert_eq!(
        json["youtube"]["client_id"],
        "1234-fake.apps.googleusercontent.com"
    );
    assert_eq!(json["youtube"]["device_flow"]["daily_quota"], 10_000);
}

/// The masked response must still deserialize into `Config` — the typed
/// round-trip several callers rely on.
#[tokio::test]
async fn masked_config_response_still_deserializes_into_config() {
    let state = state_with(secret_laden_config()).await;
    let json = get_config_json(state).await;
    let parsed: Config = serde_json::from_value(json).unwrap();
    assert_eq!(parsed.youtube.device_flow.client_secret, MASK);
    assert_eq!(parsed.youtube.device_flow.daily_quota, 10_000);
    assert_eq!(parsed.hetzner.ssh_key_name, "restreamer-ci");
}

/// The other half of the fix: because GET now masks these fields, a client that
/// PATCHes the config back verbatim sends the MASK. That must restore the real
/// credential, never persist `***` over it.
#[tokio::test]
async fn patch_config_echoing_masks_does_not_destroy_credentials() {
    let original = secret_laden_config();
    let state = state_with(original.clone()).await;
    let app = build_router(state.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "youtube": {
                            "client_secret": MASK,
                            "device_flow": { "client_secret": MASK }
                        },
                        "notifications": {
                            "discord_bot_token": MASK,
                            "discord_webhook_url": MASK
                        },
                        "obs": { "ws_password": MASK }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let live = state.config_live.read().unwrap().clone();
    assert_eq!(
        live.youtube.device_flow.client_secret, original.youtube.device_flow.client_secret,
        "device-flow client_secret was overwritten by the mask"
    );
    assert_eq!(
        live.notifications.discord_bot_token, original.notifications.discord_bot_token,
        "discord_bot_token was overwritten by the mask"
    );
    assert_eq!(
        live.notifications.discord_webhook_url, original.notifications.discord_webhook_url,
        "discord_webhook_url was overwritten by the mask"
    );
    assert_eq!(live.youtube.client_secret, original.youtube.client_secret);
    assert_eq!(live.obs.ws_password, original.obs.ws_password);
}

/// A PATCH that sets a REAL new value for a masked field must still take
/// effect — the restore must only trigger on the mask itself.
#[tokio::test]
async fn patch_config_still_accepts_a_real_new_credential() {
    let state = state_with(secret_laden_config()).await;
    let app = build_router(state.clone());
    let new_token = format!("{FAKE}-rotated-discord-bot");

    let response = app
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/config")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "notifications": { "discord_bot_token": new_token }
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let live = state.config_live.read().unwrap().clone();
    assert_eq!(live.notifications.discord_bot_token, new_token);

    // …and the PATCH response itself must come back masked.
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    let leaked = unmasked_secret_paths(&json);
    assert!(
        leaked.is_empty(),
        "PATCH /api/v1/config response returned secret-ish fields unmasked: {leaked:?}"
    );
}
