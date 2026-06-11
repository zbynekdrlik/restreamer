//! Delivery binary version lockstep — versioned immutable S3 keys
//! (2026-06-10 lockstep + 2026-06-11 race fix).
//!
//! The delivery VPS downloads its `rs-delivery` binary from the CLIENT'S OWN
//! S3 bucket. The original lockstep design (PR #245) used ONE mutable key
//! (`rs-delivery`) plus a plain-text sidecar (`rs-delivery.version`); the
//! client compared the sidecar to its own version and re-uploaded on drift.
//!
//! That shared mutable object can never be made race-safe. On 2026-06-11 two
//! concurrent CI runs — `main` building 0.22.7 and `dev` building 0.22.8 —
//! raced on the single key: last writer won, the deployed 0.22.7 client then
//! saw sidecar=0.22.8, tried to fetch a GitHub release that did not yet exist,
//! and delivery start failed.
//!
//! The fix is to put the version IN THE KEY NAME. Each build uploads its own
//! immutable object `rs-delivery-{version}` ([`binary_key`]); the client of
//! that build requests EXACTLY `rs-delivery-{client_version}`. Concurrent
//! builds/versions write disjoint keys and can never interfere. There is no
//! sidecar, no comparison, no re-upload of a shared object.
//!
//! This module enforces lockstep at two points:
//!
//! 1. **Pre-create** ([`ensure_bucket_binary`]): an anonymous HEAD of the
//!    versioned key. Present → nothing to do. Missing → download the matching
//!    GitHub release asset for the client's OWN version, sha256-verify it, and
//!    upload it under the versioned key with public-read ACL so cloud-init
//!    fetches the correct bytes. Hard error (abort delivery) when no matching
//!    release exists — a loud failure at start beats a silent broken event.
//! 2. **Post-boot** ([`versions_match`]): the orchestrator parses the VPS
//!    `/api/health` `version` field and aborts (delete VPS + Critical audit)
//!    when it differs from the client version.
//!
//! Pure logic ([`binary_key`], [`versions_match`], [`parse_sha256_file`],
//! [`parse_health_version`]) is separated from the async I/O so it is
//! unit-tested without network access.

use std::time::Duration;

use anyhow::{Context, anyhow};
use rs_cloud::hetzner::HetznerClient;
use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_core::db;
use sha2::{Digest, Sha256};
use sqlx::SqlitePool;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

/// GitHub repo that hosts the `rs-delivery` release assets.
const RELEASE_BASE: &str =
    "https://github.com/zbynekdrlik/restreamer/releases/download/restreamer-v";

/// Versioned, immutable S3 object key for the rs-delivery binary. The key
/// name pins the version, so concurrent builds/versions can never race on
/// a shared mutable object (2026-06-11 incident: main 0.22.7 and dev 0.22.8
/// CI runs overwrote each other's `rs-delivery` + sidecar).
pub fn binary_key(version: &str) -> String {
    format!("rs-delivery-{version}")
}

/// Public anonymous URL of this client's versioned rs-delivery object, used by
/// cloud-init to download the binary. Keeps the `start_delivery` call site to
/// one line (delivery.rs is at its 1000-line cap).
pub fn binary_url(config: &rs_core::config::Config, client_version: &str) -> String {
    format!(
        "{}/{}/{}",
        config.s3.endpoint,
        config.s3.bucket,
        binary_key(client_version)
    )
}

/// Post-boot gate: does the VPS-reported version match the client's?
///
/// `None` (old binary with no `version` field in `/api/health`) is a mismatch
/// by design — the gate must abort rather than assume.
pub fn versions_match(vps_version: Option<&str>, client_version: &str) -> bool {
    matches!(vps_version, Some(v) if v == client_version)
}

/// Extract the optional `version` field from an rs-delivery `/api/health`
/// JSON body. Old binaries that predate the field deserialize to `None`
/// (mismatch by design). Malformed JSON also yields `None`.
pub fn parse_health_version(body: &str) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct HealthBody {
        #[serde(default)]
        version: Option<String>,
    }
    serde_json::from_str::<HealthBody>(body)
        .ok()
        .and_then(|h| h.version)
}

/// Parse a `sha256sum`-format file (`<hex sha256>  <filename>`) into the
/// lowercased hex digest. Returns `None` if the first token is not exactly
/// 64 hex characters.
pub fn parse_sha256_file(content: &str) -> Option<String> {
    let token = content.split_whitespace().next()?;
    if token.len() == 64 && token.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(token.to_ascii_lowercase())
    } else {
        None
    }
}

/// Compute the lowercase hex sha256 of `bytes`.
fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(&hasher.finalize())
}

/// Lowercase hex encoding (avoids pulling a `hex` crate dep).
fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Ensure the client bucket holds the immutable `rs-delivery-{client_version}`
/// object.
///
/// Cheap when present (one anonymous HEAD). When absent, downloads the GitHub
/// release asset for the client's OWN version, verifies sha256, and uploads it
/// under the versioned key with public-read ACL.
///
/// Returns `Ok(Some(sha256))` if an upload happened (the verified digest of
/// the uploaded binary), `Ok(None)` if the versioned object was already
/// present. `Err` means delivery start MUST abort — the caller surfaces and
/// audits.
pub async fn ensure_bucket_binary(
    config: &rs_core::config::Config,
    client_version: &str,
) -> anyhow::Result<Option<String>> {
    if versioned_object_present(config, client_version).await {
        info!(
            version = client_version,
            "delivery binary lockstep: rs-delivery-{client_version} present in bucket, proceeding"
        );
        return Ok(None);
    }
    warn!(
        client_version,
        "delivery binary lockstep: rs-delivery-{client_version} absent, uploading from release"
    );
    let sha = upload_release_binary(config, client_version).await?;
    Ok(Some(sha))
}

/// Anonymous HEAD of `{endpoint}/{bucket}/rs-delivery-{client_version}`.
/// HTTP 200 → the immutable object is present (`true`). Any non-200 or
/// network error → treat as absent (`false`), which forces an upload.
async fn versioned_object_present(config: &rs_core::config::Config, client_version: &str) -> bool {
    let url = format!(
        "{}/{}/{}",
        config.s3.endpoint,
        config.s3.bucket,
        binary_key(client_version)
    );
    let client = reqwest::Client::new();
    match client
        .head(&url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => true,
        Ok(resp) => {
            info!(%url, status = %resp.status(), "versioned binary not present (will upload)");
            false
        }
        Err(e) => {
            warn!(%url, "versioned binary HEAD failed (will upload): {e}");
            false
        }
    }
}

/// Download the release asset for `client_version`, sha256-verify it against
/// the `.sha256` sidecar, and upload it to the client bucket under the
/// versioned immutable key with public-read ACL. Returns the verified sha256.
async fn upload_release_binary(
    config: &rs_core::config::Config,
    client_version: &str,
) -> anyhow::Result<String> {
    let asset = format!("rs-delivery-{client_version}-linux-amd64");
    let bin_url = format!("{RELEASE_BASE}{client_version}/{asset}");
    let sha_url = format!("{bin_url}.sha256");
    let client = reqwest::Client::new();

    // Binary
    let bin_resp = client
        .get(&bin_url)
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .with_context(|| format!("GET release asset {bin_url}"))?;
    if bin_resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(anyhow!(
            "no release asset for v{client_version} and bucket has no \
             rs-delivery-{client_version} — cannot guarantee VPS binary version"
        ));
    }
    if !bin_resp.status().is_success() {
        return Err(anyhow!(
            "release asset {bin_url} returned {}",
            bin_resp.status()
        ));
    }
    let bin_bytes = bin_resp
        .bytes()
        .await
        .with_context(|| format!("read release asset body {bin_url}"))?;
    info!(
        version = client_version,
        bytes = bin_bytes.len(),
        "delivery binary lockstep: downloaded release asset"
    );

    // Expected sha256
    let sha_resp = client
        .get(&sha_url)
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .with_context(|| format!("GET release sha256 {sha_url}"))?;
    if !sha_resp.status().is_success() {
        return Err(anyhow!(
            "release sha256 {sha_url} returned {}",
            sha_resp.status()
        ));
    }
    let sha_body = sha_resp
        .text()
        .await
        .with_context(|| format!("read sha256 body {sha_url}"))?;
    let expected = parse_sha256_file(&sha_body)
        .ok_or_else(|| anyhow!("unparseable sha256 file for v{client_version}: {sha_body:?}"))?;

    // Verify
    let actual = sha256_hex(&bin_bytes);
    if actual != expected {
        return Err(anyhow!(
            "sha256 mismatch for v{client_version}: expected {expected}, got {actual} \
             — refusing to upload an unverified binary"
        ));
    }
    info!(
        version = client_version,
        sha256 = %actual,
        "delivery binary lockstep: sha256 verified"
    );

    // Upload under the versioned immutable key (public-read so cloud-init
    // fetches anonymously).
    let key = binary_key(client_version);
    let s3 = rs_endpoint::s3::S3Client::new(&config.s3)
        .map_err(|e| anyhow!("S3Client::new for binary upload: {e}"))?;
    s3.upload_public_object(&key, &bin_bytes, "application/octet-stream")
        .await
        .map_err(|e| anyhow!("upload {key}: {e}"))?;
    info!(
        version = client_version,
        sha256 = %actual,
        key = %key,
        "delivery binary lockstep: uploaded versioned binary (public-read)"
    );

    Ok(actual)
}

/// Pre-create lockstep (2026-06-10 incident, 2026-06-11 race fix): ensure the
/// client bucket holds the immutable `rs-delivery-{client_version}` object
/// before a VPS is created. Cheap HEAD; uploads the matching release asset
/// when absent; hard error (audit Critical + abort) when impossible (no
/// release asset / sha mismatch / upload failure) so the event start fails
/// loudly instead of silently streaming a wrong-version binary.
pub async fn ensure_bucket_binary_or_abort(
    config: &rs_core::config::Config,
    audit_tx: Option<&mpsc::Sender<AuditRow>>,
    event_id: i64,
    binary_url: &str,
) -> anyhow::Result<()> {
    let client_version = env!("CARGO_PKG_VERSION");
    match ensure_bucket_binary(config, client_version).await {
        Ok(Some(sha256)) => {
            if let Some(tx) = audit_tx {
                rs_core::audit::record(tx, ensured_audit(event_id, client_version, &sha256));
            }
            Ok(())
        }
        Ok(None) => Ok(()),
        Err(e) => {
            if let Some(tx) = audit_tx {
                rs_core::audit::record(
                    tx,
                    pre_create_mismatch_audit(event_id, client_version, binary_url, &e.to_string()),
                );
            }
            Err(e)
        }
    }
}

/// Post-boot lockstep abort (2026-06-10 incident): the booted VPS reported a
/// different rs-delivery version than the client. Audit Critical, delete the
/// VPS (cause fully known — no post-mortem value), mark the instance failed,
/// and return a loud error so delivery start fails visibly instead of
/// silently streaming a wrong-version binary.
#[allow(clippy::too_many_arguments)]
pub async fn abort_on_version_mismatch(
    hetzner: &HetznerClient,
    pool: &SqlitePool,
    audit_tx: Option<&mpsc::Sender<AuditRow>>,
    event_id: i64,
    instance_id: i64,
    hetzner_id: i64,
    vps_version: Option<&str>,
    client_version: &str,
) -> anyhow::Error {
    warn!(
        hetzner_id,
        instance_id,
        ?vps_version,
        client_version,
        "rs-delivery VERSION MISMATCH — aborting delivery, deleting VPS"
    );
    if let Some(tx) = audit_tx {
        rs_core::audit::record(
            tx,
            post_boot_mismatch_audit(event_id, instance_id, vps_version, client_version),
        );
    }
    if let Err(e) = hetzner.delete_server(hetzner_id).await {
        error!(hetzner_id, "version-mismatch VPS delete failed: {e}");
    }
    if let Err(e) = db::update_delivery_instance_status(pool, instance_id, "failed").await {
        error!(instance_id, "version-mismatch status update failed: {e}");
    }
    anyhow!(
        "rs-delivery version mismatch: VPS reports {vps_version:?}, \
         client is {client_version} — delivery aborted"
    )
}

/// Info audit row: the versioned binary was uploaded to match the client
/// version before VPS creation. Keeps the `start_delivery` call site to one
/// `record()` line (delivery.rs is at its 1000-line cap).
pub fn ensured_audit(event_id: i64, version: &str, sha256: &str) -> AuditRow {
    AuditRow {
        severity: Severity::Info,
        source: Source::Delivery,
        event_id: Some(event_id),
        instance_id: None,
        endpoint: None,
        action: Action::DeliveryBinaryEnsured,
        detail: serde_json::json!({ "version": version, "sha256": sha256 }),
        ts_override: None,
    }
}

/// Critical audit row for a pre-create lockstep failure (no release asset,
/// sha mismatch, or upload failure) — the delivery start is aborted before a
/// VPS is ever created.
pub fn pre_create_mismatch_audit(
    event_id: i64,
    client_version: &str,
    binary_url: &str,
    error: &str,
) -> AuditRow {
    AuditRow {
        severity: Severity::Critical,
        source: Source::Delivery,
        event_id: Some(event_id),
        instance_id: None,
        endpoint: None,
        action: Action::DeliveryBinaryVersionMismatch,
        detail: serde_json::json!({
            "vps_version": serde_json::Value::Null,
            "client_version": client_version,
            "binary_url": binary_url,
            "phase": "pre_create",
            "error": error,
        }),
        ts_override: None,
    }
}

/// Critical audit row for a post-boot lockstep failure: the booted VPS
/// reported a different rs-delivery version than the client.
pub fn post_boot_mismatch_audit(
    event_id: i64,
    instance_id: i64,
    vps_version: Option<&str>,
    client_version: &str,
) -> AuditRow {
    AuditRow {
        severity: Severity::Critical,
        source: Source::Delivery,
        event_id: Some(event_id),
        instance_id: Some(instance_id),
        endpoint: None,
        action: Action::DeliveryBinaryVersionMismatch,
        detail: serde_json::json!({
            "vps_version": vps_version,
            "client_version": client_version,
            "phase": "post_boot",
        }),
        ts_override: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_key_pins_version() {
        assert_eq!(binary_key("0.22.8"), "rs-delivery-0.22.8");
        assert_eq!(binary_key("0.22.7"), "rs-delivery-0.22.7");
    }

    #[test]
    fn binary_key_distinct_per_version() {
        // The whole point of the 2026-06-11 race fix: different versions
        // produce different keys, so concurrent builds never collide.
        assert_ne!(binary_key("0.22.7"), binary_key("0.22.8"));
    }

    #[test]
    fn binary_url_joins_endpoint_bucket_versioned_key() {
        // Set the S3 fields explicitly — asserting Config::default() values
        // couples the test to unrelated config defaults (broke on CI when
        // the real defaults differed from the assumption).
        let mut cfg = rs_core::config::Config::default();
        cfg.s3.endpoint = "http://localhost:9000".to_string();
        cfg.s3.bucket = "test-bucket".to_string();
        assert_eq!(
            binary_url(&cfg, "0.22.8"),
            "http://localhost:9000/test-bucket/rs-delivery-0.22.8"
        );
    }

    #[test]
    fn versions_match_none_is_mismatch() {
        assert!(!versions_match(None, "0.22.7"));
    }

    #[test]
    fn versions_match_empty_is_mismatch() {
        assert!(!versions_match(Some(""), "0.22.7"));
    }

    #[test]
    fn versions_match_wrong_is_mismatch() {
        assert!(!versions_match(Some("0.22.6"), "0.22.7"));
    }

    #[test]
    fn versions_match_exact_is_match() {
        assert!(versions_match(Some("0.22.7"), "0.22.7"));
    }

    #[test]
    fn parse_health_version_extracts_field() {
        assert_eq!(
            parse_health_version(r#"{"status":"ok","version":"0.22.7"}"#).as_deref(),
            Some("0.22.7")
        );
    }

    #[test]
    fn parse_health_version_missing_field_is_none() {
        // Old binary: no version field.
        assert_eq!(parse_health_version(r#"{"status":"ok"}"#), None);
    }

    #[test]
    fn parse_health_version_garbage_is_none() {
        assert_eq!(parse_health_version("not json"), None);
        assert_eq!(parse_health_version(""), None);
    }

    #[test]
    fn parse_sha256_valid() {
        let line = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789  rs-delivery-0.22.7-linux-amd64";
        assert_eq!(
            parse_sha256_file(line).as_deref(),
            Some("abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
        );
    }

    #[test]
    fn parse_sha256_uppercase_is_lowercased() {
        // 64 uppercase hex chars (16 * "ABCD").
        let line = "ABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCDABCD  file";
        assert_eq!(line.split_whitespace().next().unwrap().len(), 64);
        assert_eq!(
            parse_sha256_file(line).as_deref(),
            Some("abcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd")
        );
    }

    #[test]
    fn parse_sha256_rejects_short() {
        assert_eq!(parse_sha256_file("deadbeef  file"), None);
    }

    #[test]
    fn parse_sha256_rejects_garbage() {
        assert_eq!(parse_sha256_file("not a hash"), None);
        assert_eq!(parse_sha256_file(""), None);
        // 64 chars but with a non-hex char.
        let bad = "g123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0  file";
        assert_eq!(parse_sha256_file(bad), None);
    }

    #[test]
    fn sha256_hex_known_vector() {
        // sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        // sha256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn ensured_audit_shape() {
        let row = ensured_audit(7, "0.22.7", "deadbeef");
        assert_eq!(row.severity, Severity::Info);
        assert_eq!(row.source, Source::Delivery);
        assert_eq!(row.action, Action::DeliveryBinaryEnsured);
        assert_eq!(row.event_id, Some(7));
        assert_eq!(row.detail["version"], "0.22.7");
        assert_eq!(row.detail["sha256"], "deadbeef");
    }

    #[test]
    fn pre_create_mismatch_audit_shape() {
        let row = pre_create_mismatch_audit(7, "0.22.7", "https://s3/x/rs-delivery-0.22.7", "boom");
        assert_eq!(row.severity, Severity::Critical);
        assert_eq!(row.action, Action::DeliveryBinaryVersionMismatch);
        assert_eq!(row.detail["vps_version"], serde_json::Value::Null);
        assert_eq!(row.detail["client_version"], "0.22.7");
        assert_eq!(row.detail["binary_url"], "https://s3/x/rs-delivery-0.22.7");
        assert_eq!(row.detail["phase"], "pre_create");
        assert_eq!(row.detail["error"], "boom");
    }

    #[test]
    fn post_boot_mismatch_audit_shape() {
        let row = post_boot_mismatch_audit(7, 20, Some("0.22.6"), "0.22.7");
        assert_eq!(row.severity, Severity::Critical);
        assert_eq!(row.action, Action::DeliveryBinaryVersionMismatch);
        assert_eq!(row.instance_id, Some(20));
        assert_eq!(row.detail["vps_version"], "0.22.6");
        assert_eq!(row.detail["client_version"], "0.22.7");
        assert_eq!(row.detail["phase"], "post_boot");
        // None vps_version must serialize to JSON null.
        let none_row = post_boot_mismatch_audit(7, 20, None, "0.22.7");
        assert_eq!(none_row.detail["vps_version"], serde_json::Value::Null);
    }
}
