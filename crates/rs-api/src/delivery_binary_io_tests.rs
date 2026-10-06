//! The I/O half of the delivery-binary lockstep (#246): `ensure_bucket_binary`
//! and its upload paths against a mocked client bucket and a mocked GitHub
//! release (wiremock), plus the bundle lookup next to an executable. A child
//! module of `delivery_binary`, so it reaches the private helpers (#367).

use super::*;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const VERSION: &str = "9.9.9";
const RELEASE_BYTES: &[u8] = b"release-binary-bytes";
const BUNDLED_BYTES: &[u8] = b"bundled-binary-bytes";

fn config_for(s3: &MockServer) -> rs_core::config::Config {
    let mut cfg = rs_core::config::Config::for_testing();
    cfg.s3.endpoint = s3.uri();
    cfg.s3.bucket = "client-bucket".to_string();
    cfg
}

/// The client bucket: HEAD of the versioned key answers `head_status`, and
/// exactly `uploads` PUTs of it are expected (checked when the server drops).
async fn bucket(head_status: u16, uploads: u64) -> MockServer {
    let server = MockServer::start().await;
    let key = format!("/client-bucket/rs-delivery-{VERSION}");
    Mock::given(method("HEAD"))
        .and(path(key.clone()))
        .respond_with(ResponseTemplate::new(head_status))
        .mount(&server)
        .await;
    Mock::given(method("PUT"))
        .and(path(key))
        .respond_with(ResponseTemplate::new(200))
        .expect(uploads)
        .mount(&server)
        .await;
    server
}

/// The GitHub release: the binary and its `.sha256` sidecar. Points
/// the test release-base override (`TEST_RELEASE_BASE_ENV`) at it.
async fn release() -> MockServer {
    let server = MockServer::start().await;
    let asset = format!("/restreamer-v{VERSION}/rs-delivery-{VERSION}-linux-amd64");
    Mock::given(method("GET"))
        .and(path(asset.clone()))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(RELEASE_BYTES))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("{asset}.sha256")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{}  rs-delivery\n", sha256_hex(RELEASE_BYTES))),
        )
        .mount(&server)
        .await;
    unsafe {
        std::env::set_var(
            TEST_RELEASE_BASE_ENV,
            format!("{}/restreamer-v", server.uri()),
        )
    };
    server
}

/// A bundled binary in `dir` with a `.sha256` sidecar claiming `sidecar_sha`.
fn bundled(dir: &std::path::Path, sidecar_sha: &str) -> std::path::PathBuf {
    let p = dir.join("rs-delivery-linux");
    std::fs::write(&p, BUNDLED_BYTES).unwrap();
    std::fs::write(
        dir.join("rs-delivery-linux.sha256"),
        format!("{sidecar_sha}  rs-delivery-linux\n"),
    )
    .unwrap();
    p
}

#[tokio::test]
async fn a_present_versioned_key_needs_no_upload() {
    let _env = RELEASE_ENV_LOCK.lock().await;
    let s3 = bucket(200, 0).await;
    let got = ensure_bucket_binary(&config_for(&s3), VERSION)
        .await
        .unwrap();
    assert_eq!(got, None, "the bucket already holds rs-delivery-{VERSION}");
}

/// A missing key with no bundle next to the running exe (a test binary has
/// none) is filled from the sha-verified GitHub release.
#[tokio::test]
async fn a_missing_key_is_filled_from_the_verified_release() {
    let _env = RELEASE_ENV_LOCK.lock().await;
    let s3 = bucket(404, 1).await;
    let _gh = release().await;
    let got = ensure_bucket_binary(&config_for(&s3), VERSION).await;
    unsafe { std::env::remove_var(TEST_RELEASE_BASE_ENV) };
    assert_eq!(got.unwrap(), Some(sha256_hex(RELEASE_BYTES)));
}

/// A release whose `.sha256` does not match its bytes is never uploaded.
#[tokio::test]
async fn a_release_with_a_wrong_sidecar_is_refused() {
    let _env = RELEASE_ENV_LOCK.lock().await;
    let s3 = bucket(404, 0).await;
    let gh = MockServer::start().await;
    let asset = format!("/restreamer-v{VERSION}/rs-delivery-{VERSION}-linux-amd64");
    Mock::given(method("GET"))
        .and(path(asset.clone()))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(RELEASE_BYTES))
        .mount(&gh)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("{asset}.sha256")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("{}  rs-delivery\n", sha256_hex(b"other bytes"))),
        )
        .mount(&gh)
        .await;
    unsafe { std::env::set_var(TEST_RELEASE_BASE_ENV, format!("{}/restreamer-v", gh.uri())) };
    let got = upload_release_binary(&config_for(&s3), VERSION).await;
    unsafe { std::env::remove_var(TEST_RELEASE_BASE_ENV) };
    let err = got.expect_err("a sha mismatch must not upload");
    assert!(err.to_string().contains("sha256 mismatch"), "{err}");
}

/// The #246 zero-GitHub path: a bundled binary whose sidecar matches is
/// uploaded, and its digest returned. A wrong sidecar is refused.
#[tokio::test]
async fn a_bundled_binary_is_uploaded_only_when_its_sidecar_matches() {
    let dir = tempfile::tempdir().unwrap();
    let s3 = bucket(404, 1).await;
    let good = bundled(dir.path(), &sha256_hex(BUNDLED_BYTES));
    let got = upload_bundled_binary(&config_for(&s3), VERSION, &good).await;
    assert_eq!(got.unwrap(), sha256_hex(BUNDLED_BYTES));
    drop(s3);

    let s3 = bucket(404, 0).await;
    let bad = bundled(dir.path(), &sha256_hex(b"other bytes"));
    let err = upload_bundled_binary(&config_for(&s3), VERSION, &bad)
        .await
        .expect_err("a sidecar that does not match must refuse the upload");
    assert!(err.to_string().contains("sha256 mismatch"), "{err}");
}

/// `upload_binary_bytes` PUTs the bytes and returns their own digest.
#[tokio::test]
async fn upload_binary_bytes_returns_the_digest_of_what_it_put() {
    let s3 = bucket(404, 1).await;
    let got = upload_binary_bytes(&config_for(&s3), VERSION, b"abc").await;
    assert_eq!(got.unwrap(), sha256_hex(b"abc"));
}

/// The bundle is looked up next to the executable, then in `resources/`.
#[test]
fn the_bundle_is_found_next_to_the_exe_then_in_resources() {
    let dir = tempfile::tempdir().unwrap();
    let exe = dir.path().join("Restreamer.exe");
    assert_eq!(find_bundled_binary(&exe), None, "no bundle in a dev build");

    std::fs::create_dir(dir.path().join("resources")).unwrap();
    let in_resources = dir.path().join("resources").join(BUNDLED_BINARY_NAME);
    std::fs::write(&in_resources, BUNDLED_BYTES).unwrap();
    assert_eq!(find_bundled_binary(&exe), Some(in_resources));

    let next_to_exe = dir.path().join(BUNDLED_BINARY_NAME);
    std::fs::write(&next_to_exe, BUNDLED_BYTES).unwrap();
    assert_eq!(
        find_bundled_binary(&exe),
        Some(next_to_exe),
        "the NSIS placement next to the exe wins"
    );
}
