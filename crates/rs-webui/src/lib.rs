//! Embedded WASM dashboard frontend (#248 / #107).
//!
//! The dashboard used to be served from an on-disk `www/` directory placed
//! next to the binary. Nothing in the NSIS installer/upgrade path ever shipped
//! or refreshed it, so a fresh install 404'd (#107) and partial upgrades left
//! `www/` drifting from the exe (#248). This crate embeds the built frontend
//! (`trunk build --release` output at repo-root `dist/`) INTO the binary via
//! `rust_embed`, so one artifact carries the whole client and the drift class
//! dies by construction.
//!
//! The serving/version logic is written GENERIC over the `rust_embed::RustEmbed`
//! trait so it is unit-testable against a small committed fixture asset set
//! (`tests/fixtures/`) with no trunk build — a plain workspace `cargo test`
//! exercises every path.

use axum::body::Body;
use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

/// The real frontend assets, embedded from the trunk output at repo-root
/// `dist/`. Empty in a non-trunk workspace build (see `build.rs`).
#[derive(RustEmbed)]
// Relative to CARGO_MANIFEST_DIR (crates/rs-webui) → repo-root `dist/`. A bare
// relative path avoids rust-embed's `interpolate-folder-path` feature.
#[folder = "../../dist"]
struct FrontendAssets;

/// SPA entry point served for `/` and every client-side route.
const INDEX: &str = "index.html";

/// File emitted alongside the frontend build carrying its `BUILD_VERSION`
/// (written by CI/release right after `trunk build`). Used for the startup
/// drift self-check.
const VERSION_FILE: &str = "version.txt";

/// Axum fallback handler: serve the embedded frontend for any route not
/// matched by an API route.
pub async fn serve_embedded(uri: Uri) -> Response {
    serve::<FrontendAssets>(uri.path())
}

/// Generic core of [`serve_embedded`], testable against any embedded asset set.
///
/// Resolves `path` to an embedded file; unknown paths fall back to the SPA
/// entry (`index.html`) so client-side routing (`/settings`, …) works. Each
/// response carries a cache-control header appropriate to the asset.
pub fn serve<A: RustEmbed>(path: &str) -> Response {
    let trimmed = path.trim_start_matches('/');
    let lookup = if trimmed.is_empty() { INDEX } else { trimmed };

    if let Some(content) = A::get(lookup) {
        return asset_response(lookup, content.data.into_owned());
    }
    // SPA fallback — an unknown, non-asset route is a client-side route.
    match A::get(INDEX) {
        Some(content) => asset_response(INDEX, content.data.into_owned()),
        None => (StatusCode::NOT_FOUND, "frontend not embedded").into_response(),
    }
}

fn asset_response(path: &str, data: Vec<u8>) -> Response {
    let mime = mime_guess::from_path(path).first_or_octet_stream();
    let mut resp = Response::new(Body::from(data));
    let headers = resp.headers_mut();
    if let Ok(ct) = HeaderValue::from_str(mime.as_ref()) {
        headers.insert(header::CONTENT_TYPE, ct);
    }
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(cache_control_for(path)),
    );
    resp
}

/// Cache-control policy: content-hashed assets are immutable and cached for a
/// year; everything else (`index.html`, `version.txt`, `manifest.json`, the
/// service worker, icons) is `no-cache` so a new build is picked up on the
/// next request instead of being heuristic-cached for days (#248).
pub fn cache_control_for(path: &str) -> &'static str {
    if is_hashed_asset(path) {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    }
}

/// A trunk content-hashed asset — e.g. `leptos-ui-<hex>.js`,
/// `leptos-ui-<hex>_bg.wasm`, `style-<hex>.css`. Detected as a `.js`/`.wasm`/
/// `.css` file whose name contains a hex segment of at least 16 chars.
fn is_hashed_asset(path: &str) -> bool {
    let file = path.rsplit('/').next().unwrap_or(path);
    let stem = match file.rsplit_once('.') {
        Some((stem, "js" | "wasm" | "css")) => stem,
        _ => return false,
    };
    stem.split(['-', '_', '.'])
        .any(|seg| seg.len() >= 16 && seg.chars().all(|c| c.is_ascii_hexdigit()))
}

/// The version reported by the embedded frontend build (`version.txt`), or
/// `None` when no frontend is embedded (a dev / non-trunk build).
pub fn frontend_version() -> Option<String> {
    version_of::<FrontendAssets>()
}

fn version_of<A: RustEmbed>() -> Option<String> {
    let file = A::get(VERSION_FILE)?;
    let v = String::from_utf8_lossy(&file.data).trim().to_string();
    if v.is_empty() { None } else { Some(v) }
}

/// Startup drift self-check (#248 belt-and-braces). Compares the embedded
/// frontend version against the binary's own version. `None` embedded version
/// (dev/unbuilt) is never a drift. Returns `Some((frontend, binary))` on a
/// genuine mismatch so the caller can emit a `Critical` audit row.
pub fn compare_versions(embedded: Option<&str>, binary: &str) -> Option<(String, String)> {
    match embedded {
        Some(fe) if fe != binary => Some((fe.to_string(), binary.to_string())),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
