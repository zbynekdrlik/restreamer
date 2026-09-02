//! Ensure the frontend embed-source directory exists at compile time.
//!
//! `rust_embed`'s derive macro errors with "folder does not exist" if the
//! embed folder is absent. The real frontend assets are produced by
//! `trunk build --release` into the repo-root `dist/` BEFORE `cargo tauri
//! build` compiles this crate (see `.github/workflows/release.yml` +
//! `ci.yml`). But an ordinary workspace `cargo test` / `cargo clippy` (dev1
//! is Tier-0; dev2 runs the suite) never runs trunk, so `dist/` is absent
//! there. Create it empty so the crate compiles with a zero-file asset set
//! instead of failing the build. An empty embed makes `frontend_version()`
//! return `None` (treated as "dev / not built" — no drift audit) and the
//! embedded fallback return 404, which the on-disk `www_dir` override and the
//! fixture-backed unit tests cover.

use std::path::Path;

fn main() {
    // Same location as the `#[folder = "../../dist"]` in `src/lib.rs`
    // (rust-embed resolves it relative to CARGO_MANIFEST_DIR):
    // crates/rs-webui -> repo root -> dist.
    let manifest = env!("CARGO_MANIFEST_DIR");
    let dist = Path::new(manifest).join("..").join("..").join("dist");
    if !dist.is_dir() {
        // Best-effort: if creation fails, the derive will surface the real
        // error, which is strictly more informative than a panic here.
        let _ = std::fs::create_dir_all(&dist);
    }
    // Re-run if the dist directory's presence changes.
    println!("cargo:rerun-if-changed=../../dist");
}
