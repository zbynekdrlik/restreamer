//! Shared test mutex for tests that mutate the process-global
//! `YOUTUBE_API_BASE` environment variable. Cargo runs tests in
//! parallel; without a single shared mutex, two test files can race
//! and one ends up hitting the real Google API (HTTP 401) instead of
//! the other test's wiremock.
//!
//! Every test in this crate that touches `YOUTUBE_API_BASE` must lock
//! [`env_guard`] before set_var and hold the guard until after
//! remove_var.

use std::sync::OnceLock;

pub fn env_guard() -> &'static tokio::sync::Mutex<()> {
    static M: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    M.get_or_init(|| tokio::sync::Mutex::new(()))
}
