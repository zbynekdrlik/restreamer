//! Shared serialization gate for tests that mutate the process-global
//! `YOUTUBE_API_BASE` env var. Without it, parallel `#[tokio::test]`
//! instances across modules race on reqwest reads of the env var, so one
//! test's reqwest call would hit another test's wiremock URL.

use std::sync::OnceLock;

pub fn env_guard() -> &'static tokio::sync::Mutex<()> {
    static M: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    M.get_or_init(|| tokio::sync::Mutex::new(()))
}
