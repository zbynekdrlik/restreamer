pub mod disk_pressure;
pub mod metrics;
pub mod s3;
pub mod uploader;

/// Test-only helpers. Compiled only under `cfg(test)` or the `testing`
/// feature; NEVER part of a release binary. Lets integration tests drive
/// the real uploader path against a mock S3 with an accelerated retry clock.
#[cfg(any(test, feature = "testing"))]
pub mod testing {
    use sqlx::SqlitePool;

    /// Drive the real upload worker loop against a mock S3 endpoint until no
    /// uploadable chunk remains (or a 30s safety deadline). Uses the SAME
    /// `upload_one` / `should_abandon_upload` path as production, so the
    /// never-drop decision is exercised end-to-end, not bypassed.
    pub async fn run_uploader_until_idle(
        pool: &SqlitePool,
        s3_endpoint: &str,
        bucket: &str,
    ) -> anyhow::Result<()> {
        crate::uploader::testing_support::drive_until_idle(pool, s3_endpoint, bucket).await
    }
}

use thiserror::Error;

#[derive(Debug, Error)]
pub enum EndpointError {
    #[error("S3 error: {0}")]
    S3(String),

    #[error("io error: {0}")]
    Io(String),
}
