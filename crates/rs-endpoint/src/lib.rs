pub mod disk_pressure;
pub mod metrics;
pub mod s3;
pub mod uploader;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum EndpointError {
    #[error("S3 error: {0}")]
    S3(String),

    #[error("io error: {0}")]
    Io(String),
}
