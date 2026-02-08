pub mod manager_api;
pub mod s3;
pub mod uploader;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum EndpointError {
    #[error("S3 error: {0}")]
    S3(String),

    #[error("manager API error: {0}")]
    Manager(String),

    #[error("manager returned 403 (forbidden)")]
    ManagerForbidden,

    #[error("io error: {0}")]
    Io(String),
}
