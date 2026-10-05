pub mod flv_chunker;
pub mod ingest_skew;
pub mod media_receiver;
pub mod rtmp_server;
pub mod wall_clock;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum InpointError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),
}
