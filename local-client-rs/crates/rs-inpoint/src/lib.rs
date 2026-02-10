pub mod chunker;
pub mod media_receiver;
pub mod muxer;
pub mod rtmp_server;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum InpointError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("muxer error: {0}")]
    Muxer(String),
}
