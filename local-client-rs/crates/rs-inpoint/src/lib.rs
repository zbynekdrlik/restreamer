pub mod chunker;
pub mod handshake;
pub mod muxer;
pub mod rtmp_server;
pub mod session;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum InpointError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("handshake error: {0}")]
    Handshake(String),

    #[error("protocol error: {0}")]
    Protocol(String),
}
