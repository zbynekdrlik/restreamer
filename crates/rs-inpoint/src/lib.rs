pub mod flv_chunker;
mod frame_stats;
mod ingest_report;
pub mod ingest_skew;
pub mod media_receiver;
pub mod rtmp_server;
mod src_track;
pub mod wall_clock;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum InpointError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),
}
