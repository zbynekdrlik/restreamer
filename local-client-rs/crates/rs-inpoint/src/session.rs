use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tracing::{debug, trace};

use crate::InpointError;
use crate::chunker::ChunkSink;
use crate::handshake;

/// Handles a single RTMP publisher session.
///
/// Performs the RTMP handshake, then reads RTMP chunks from the stream and
/// forwards raw media data to the chunk sink.
pub struct RtmpSession {
    stream: TcpStream,
    sink: Arc<ChunkSink>,
}

impl RtmpSession {
    pub fn new(stream: TcpStream, sink: Arc<ChunkSink>) -> Self {
        Self { stream, sink }
    }

    pub async fn run(&mut self) -> Result<(), InpointError> {
        // Perform RTMP handshake (C0+C1+C2 / S0+S1+S2)
        handshake::perform_handshake(&mut self.stream).await?;
        debug!("RTMP handshake completed");

        // Read RTMP chunks and forward media data to the sink
        let mut buf = vec![0u8; 4096];
        loop {
            let n = self.stream.read(&mut buf).await?;
            if n == 0 {
                debug!("RTMP connection closed by peer");
                break;
            }
            trace!("Received {n} bytes from RTMP client");
            self.sink.write_data(&buf[..n]).await;
        }

        Ok(())
    }
}
