use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

use crate::InpointError;

/// RTMP handshake constants.
const RTMP_VERSION: u8 = 3;
const HANDSHAKE_SIZE: usize = 1536;

/// Perform the RTMP handshake with a connecting client.
///
/// The handshake sequence:
/// 1. Client sends C0 (1 byte: version) + C1 (1536 bytes: time + zero + random)
/// 2. Server sends S0 (1 byte: version) + S1 (1536 bytes: time + zero + random)
///    + S2 (1536 bytes: echo of C1)
/// 3. Client sends C2 (1536 bytes: echo of S1)
pub async fn perform_handshake(stream: &mut TcpStream) -> Result<(), InpointError> {
    // Read C0 + C1
    let c0 = read_u8(stream).await?;
    if c0 != RTMP_VERSION {
        return Err(InpointError::Handshake(format!(
            "unsupported RTMP version: {c0}"
        )));
    }
    debug!("Received C0: version {c0}");

    let mut c1 = vec![0u8; HANDSHAKE_SIZE];
    stream.read_exact(&mut c1).await?;
    debug!("Received C1: {HANDSHAKE_SIZE} bytes");

    // Build S0 + S1 + S2
    let s0 = RTMP_VERSION;
    let mut s1 = vec![0u8; HANDSHAKE_SIZE];
    // S1: time (4 bytes) + zero (4 bytes) + random data
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u32;
    s1[..4].copy_from_slice(&time.to_be_bytes());
    // Fill rest with pseudo-random data
    for (i, byte) in s1[8..].iter_mut().enumerate() {
        *byte = (i % 256) as u8;
    }

    // S2 is echo of C1
    let s2 = c1.clone();

    // Send S0 + S1 + S2
    stream.write_all(&[s0]).await?;
    stream.write_all(&s1).await?;
    stream.write_all(&s2).await?;
    stream.flush().await?;
    debug!("Sent S0+S1+S2");

    // Read C2
    let mut c2 = vec![0u8; HANDSHAKE_SIZE];
    stream.read_exact(&mut c2).await?;
    debug!("Received C2: handshake complete");

    Ok(())
}

async fn read_u8(stream: &mut TcpStream) -> Result<u8, InpointError> {
    let mut buf = [0u8; 1];
    stream.read_exact(&mut buf).await?;
    Ok(buf[0])
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn handshake_succeeds() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            perform_handshake(&mut stream).await
        });

        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();

            // Send C0 + C1
            stream.write_all(&[RTMP_VERSION]).await.unwrap();
            let c1 = vec![0u8; HANDSHAKE_SIZE];
            stream.write_all(&c1).await.unwrap();
            stream.flush().await.unwrap();

            // Read S0 + S1 + S2
            let mut s0 = [0u8; 1];
            stream.read_exact(&mut s0).await.unwrap();
            assert_eq!(s0[0], RTMP_VERSION);

            let mut s1 = vec![0u8; HANDSHAKE_SIZE];
            stream.read_exact(&mut s1).await.unwrap();

            let mut s2 = vec![0u8; HANDSHAKE_SIZE];
            stream.read_exact(&mut s2).await.unwrap();

            // S2 should be echo of C1
            assert_eq!(s2, c1);

            // Send C2 (echo of S1)
            stream.write_all(&s1).await.unwrap();
            stream.flush().await.unwrap();
        });

        let (server_result, client_result) = tokio::join!(server, client);
        server_result.unwrap().unwrap();
        client_result.unwrap();
    }

    #[tokio::test]
    async fn handshake_rejects_bad_version() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            perform_handshake(&mut stream).await
        });

        let client = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            // Send bad version
            stream.write_all(&[99]).await.unwrap();
            stream.flush().await.unwrap();
        });

        let (server_result, _) = tokio::join!(server, client);
        let err = server_result.unwrap().unwrap_err();
        assert!(err.to_string().contains("unsupported RTMP version"));
    }
}
