use super::*;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MAX: Duration = Duration::from_secs(15);

#[test]
fn drain_step_waits_only_while_bytes_are_queued_and_the_bound_holds() {
    let ms = Duration::from_millis;
    assert_eq!(
        drain_step(Some(0), false, ms(0), MAX),
        Some(DrainOutcome::Drained)
    );
    assert_eq!(drain_step(Some(1), false, ms(0), MAX), None);
    assert_eq!(drain_step(Some(1), false, MAX - ms(1), MAX), None);
    assert_eq!(
        drain_step(Some(1), false, MAX, MAX),
        Some(DrainOutcome::TimedOut)
    );
    assert_eq!(
        drain_step(None, false, ms(0), MAX),
        Some(DrainOutcome::Unmeasured)
    );
    assert_eq!(
        drain_step(Some(1), true, ms(0), MAX),
        Some(DrainOutcome::SessionDead)
    );
    assert_eq!(
        drain_step(Some(0), true, ms(0), MAX),
        Some(DrainOutcome::SessionDead)
    );
}

/// The AMF0 command bodies, byte for byte (string marker 0x02 + u16 length,
/// number marker 0x00 + f64 BE, null 0x05), as ffmpeg's rtmpproto writes them.
#[test]
fn unpublish_command_bodies() {
    let mut fc = vec![0x02, 0, 11];
    fc.extend_from_slice(b"FCUnpublish");
    fc.push(0x00);
    fc.extend_from_slice(&4.0f64.to_be_bytes());
    fc.push(0x05);
    fc.extend_from_slice(&[0x02, 0, 3]);
    fc.extend_from_slice(b"key");
    assert_eq!(fcunpublish_body(4.0, "key"), fc);

    let mut del = vec![0x02, 0, 12];
    del.extend_from_slice(b"deleteStream");
    del.push(0x00);
    del.extend_from_slice(&5.0f64.to_be_bytes());
    del.push(0x05);
    del.push(0x00);
    del.extend_from_slice(&1.0f64.to_be_bytes());
    assert_eq!(delete_stream_body(5.0, 1), del);
}

/// On Linux the queue is readable: bytes the peer has not read pile up once
/// its receive window is full, and the count falls to 0 once it reads them.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn send_queue_counts_unacknowledged_bytes() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut client = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut server, _) = listener.accept().await.unwrap();
    let queue = SendQueue::of(&client);
    assert_eq!(queue.unacked_bytes(), Some(0));

    // Fill the peer's receive window and our send buffer without blocking.
    let block = vec![7u8; 64 * 1024];
    let mut written = 0usize;
    loop {
        match tokio::time::timeout(Duration::from_millis(200), client.write_all(&block)).await {
            Ok(Ok(())) => written += block.len(),
            Ok(Err(e)) => panic!("write failed: {e}"),
            Err(_) => break,
        }
    }
    let queued = queue.unacked_bytes().expect("readable on Linux");
    assert!(queued > 0, "the peer read nothing, so bytes must be queued");

    // Let the write that timed out finish so every byte the peer reads is
    // accounted for, then drain the peer.
    let reader = tokio::spawn(async move {
        let mut buf = vec![0u8; 256 * 1024];
        let mut total = 0usize;
        loop {
            match server.read(&mut buf).await {
                Ok(0) | Err(_) => return total,
                Ok(n) => total += n,
            }
        }
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while queue.unacked_bytes() != Some(0) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the queue never drained: {:?}",
            queue.unacked_bytes()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(client);
    let total = reader.await.unwrap();
    assert!(total >= written, "read {total} of at least {written}");
}
