//! #368: a local RTMP player that stops reading is disconnected once it
//! falls behind, while the publisher keeps going.
//!
//! On stream.lan the operator's VLC, playing `rtmp://127.0.0.1:1234/live`,
//! read slower than real time. xiu's unbounded per-subscriber queue grew to
//! 8 GB, and freeing it stalled the ingest for 7 s, so OBS dropped frames.
//! Here a real ffmpeg publishes at a constant bitrate, and an in-process
//! player runs handshake, connect, createStream and play, and then never
//! reads. The server must end that play session (and close its socket) on
//! its own.
//!
//! Requires: ffmpeg with libx264 and aac encoder support.
#![allow(
    clippy::disallowed_methods,
    reason = "test code: #[tokio::test] expands to Runtime::block_on and the player socket is cloned with std; never on the ingest runtime (#368 clippy.toml)"
)]

use std::sync::Arc;
use std::time::Duration;

use bytesio::bytesio::{TNetIO, TcpIO};
use rs_inpoint::flv_chunker::FlvChunkSink;
use rs_inpoint::rtmp_server::RtmpServer;
use rtmp::handshake::define::{ClientHandshakeState, RTMP_HANDSHAKE_SIZE};
use rtmp::handshake::handshake_client::SimpleHandshakeClient;
use rtmp::netconnection::writer::{ConnectProperties, NetConnection};
use rtmp::netstream::writer::NetStreamWriter;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;

/// Frames a player may have queued in this test (production: 1000).
const TEST_LIMIT: usize = 50;

/// A constant ~16 Mbit/s 720p30 stream (x264 CBR with filler), so the
/// player's socket buffers fill within a few seconds and the queue grows.
fn spawn_cbr_publisher(port: u16, duration_secs: u32) -> std::process::Child {
    std::process::Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-re",
            "-f",
            "lavfi",
            "-i",
            &format!("testsrc=size=1280x720:rate=30:duration={duration_secs}"),
            "-f",
            "lavfi",
            "-i",
            &format!("sine=frequency=1000:sample_rate=44100:duration={duration_secs}"),
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-g",
            "30",
            "-bf",
            "0",
            "-b:v",
            "16M",
            "-minrate",
            "16M",
            "-maxrate",
            "16M",
            "-bufsize",
            "4M",
            "-x264-params",
            "nal-hrd=cbr",
            "-c:a",
            "aac",
            "-b:a",
            "64k",
            "-f",
            "flv",
            &format!("rtmp://127.0.0.1:{port}/live/test"),
        ])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("ffmpeg is required for this test but could not be spawned")
}

/// Connect a player with a tiny receive buffer, run the RTMP client side up
/// to `play`, and return a second handle to its socket. Nothing reads from
/// it until the test does.
async fn start_stalled_player(port: u16) -> tokio::net::TcpStream {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let stream = socket
        .connect(format!("127.0.0.1:{port}").parse().unwrap())
        .await
        .unwrap();
    let std_stream = stream.into_std().unwrap();
    let reader = std_stream.try_clone().unwrap();
    let stream = tokio::net::TcpStream::from_std(std_stream).unwrap();
    let io: Arc<Mutex<Box<dyn TNetIO + Send + Sync>>> =
        Arc::new(Mutex::new(Box::new(TcpIO::new(stream))));

    let mut handshaker = SimpleHandshakeClient::new(Arc::clone(&io));
    loop {
        handshaker.handshake().await.unwrap();
        if handshaker.state == ClientHandshakeState::Finish {
            break;
        }
        let mut read = 0;
        while read < RTMP_HANDSHAKE_SIZE * 2 + 1 {
            let data = io.lock().await.read().await.unwrap();
            read += data.len();
            handshaker.extend_data(&data[..]);
        }
    }

    let props = ConnectProperties {
        app: Some("live".into()),
        tc_url: Some(format!("rtmp://127.0.0.1:{port}/live")),
        ..ConnectProperties::default()
    };
    NetConnection::new(Arc::clone(&io))
        .write_connect(&1.0, &props)
        .await
        .unwrap();
    NetConnection::new(Arc::clone(&io))
        .write_create_stream(&2.0)
        .await
        .unwrap();
    NetStreamWriter::new(Arc::clone(&io))
        .write_play(&3.0, &"test".to_string(), &-2.0, &-1.0, &false)
        .await
        .unwrap();

    // `io` closes its descriptor when it drops here; the connection stays
    // open through the cloned one.
    drop(io);
    reader.set_nonblocking(true).unwrap();
    tokio::net::TcpStream::from_std(reader).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_player_that_stops_reading_is_disconnected_while_the_publisher_goes_on() {
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = std_listener.local_addr().unwrap().port();
    std_listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();
    let inpoint_state = rs_core::models::InpointState::new();
    let server = RtmpServer::new("127.0.0.1", port).with_play_queue_limit(TEST_LIMIT);
    let shutdown = server.shutdown_handle();
    let state = inpoint_state.clone();
    let server_task = tokio::spawn(async move {
        server
            .run_on_listener(listener, Arc::new(FlvChunkSink::new_null()), state)
            .await
    });

    let mut publisher = spawn_cbr_publisher(port, 40);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while !inpoint_state.is_connected() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the publisher never connected"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut player = start_stalled_player(port).await;
    // ~2 MB/s fills the socket buffers within a few seconds; after that
    // every frame queues for the player.
    tokio::time::sleep(Duration::from_secs(10)).await;

    // A disconnected player reads what is still buffered, then EOF. A
    // player still subscribed keeps getting the live stream and never
    // sees EOF before the publisher ends (40 s).
    let mut buf = vec![0u8; 64 * 1024];
    let drained = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match player.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;
    let publisher_alive = publisher.try_wait().unwrap().is_none();
    let _ = publisher.kill();
    let _ = publisher.wait();
    let _ = shutdown.send(());
    let _ = tokio::time::timeout(Duration::from_secs(5), server_task).await;

    assert!(
        drained.is_ok(),
        "the server must close a player with more than {TEST_LIMIT} frames queued"
    );
    assert!(
        publisher_alive,
        "the publisher must keep going while the slow player is cut"
    );
}
