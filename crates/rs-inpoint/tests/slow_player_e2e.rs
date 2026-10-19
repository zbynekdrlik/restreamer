//! #368: a local RTMP player that falls behind is disconnected, while the
//! publisher keeps going.
//!
//! On stream.lan the operator's VLC, playing `rtmp://127.0.0.1:1234/live`,
//! read slower than real time. xiu's unbounded per-subscriber queue grew to
//! 8 GB, and freeing it stalled the ingest for 7 s, so OBS dropped frames.
//! Here a real ffmpeg publishes at a constant bitrate. An in-process player
//! runs handshake, connect, createStream and play, and then either reads
//! far slower than the stream or not at all.
//!
//! Requires: ffmpeg with libx264 and aac encoder support.
#![allow(
    clippy::disallowed_methods,
    reason = "test code: #[tokio::test] expands to Runtime::block_on and the player socket is cloned with std; never on the ingest runtime (#368 clippy.toml)"
)]

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytesio::bytesio::{TNetIO, TcpIO};
use rs_core::models::InpointState;
use rs_inpoint::flv_chunker::FlvChunkSink;
use rs_inpoint::rtmp_server::RtmpServer;
use rtmp::handshake::define::{ClientHandshakeState, RTMP_HANDSHAKE_SIZE};
use rtmp::handshake::handshake_client::SimpleHandshakeClient;
use rtmp::netconnection::writer::{ConnectProperties, NetConnection};
use rtmp::netstream::writer::NetStreamWriter;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;

/// Everything the server logs, captured once for the whole test binary. The
/// server's verdict on a player is its `RTMP session ended` line, read
/// without touching the player's socket: reading it would let a stalled
/// write finish.
fn server_log() -> Arc<std::sync::Mutex<String>> {
    static LOG: OnceLock<Arc<std::sync::Mutex<String>>> = OnceLock::new();
    LOG.get_or_init(|| {
        let log = Arc::new(std::sync::Mutex::new(String::new()));
        let sink = Arc::clone(&log);
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(move || LogSink(Arc::clone(&sink)))
            .init();
        log
    })
    .clone()
}

struct LogSink(Arc<std::sync::Mutex<String>>);

impl std::io::Write for LogSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap()
            .push_str(&String::from_utf8_lossy(bytes));
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Wait until the server logs a session end that contains `reason`.
async fn session_ended_with(reason: &str, within: Duration) -> bool {
    let log = server_log();
    let deadline = tokio::time::Instant::now() + within;
    while tokio::time::Instant::now() < deadline {
        let ended = log
            .lock()
            .unwrap()
            .lines()
            .any(|l| l.contains("RTMP session ended") && l.contains(reason));
        if ended {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Kills the publisher however the test ends (`Child` does not on drop).
struct Publisher(std::process::Child);

impl Publisher {
    fn alive(&mut self) -> bool {
        self.0.try_wait().unwrap().is_none()
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A constant ~16 Mbit/s 720p30 stream (x264 CBR with filler), so a player
/// that does not keep up falls behind within seconds.
fn spawn_cbr_publisher(port: u16, duration_secs: u32) -> Publisher {
    let child = std::process::Command::new("ffmpeg")
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
        .expect("ffmpeg is required for this test but could not be spawned");
    Publisher(child)
}

struct Rig {
    port: u16,
    shutdown: tokio::sync::broadcast::Sender<()>,
    server: tokio::task::JoinHandle<Result<(), rs_inpoint::InpointError>>,
    publisher: Publisher,
}

impl Rig {
    /// A server (with `limit` frames per player, or the production default)
    /// and a publisher that is already connected.
    async fn start(limit: Option<usize>, publish_secs: u32) -> Self {
        server_log();
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = std_listener.local_addr().unwrap().port();
        std_listener.set_nonblocking(true).unwrap();
        let listener = tokio::net::TcpListener::from_std(std_listener).unwrap();
        let state = InpointState::new();
        let mut server = RtmpServer::new("127.0.0.1", port);
        if let Some(limit) = limit {
            server = server.with_play_queue_limit(limit);
        }
        let shutdown = server.shutdown_handle();
        let server_state = state.clone();
        let server = tokio::spawn(async move {
            server
                .run_on_listener(listener, Arc::new(FlvChunkSink::new_null()), server_state)
                .await
        });
        let publisher = spawn_cbr_publisher(port, publish_secs);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        while !state.is_connected() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the publisher never connected"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Self {
            port,
            shutdown,
            server,
            publisher,
        }
    }

    async fn stop(self) {
        let _ = self.shutdown.send(());
        let _ = tokio::time::timeout(Duration::from_secs(5), self.server).await;
    }
}

/// Connect a player with a tiny receive buffer, run the RTMP client side up
/// to `play`, and return a second handle to its socket. Nothing reads from
/// it until the test does.
async fn start_player(port: u16) -> tokio::net::TcpStream {
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

/// Read until EOF, `chunk` bytes at a time with `pause` between reads.
async fn read_to_eof(player: &mut tokio::net::TcpStream, chunk: usize, pause: Duration) {
    let mut buf = vec![0u8; chunk];
    loop {
        match player.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => tokio::time::sleep(pause).await,
        }
    }
}

/// A player reading ~1 MB/s of a ~2 MB/s stream falls behind on every
/// frame, yet every write to it still completes (a ~1 MB keyframe in ~1 s,
/// far below xiu's 10 s write stall). Once more than `limit` frames wait
/// for it, it is cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_player_that_reads_too_slowly_is_cut_by_the_queue_limit() {
    let mut rig = Rig::start(Some(50), 45).await;
    let mut player = start_player(rig.port).await;
    let reader = tokio::spawn(async move {
        read_to_eof(&mut player, 32 * 1024, Duration::from_millis(30)).await;
    });

    let cut = session_ended_with("player fell behind", Duration::from_secs(25)).await;
    reader.abort();
    let publisher_alive = rig.publisher.alive();
    rig.stop().await;

    assert!(
        cut,
        "a player more than 50 frames behind must be disconnected"
    );
    assert!(publisher_alive, "the publisher must keep going");
}

/// A player that stops reading entirely blocks the server's write to it, so
/// the queue check never runs again while the queue grows. With the
/// production limit, the stalled write itself must end the session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_player_that_stops_reading_is_cut_with_the_production_limit() {
    let mut rig = Rig::start(None, 45).await;
    let mut player = start_player(rig.port).await;

    // The socket buffers fill within a few seconds, then the write stalls.
    let cut = session_ended_with("player stopped reading", Duration::from_secs(30)).await;
    // Once ended, the connection closes: what is buffered, then EOF.
    let closed = cut
        && tokio::time::timeout(
            Duration::from_secs(15),
            read_to_eof(&mut player, 64 * 1024, Duration::ZERO),
        )
        .await
        .is_ok();
    let publisher_alive = rig.publisher.alive();
    rig.stop().await;

    assert!(cut, "a player that stopped reading must be disconnected");
    assert!(closed, "the disconnected player's socket must close");
    assert!(publisher_alive, "the publisher must keep going");
}
