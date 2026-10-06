//! #368: OBS dropped frames on Sunday 2026-10-04 (416) and Thursday
//! 2026-10-01 (517) because the RTMP ingest read the OBS socket on the SAME
//! tokio runtime as everything else. When that runtime stopped polling for
//! ~5-7 s (a blocking call, a starved worker), nobody read the socket and OBS
//! dropped its queued frames.
//!
//! These tests drive the inpoint exactly as the orchestrator starts it
//! (`InpointService::start`) with an in-process RTMP publisher
//! (`rs_rtmp_push::RtmpPusher`, paced in real time on its own thread), and
//! watch the chunker through an injected wall clock. The chunker reads that
//! clock at every chunk boundary, on the thread that processes the frames,
//! so the clock's call log shows WHEN frames were processed and on WHICH
//! thread.

use super::*;

use std::sync::Mutex;
use std::time::{Duration, Instant};

use rs_core::stable_since::StableSince;
use rs_inpoint::wall_clock::{SystemWallClock, WallClock};
use rs_rtmp_push::{PusherConfig, RtmpPusher};

/// The thread that must process every frame.
const INGEST_THREAD: &str = "restreamer-ingest";
/// Chunk length: with a keyframe every 33 ms, a chunk boundary (and so a
/// clock read) every ~66 ms while frames flow.
const CHUNK: Duration = Duration::from_millis(50);
/// Each starvation task blocks one main-runtime worker this long.
const STARVE_EACH: Duration = Duration::from_millis(1_500);
/// The longest gap in frame processing the test accepts. OBS drops frames
/// once its send queue holds ~700 ms.
const MAX_INGEST_GAP: Duration = Duration::from_millis(200);

/// One chunker clock read: when, and on which thread.
type ClockCall = (Instant, Option<String>);

/// A wall clock that records every read. With `fixed_ms` it always tells
/// that time, so chunk file names are known in advance
/// (`chunk_<ms>_<index:06>.bin`).
#[derive(Default)]
struct RecordingClock {
    calls: Mutex<Vec<ClockCall>>,
    fixed_ms: Option<i64>,
}

impl WallClock for RecordingClock {
    fn now_ms(&self) -> i64 {
        let thread = std::thread::current().name().map(str::to_owned);
        self.calls
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((Instant::now(), thread));
        self.fixed_ms.unwrap_or_else(|| SystemWallClock.now_ms())
    }
}

impl RecordingClock {
    fn calls(&self) -> Vec<ClockCall> {
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Bounded wait until the chunker has read the clock `n` times.
    fn wait_for_calls(&self, n: usize, what: &str) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while self.calls().len() < n {
            assert!(
                Instant::now() < deadline,
                "{what}: the chunker read the clock only {} times in 15 s",
                self.calls().len()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// Bounded wait for the first clock read after `from`.
    fn first_call_after(&self, from: Instant, what: &str) -> Instant {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some(at) = self
                .calls()
                .into_iter()
                .map(|(at, _)| at)
                .find(|at| *at > from)
            {
                return at;
            }
            assert!(
                Instant::now() < deadline,
                "{what}: the chunker processed no frame in 15 s"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    /// The longest stretch inside `[from, to]` with no clock read, counting
    /// the window's edges.
    fn max_gap(&self, from: Instant, to: Instant) -> Duration {
        let mut edges = vec![from];
        edges.extend(
            self.calls()
                .into_iter()
                .map(|(at, _)| at)
                .filter(|at| *at > from && *at < to),
        );
        edges.push(to);
        edges.sort();
        edges
            .windows(2)
            .map(|w| w[1].duration_since(w[0]))
            .max()
            .unwrap_or_default()
    }
}

/// A free loopback port. The inpoint binds by address (as in production), so
/// the port is released here and re-bound by the server.
fn free_port() -> u16 {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a probe port");
    probe.local_addr().expect("probe local_addr").port()
}

fn flv_tag(out: &mut Vec<u8>, tag_type: u8, ts: u32, body: &[u8]) {
    let size = body.len() as u32;
    out.push(tag_type);
    out.extend_from_slice(&size.to_be_bytes()[1..]);
    out.extend_from_slice(&(ts & 0x00FF_FFFF).to_be_bytes()[1..]);
    out.push((ts >> 24) as u8);
    out.extend_from_slice(&[0, 0, 0]);
    out.extend_from_slice(body);
    out.extend_from_slice(&(11 + size).to_be_bytes());
}

/// `len` of synthetic A/V as an FLV byte stream: a video keyframe every
/// 33 ms and an AAC frame every 23 ms, in content order.
fn synthetic_flv(len: Duration) -> Vec<u8> {
    let end = u32::try_from(len.as_millis()).expect("short stream");
    let mut tags: Vec<(u32, u8)> = (0..=end).step_by(33).map(|ts| (ts, 9)).collect();
    tags.extend((0..=end).step_by(23).map(|ts| (ts, 8)));
    tags.sort();
    let mut out = vec![b'F', b'L', b'V', 1, 0x05, 0, 0, 0, 9, 0, 0, 0, 0];
    for (ts, tag_type) in tags {
        let body: &[u8] = if tag_type == 9 {
            &[0x17, 0x01, 0, 0, 0, 0xAB, 0xCD]
        } else {
            &[0xAF, 0x01, 0x21, 0x10]
        };
        flv_tag(&mut out, tag_type, ts, body);
    }
    out
}

/// Publish `len` of synthetic A/V to the inpoint in real time, from a thread
/// and runtime of its own (so a starved main runtime cannot slow the
/// publisher, exactly like OBS). Retries the whole publish until one goes
/// through: the server may not be listening yet, or may be restarting.
fn spawn_publisher(port: u16, len: Duration) -> std::thread::JoinHandle<Result<(), String>> {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        let flv = synthetic_flv(len);
        let url = format!("rtmp://127.0.0.1:{port}/live/obs-368");
        let deadline = Instant::now() + Duration::from_secs(15);
        rt.block_on(async {
            loop {
                let mut pusher = RtmpPusher::new(url.clone(), PusherConfig { timeout_ms: 2_000 });
                let attempt = match pusher.push_flv_bytes(&[]).await {
                    Ok(()) => pusher.push_flv_bytes(&flv).await,
                    Err(e) => Err(e),
                };
                pusher.close().await;
                match attempt {
                    Ok(()) => return Ok(()),
                    Err(e) if Instant::now() >= deadline => {
                        return Err(format!("publisher gave up: {e:?}"));
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                }
            }
        })
    })
}

/// A started inpoint plus the channels the orchestrator would hold.
struct Harness {
    main_rt: tokio::runtime::Runtime,
    service: InpointService,
    clock: Arc<RecordingClock>,
    restart_tx: mpsc::Sender<()>,
    shutdown_tx: broadcast::Sender<()>,
    port: u16,
    #[cfg_attr(not(unix), allow(dead_code))]
    sink: Arc<FlvChunkSink>,
    #[cfg_attr(not(unix), allow(dead_code))]
    chunk_dir: tempfile::TempDir,
}

/// Start the inpoint the way the orchestrator does, inside a multi-thread
/// "main" runtime with two workers (the app runtime's shape).
fn start_inpoint() -> Harness {
    start_inpoint_with(RecordingClock::default())
}

/// `start_inpoint` with the chunker reading `clock`.
fn start_inpoint_with(clock: RecordingClock) -> Harness {
    start_inpoint_with_state(clock, InpointState::new())
}

/// `start_inpoint_with` sharing `inpoint_state` with the inpoint, wired the
/// way the orchestrator wires it.
fn start_inpoint_with_state(clock: RecordingClock, inpoint_state: InpointState) -> Harness {
    let main_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("main runtime");
    let chunk_dir = tempfile::tempdir().expect("chunk dir");
    let clock = Arc::new(clock);
    let flv_chunk_sink = Arc::new(
        FlvChunkSink::new(chunk_dir.path().to_path_buf(), CHUNK)
            .with_wall_clock(Arc::clone(&clock) as Arc<dyn WallClock>),
    );
    let port = free_port();
    let (restart_tx, restart_rx) = mpsc::channel(1);
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let (ws_tx, _) = broadcast::channel(16);
    let service = main_rt.block_on(async {
        InpointService::start(InpointParams {
            bind: "127.0.0.1".into(),
            port,
            flv_chunk_sink: Arc::clone(&flv_chunk_sink),
            inpoint_state,
            ws_tx,
            restart_rx,
            shutdown_rx,
        })
    });
    Harness {
        main_rt,
        service,
        clock,
        restart_tx,
        shutdown_tx,
        port,
        sink: flv_chunk_sink,
        chunk_dir,
    }
}

impl Harness {
    /// Shut the inpoint down as the orchestrator does and wait for it.
    fn stop(&mut self) {
        let _ = self.shutdown_tx.send(());
        let service = &mut self.service;
        self.main_rt
            .block_on(async { tokio::time::timeout(Duration::from_secs(10), service.stop()).await })
            .expect("the inpoint stops within 10 s")
            .expect("the inpoint supervision loop must not panic");
    }
}

fn assert_all_on_ingest_thread(calls: &[ClockCall], phase: &str) {
    assert!(
        !calls.is_empty(),
        "{phase}: the chunker processed no frames"
    );
    for (_, thread) in calls {
        assert_eq!(
            thread.as_deref(),
            Some(INGEST_THREAD),
            "{phase}: frames must be processed on the dedicated ingest thread, \
             never on a main-runtime thread"
        );
    }
}

/// Design test (i). While every worker of the main runtime is blocked (a
/// blocking call on an async worker, the Sunday 09:16 / 10:01 shape), the
/// inpoint keeps reading the publisher: no gap in frame processing reaches
/// `MAX_INGEST_GAP`.
#[test]
fn inpoint_keeps_reading_while_the_main_runtime_is_starved() {
    let mut h = start_inpoint();
    let publisher = spawn_publisher(h.port, Duration::from_secs(6));
    h.clock.wait_for_calls(6, "before the starvation");

    let starve_from = Instant::now();
    for _ in 0..4 {
        h.main_rt.spawn(async { std::thread::sleep(STARVE_EACH) });
    }
    std::thread::sleep(2 * STARVE_EACH + Duration::from_millis(200));
    let starve_to = Instant::now();
    let gap = h.clock.max_gap(starve_from, starve_to);

    let published = publisher.join().expect("publisher thread");
    h.stop();
    assert!(
        gap < MAX_INGEST_GAP,
        "the inpoint stopped processing frames for {gap:?} while the main runtime was \
         starved (limit {MAX_INGEST_GAP:?}): ingest must not share the app runtime (#368)"
    );
    published.expect("the publisher streamed through the starvation");
}

/// Design test (ii). The orchestrator's start, restart and stop paths keep
/// working on the dedicated runtime: frames before AND after an operator
/// restart are processed on the ingest thread, and a stop releases the RTMP
/// port.
#[test]
fn inpoint_restart_and_stop_run_on_the_dedicated_ingest_thread() {
    let mut h = start_inpoint();
    spawn_publisher(h.port, Duration::from_millis(1_500))
        .join()
        .expect("publisher thread")
        .expect("first publish");
    h.clock.wait_for_calls(3, "first session");
    let first = h.clock.calls();
    assert_all_on_ingest_thread(&first, "before the restart");

    h.restart_tx
        .blocking_send(())
        .expect("the supervision loop takes restart requests");
    // The loop has taken the request once the channel has room again; it
    // then stops the old server (its MediaReceiver goes with it) within
    // milliseconds. Frames processed after `restarted` can only have come
    // through the NEW server.
    let deadline = Instant::now() + Duration::from_secs(10);
    while h.restart_tx.capacity() < h.restart_tx.max_capacity() {
        assert!(
            Instant::now() < deadline,
            "the supervision loop never took the restart request"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let restarted = Instant::now() + Duration::from_millis(300);
    let mut after = Vec::new();
    // A publisher that reached the old server just before it stopped
    // publishes into nothing: publish again until the new server has it.
    for _ in 0..3 {
        spawn_publisher(h.port, Duration::from_millis(1_500))
            .join()
            .expect("publisher thread")
            .expect("publish after the restart");
        after = h
            .clock
            .calls()
            .into_iter()
            .filter(|(at, _)| *at > restarted)
            .collect();
        if after.len() >= 3 {
            break;
        }
    }
    assert!(
        after.len() >= 3,
        "the restarted server processed no frames ({} clock reads)",
        after.len()
    );
    assert_all_on_ingest_thread(&after, "after the restart");

    h.stop();
    assert!(
        std::net::TcpStream::connect(("127.0.0.1", h.port)).is_err(),
        "a stopped inpoint must not accept RTMP connections"
    );
}

/// #368 review finding: shutting the ingest runtime down cancels its tasks.
/// A chunk still being written when the inpoint stops must still be written
/// AND reported: the report is what the chunk forwarder turns into the DB
/// row the uploader needs. On the app runtime those writes finished during
/// the orchestrator's shutdown drain.
///
/// The first chunk's file is a FIFO, so its write blocks until the test opens
/// the read end, 500 ms after `stop()` began.
#[cfg(unix)]
#[test]
fn stop_still_reports_a_chunk_that_is_being_written() {
    let mut h = start_inpoint_with(RecordingClock {
        fixed_ms: Some(1_000),
        ..Default::default()
    });
    let fifo = h.chunk_dir.path().join("chunk_1000_000000.bin");
    let made = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("run mkfifo");
    assert!(made.success(), "mkfifo {fifo:?}");
    let mut reports = h.sink.subscribe();
    spawn_publisher(h.port, Duration::from_millis(1_500))
        .join()
        .expect("publisher thread")
        .expect("publish");
    h.clock.wait_for_calls(6, "chunks were cut");
    // Opening the FIFO blocks until a writer opens it: fail here, rather
    // than hang below, if chunk 0 is not being written to it.
    assert_eq!(
        h.main_rt
            .block_on(h.sink.wait_for_writes(Duration::from_millis(100))),
        1,
        "chunk 0's write must be blocked on the FIFO"
    );

    let reader = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(500));
        std::fs::read(&fifo)
    });
    h.stop();
    let written = reader
        .join()
        .expect("reader thread")
        .expect("read the FIFO");
    assert!(!written.is_empty(), "the first chunk was written");

    let mut reported = Vec::new();
    while let Ok(chunk) = reports.try_recv() {
        reported.push(chunk.index);
    }
    assert!(
        reported.contains(&0),
        "the chunk still being written at stop must be reported, got chunk indexes {reported:?}"
    );
}

/// How long the API-side task holds the publisher-stable cell in
/// `ingest_never_waits_for_an_api_task_holding_the_stable_since_cell`.
const API_HOLD: Duration = Duration::from_secs(3);
/// The longest a connecting publisher may wait for its first processed
/// frame. Far below `API_HOLD`, so a session start that waited for the API
/// task fails by seconds; far above a loopback RTMP handshake on a loaded
/// runner.
const MAX_SESSION_START: Duration = Duration::from_secs(1);

/// The API side of the publisher-stable cell, as a task on the main runtime:
/// it takes what a `/status`, `POST /delivery/start` or tray handler takes
/// from the cell and then stays in the handler for `hold`, across an await,
/// as when its runtime stalls mid-handler. Since #368 that is a copied value
/// and nothing stays held; with the tokio `Mutex` it was the lock guard.
/// Returns once the task has taken it.
fn api_task_holds(main_rt: &tokio::runtime::Runtime, cell: &StableSince, hold: Duration) {
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let cell = cell.clone();
    main_rt.spawn(async move {
        let _seen = cell.stable_secs();
        let _ = held_tx.send(());
        tokio::time::sleep(hold).await;
    });
    held_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the API task takes the cell");
}

/// #368 (ROZHODNUTÉ issuecomment-6013075985 item 2). The publisher-stable
/// cell (`rtmp_stable_since`) is shared by the ingest thread, which sets it
/// when OBS connects and clears it when OBS leaves, and the API handlers on
/// the main runtime (`/status`, `POST /delivery/start`, the tray). The ingest
/// path must never wait for the API side: while an API task holds the cell,
/// a publisher that connects must have its frames processed at once.
#[test]
fn ingest_never_waits_for_an_api_task_holding_the_stable_since_cell() {
    let stable_since = StableSince::new();
    let mut h = start_inpoint_with_state(
        RecordingClock::default(),
        InpointState::new().with_stable_since(stable_since.clone()),
    );
    // A first session proves the server is up and warm, so the measured
    // session pays only its own handshake. Then let its end settle, so no
    // read of its last chunk can land in the measured window.
    spawn_publisher(h.port, Duration::from_millis(500))
        .join()
        .expect("publisher thread")
        .expect("warm-up publish");
    h.clock.wait_for_calls(3, "the warm-up session");
    std::thread::sleep(Duration::from_millis(500));

    api_task_holds(&h.main_rt, &stable_since, API_HOLD);
    let connect = Instant::now();
    // The session outlasts the hold, so a start that waited for the API task
    // shows up as a measured wait, not only as a lost session.
    let publisher = spawn_publisher(h.port, API_HOLD + Duration::from_secs(2));
    let first_frame = h
        .clock
        .first_call_after(connect, "the session during the API hold");
    let waited = first_frame.duration_since(connect);

    let published = publisher.join().expect("publisher thread");
    h.stop();
    assert!(
        waited < MAX_SESSION_START,
        "a publisher that connected while an API task held the publisher-stable cell \
         waited {waited:?} for its first processed frame (limit {MAX_SESSION_START:?}): \
         the ingest path must never wait for a lock the API side can hold (#368)"
    );
    published.expect("the publisher streamed its session");
}
