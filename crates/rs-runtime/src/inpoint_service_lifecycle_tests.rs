//! #368: the inpoint owns its ingest runtime from start to stop.

use super::*;

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::ingest_runtime::INGEST_THREAD_NAME;

fn main_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("main runtime")
}

/// The params for an inpoint on `port` with a null chunker, plus its
/// shutdown and restart senders. Keep the restart sender alive: the loop
/// ends when that channel closes.
fn params(port: u16) -> (InpointParams, broadcast::Sender<()>, mpsc::Sender<()>) {
    let (restart_tx, restart_rx) = mpsc::channel(1);
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let (ws_tx, _) = broadcast::channel(16);
    let p = InpointParams {
        bind: "127.0.0.1".into(),
        port,
        flv_chunk_sink: Arc::new(FlvChunkSink::new_null()),
        inpoint_state: InpointState::new(),
        ws_tx,
        restart_rx,
        shutdown_rx,
    };
    (p, shutdown_tx, restart_tx)
}

fn stop(main_rt: &tokio::runtime::Runtime, service: &mut InpointService) {
    main_rt
        .block_on(async { tokio::time::timeout(Duration::from_secs(15), service.stop()).await })
        .expect("the inpoint stops within 15 s")
        .expect("the supervision loop must not panic");
}

/// `stop` shuts the ingest runtime down AND waits for its thread: a
/// blocking task still running on it is finished by the time `stop`
/// returns, and the runtime takes no more work.
#[test]
fn stop_shuts_the_ingest_runtime_down() {
    let main_rt = main_runtime();
    let (p, shutdown_tx, _restart_tx) = params(0);
    let mut service = main_rt.block_on(async { InpointService::start(p) });
    assert!(service.is_ingest_running());
    let ingest = service.ingest_handle().expect("a started inpoint").clone();
    let thread = main_rt
        .block_on(ingest.spawn(async { std::thread::current().name().map(str::to_owned) }))
        .expect("the ingest runtime runs tasks");
    assert_eq!(thread.as_deref(), Some(INGEST_THREAD_NAME));
    let finished = Arc::new(AtomicBool::new(false));
    {
        let finished = Arc::clone(&finished);
        ingest.spawn_blocking(move || {
            std::thread::sleep(Duration::from_millis(300));
            finished.store(true, Ordering::SeqCst);
        });
    }

    shutdown_tx.send(()).expect("the supervision loop listens");
    stop(&main_rt, &mut service);
    assert!(
        finished.load(Ordering::SeqCst),
        "stop waited for the ingest runtime's thread, not just dropped it"
    );
    assert!(!service.is_ingest_running());
    assert!(service.ingest_handle().is_none());
    assert!(
        main_rt.block_on(ingest.spawn(async {})).is_err(),
        "a stopped inpoint's runtime runs no more tasks"
    );
}

/// No ingest runtime (its thread could not be created): the inpoint still
/// serves RTMP, on the current runtime, and stops cleanly. A dead service is
/// worse than a degraded one (#106).
#[test]
fn without_an_ingest_runtime_the_inpoint_still_serves() {
    let main_rt = main_runtime();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .expect("a free port")
        .port();
    let (p, shutdown_tx, _restart_tx) = params(port);
    let mut service = main_rt.block_on(async {
        InpointService::start_on(p, Err(std::io::Error::other("no threads left")))
    });
    assert!(service.ingest_handle().is_none());
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(
            Instant::now() < deadline,
            "the RTMP server never listened on {port}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    shutdown_tx.send(()).expect("the supervision loop listens");
    stop(&main_rt, &mut service);
}
