//! #368: the inpoint owns its ingest runtime from start to stop.

use super::*;

use std::time::Duration;

use crate::ingest_runtime::INGEST_THREAD_NAME;

#[test]
fn stop_shuts_the_ingest_runtime_down() {
    let main_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("main runtime");
    let (_restart_tx, restart_rx) = mpsc::channel(1);
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let (ws_tx, _) = broadcast::channel(16);
    let mut service = main_rt
        .block_on(async {
            InpointService::start(InpointParams {
                bind: "127.0.0.1".into(),
                port: 0,
                flv_chunk_sink: Arc::new(FlvChunkSink::new_null()),
                inpoint_state: InpointState::new(),
                ws_tx,
                restart_rx,
                shutdown_rx,
            })
        })
        .expect("the inpoint starts");
    assert!(service.is_ingest_running());
    let ingest = service.ingest_handle().expect("a started inpoint").clone();
    let thread = main_rt
        .block_on(ingest.spawn(async { std::thread::current().name().map(str::to_owned) }))
        .expect("the ingest runtime runs tasks");
    assert_eq!(thread.as_deref(), Some(INGEST_THREAD_NAME));

    shutdown_tx.send(()).expect("the supervision loop listens");
    main_rt
        .block_on(async { tokio::time::timeout(Duration::from_secs(10), service.stop()).await })
        .expect("the inpoint stops within 10 s")
        .expect("the supervision loop must not panic");
    assert!(
        !service.is_ingest_running(),
        "stop shuts the ingest runtime down and joins its thread"
    );
    assert!(service.ingest_handle().is_none());
    assert!(
        main_rt.block_on(ingest.spawn(async {})).is_err(),
        "a stopped inpoint's runtime runs no more tasks"
    );
}
