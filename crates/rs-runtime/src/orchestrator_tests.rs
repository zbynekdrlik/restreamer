use super::*;
use rs_core::db;

/// Test: ServiceCore should accept an externally provided pool via with_pool()
/// This prevents duplicate pool creation in GUI mode.
#[tokio::test]
async fn service_core_with_pool_stores_provided_pool() {
    // Arrange: Create a pool externally (simulating GUI mode)
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();

    // Create test config
    let config = Config::for_testing();
    let config_path = PathBuf::from("/tmp/test-config.json");
    let log_buffer = LogBuffer::new(10);

    // Act: Create ServiceCore with externally provided pool
    let core = ServiceCore::new(config, config_path, log_buffer).with_pool(pool.clone());

    // Assert: ServiceCore should have the provided pool stored
    assert!(
        core.provided_pool.is_some(),
        "ServiceCore should store the provided pool"
    );
}

/// Test: When pool is provided, the provided pool should contain our test data
/// This verifies we're using the SAME pool, not creating a new one.
#[tokio::test]
async fn service_core_with_pool_uses_same_pool_instance() {
    // Arrange: Create a pool and insert test data
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();

    // Insert a test client profile to verify we're using THIS pool
    db::upsert_client_profile(&pool, "test-client-uuid")
        .await
        .unwrap();

    let config = Config::for_testing();
    let config_path = PathBuf::from("/tmp/test-config.json");
    let log_buffer = LogBuffer::new(10);

    // Act: Create ServiceCore with the pool containing test data
    let core = ServiceCore::new(config, config_path, log_buffer).with_pool(pool.clone());

    // Assert: The pool should be the same one we provided (has our test data)
    let provided_pool = core.provided_pool.as_ref().unwrap();
    let profile = db::get_client_profile(provided_pool).await.unwrap();
    assert!(profile.is_some(), "Should find test data in provided pool");
    assert_eq!(profile.unwrap().user_uuid, "test-client-uuid");
}

async fn eventually(what: &str, mut ok: impl AsyncFnMut() -> bool) {
    for _ in 0..500 {
        if ok().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("timed out after 10 s waiting until {what}");
}

/// #106 end to end. While another socket holds the RTMP port, the inpoint
/// loop records the bind error and keeps waiting: a conflict never ends it.
/// A restart request re-probes at once, and once the port is free the RTMP
/// server really listens on it. Shutdown then ends the loop.
#[tokio::test]
async fn inpoint_loop_waits_out_a_port_conflict_then_serves_rtmp() {
    let hog = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = hog.local_addr().unwrap().port();
    let inpoint_state = InpointState::new();
    let (ws_tx, _ws_rx) = broadcast::channel(16);
    let (restart_tx, restart_rx) = mpsc::channel(4);
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let task = tokio::spawn(run_inpoint_loop(
        "127.0.0.1".into(),
        port,
        Arc::new(FlvChunkSink::new_null()),
        inpoint_state.clone(),
        ws_tx,
        restart_rx,
        shutdown_rx,
        tokio::runtime::Handle::current(),
    ));

    eventually("the bind conflict is recorded", async || {
        inpoint_state.bind_error().is_some()
    })
    .await;
    assert!(!task.is_finished(), "a port conflict never ends the loop");

    drop(hog);
    restart_tx.send(()).await.unwrap();
    eventually("the RTMP server listens on the freed port", async || {
        tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
    })
    .await;
    assert_eq!(inpoint_state.bind_error(), None, "the recovery clears it");

    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("shutdown ends the inpoint loop")
        .expect("the inpoint loop must not panic");
}
