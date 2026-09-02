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
