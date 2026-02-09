# Testing Strategy for local-client-rs

## Current Test Coverage

### rs-core (18 tests)

- ✅ Database CRUD operations for all tables
- ✅ Schema migrations and versioning
- ✅ Model serialization/deserialization
- ✅ Configuration loading and validation
- ✅ Environment variable overrides

### rs-endpoint (4 tests)

- ✅ ManagerClient URL building
- ✅ S3Client key generation
- ✅ Data structure serialization

### rs-api (18 tests)

- ✅ HTTP endpoint routing
- ✅ WebSocket connections
- ✅ Status endpoints

### rs-inpoint (14 tests)

- ✅ RTMP server setup
- ✅ Media chunking logic
- ✅ File handling

## Integration Tests Needed (Future Work)

Due to axum 0.8's strict Handler trait requirements, the following integration tests require additional investigation:

### rs-endpoint / manager_api.rs

**Needed tests:**

- `get_active_stream` with 200/403/404 responses
- `notify_chunk_uploaded` with success and failure
- `check_chunk` with verified=true and verified=false

**Challenge:** axum 0.8 Handler trait requires concrete types, not `impl Future` returns. Simple async fn test handlers don't satisfy the trait bounds.

**Solutions to investigate:**

1. Use `wiremock` or `mockito` crates for HTTP mocking instead of axum
2. Use `tower::Service` directly for testing
3. Explore axum testing utilities (if available)
4. Use concrete struct implementations of Handler trait

### rs-endpoint / uploader.rs

**Needed tests:**

- Unsent chunks get picked up and marked as in_process
- Retry logic on S3 failure (3 retries with exponential backoff)
- Rollback on manager notification failure
- Verified=false triggers rollback
- Shutdown signal respected

**Challenge:** Requires mocking both S3 and HTTP manager endpoints. S3 mocking is particularly complex as rust-s3 doesn't have built-in test utilities.

**Solutions to investigate:**

1. Use MinIO or LocalStack for local S3-compatible testing
2. Create S3Client trait abstraction for easier mocking
3. Test retry and rollback logic in isolation with mock implementations

### rs-service / poller.rs

**Needed tests:**

- 200 response creates/updates streaming event
- 404 response deletes local streaming event
- 403 response disables delivering flag
- Errors broadcast as WsEvent::Error
- Poll interval timing

**Challenge:** Same axum 0.8 Handler trait issues + needs to test WebSocket broadcasts.

**Solutions to investigate:**

1. HTTP mocking as above
2. Test WsEvent broadcasts with real broadcast channels (no mocking needed here)
3. Test interval timing with configurable Duration

## Testing Best Practices

### Current Approach

- Real SQLite `:memory:` databases (no mocking)
- Actual async runtime (tokio::test)
- Integration tests test full code paths
- Foreign key constraints enabled in tests

### Why No Heavy Mocking?

Following CLAUDE.md instructions:

> Always write real end-to-end (E2E) tests — not mocked, not hidden, not stubbed. Tests must exercise the actual code paths.

This means:

- ✅ Use real SQLite databases
- ✅ Use real HTTP clients (when testing client logic)
- ✅ Test actual async concurrency
- ❌ Don't mock database layer
- ❌ Don't mock core business logic
- ⚠️ Only mock external services (manager API, S3)

## Running Tests

```bash
# All library tests
cargo test --workspace --lib

# Specific crate
cargo test --package rs-core

# Specific test
cargo test --package rs-core --lib client_profile_crud

# With output
cargo test -- --nocapture

# Check coverage (requires cargo-tarpaulin)
cargo tarpaulin --workspace --lib
```

## Code Coverage Goals

Per CLAUDE.md:

> 60% minimum test coverage target

Current estimated coverage:

- rs-core: ~90% (excellent database test coverage)
- rs-endpoint: ~30% (needs integration tests)
- rs-api: ~70% (good HTTP endpoint coverage)
- rs-inpoint: ~50% (basic chunking tests)
- rs-service: ~20% (minimal, needs poller/uploader tests)

**Priority:** Add integration tests for rs-endpoint and rs-service to reach 60% workspace coverage.

## Test File Organization

### Library Crates (rs-core, rs-endpoint, etc.)

Tests can be:

1. Inline with `#[cfg(test)] mod tests` at bottom of file
2. In separate `tests/` directory for integration tests

### Binary Crates (rs-service)

Tests MUST be:

1. Inline with `#[cfg(test)] mod tests` (no separate test files for binaries)
2. Test functions and types that are `pub` or `pub(crate)`

## Known Issues

### Axum 0.8 Handler Trait Complexity

The Handler trait in axum 0.8 requires:

```rust
Handler<T, S>
```

where the function signature must produce a concrete type, not `impl Future`.

This fails:

```rust
async fn handler() -> Json<MyResponse> { ... }  // Returns impl Future
```

This theoretically works but requires complex trait bounds:

```rust
fn handler() -> impl IntoResponse + Send { ... }
```

Recommendation: Use dedicated HTTP mocking libraries like wiremock that don't require implementing axum's Handler trait.

## Next Steps

1. **Immediate:** Document test gaps (this file)
2. **Short-term:** Add wiremock dependency and retry integration tests
3. **Medium-term:** Add S3 mocking with MinIO/LocalStack
4. **Long-term:** Achieve 60%+ code coverage across workspace

## References

- [Axum Testing Guide](https://docs.rs/axum/latest/axum/test_helpers/index.html)
- [Tower Service Testing](https://docs.rs/tower/latest/tower/trait.Service.html)
- [Wiremock for Rust](https://docs.rs/wiremock/latest/wiremock/)
- [SQLx Testing](https://docs.rs/sqlx/latest/sqlx/testing/index.html)
