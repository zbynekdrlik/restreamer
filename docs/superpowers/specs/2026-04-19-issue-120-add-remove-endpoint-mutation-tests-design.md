# Issue #120: Unit-test `add_endpoint_to_delivery` & `remove_endpoint_from_delivery` — Design

**Goal:** Kill the 4 surviving mutants on `add_endpoint_to_delivery` and `remove_endpoint_from_delivery` in `crates/rs-api/src/delivery_endpoints.rs`, then remove the two `--exclude-re` lines from `.github/workflows/ci.yml` (lines 231-232).

## Background

PR #105 added these guard clauses:

```rust
if !is_delivery_active(&instance.status) {
    return Err(anyhow::anyhow!(
        "Delivery instance is in state '{}', not in an active delivery state",
        instance.status
    ));
}
```

Mutation testing found 4 surviving mutants across both functions:
- Delete `!` → guard inverts (Err only on active states).
- Replace function body with `Ok(())` → no-op.

The functions were excluded from mutation testing to unblock PR #105; #120 is the follow-up test-debt ticket.

## Approach

Both mutant classes are killed by a single test per function that asserts the specific guard-clause error message.

**Test 1 (mutant: `Ok(())` body replacement):** Call the function with `instance.status = "creating"`. Assert `Err(..)` whose message contains `"not in an active delivery state"`.
- Original: guard fires → returns that exact Err. ✓
- `Ok(())` mutant: returns Ok → test expects Err → fail. ✓ mutant killed.
- `!` deleted mutant: guard doesn't fire on "creating" → proceeds to HTTP call → `instance.ipv4 = "unreachable.invalid"` → DNS fails fast → different Err message → `.contains("active delivery state")` fails. ✓ mutant killed.

No mock HTTP server needed. Asserting on the error *message* rather than just `is_err()` kills both classes with one test.

## Test File Layout

Integration tests at `crates/rs-api/tests/delivery_endpoints_tests.rs` (new file, ~90 lines).

```rust
use rs_api::delivery::DeliveryOrchestrator;
use rs_api::delivery_endpoints::{
    add_endpoint_to_delivery, remove_endpoint_from_delivery, StartPosition,
};
use rs_core::config::Config;
use rs_core::db;
use sqlx::SqlitePool;

/// Build an in-memory DB + orchestrator + config and seed:
/// - one endpoint_configs row
/// - one delivery_instances row with the given `status`
///   (ipv4 = "unreachable.invalid" so mutated code fails fast on HTTP)
/// Returns (orch, pool, config, event_id, endpoint_id).
async fn setup_with_status(
    status: &str,
) -> (DeliveryOrchestrator, SqlitePool, Config, i64, i64) {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();

    let endpoint_id: i64 = sqlx::query_scalar(
        "INSERT INTO endpoint_configs (alias, service_type, stream_key)
         VALUES ('yt', 'YT_HLS', 'k') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let event_id: i64 = 42; // event row not required for guard path

    let instance_id = db::create_delivery_instance(
        &pool,
        /* hetzner_id */ 1,
        /* name */ "test-instance",
        /* ipv4 */ "unreachable.invalid",
        /* server_type */ "cx22",
        Some(event_id),
        /* auth_token */ "test-token",
    )
    .await
    .unwrap();

    db::update_delivery_instance_status(&pool, instance_id, status)
        .await
        .unwrap();

    let mut config = Config::for_testing();
    config.hetzner.api_token = "test-token".to_string();
    let orch = DeliveryOrchestrator::new(pool.clone(), config.clone()).unwrap();

    (orch, pool, config, event_id, endpoint_id)
}

#[tokio::test]
async fn add_endpoint_to_delivery_rejects_inactive_delivery() {
    let (orch, pool, config, event_id, endpoint_id) =
        setup_with_status("creating").await;
    let err = add_endpoint_to_delivery(
        &orch, &pool, &config, event_id, endpoint_id, StartPosition::Live,
    )
    .await
    .expect_err("creating state must be rejected");
    assert!(
        err.to_string().contains("not in an active delivery state"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn remove_endpoint_from_delivery_rejects_inactive_delivery() {
    let (orch, pool, _config, event_id, _endpoint_id) =
        setup_with_status("creating").await;
    let err = remove_endpoint_from_delivery(&orch, &pool, event_id, "yt")
        .await
        .expect_err("creating state must be rejected");
    assert!(
        err.to_string().contains("not in an active delivery state"),
        "unexpected error: {err}"
    );
}
```

## CI Changes

Remove lines 231-232 from `.github/workflows/ci.yml`:

```diff
-            --exclude-re 'add_endpoint_to_delivery' \
-            --exclude-re 'remove_endpoint_from_delivery' \
```

## Version Bump

`dev` is 0.3.64 (== `main` after the v0.3.64 release). First commit on this feature bumps to **0.3.65** in four files:

- `Cargo.toml` (workspace version, line 24)
- `src-tauri/Cargo.toml`
- `src-tauri/tauri.conf.json`
- `leptos-ui/Cargo.toml`

## Acceptance

- [ ] `crates/rs-api/tests/delivery_endpoints_tests.rs` added with 2 tests
- [ ] Two `--exclude-re` lines removed from `.github/workflows/ci.yml`
- [ ] CI green: test-integrity ✓, mutation-testing ✓ (the two functions now pass without exclusion)
- [ ] Version bumped to 0.3.65 in all 4 files
- [ ] PR from `dev` to `main`, mergeable, clean

## Non-Goals

- **No happy-path test** (active status + mock HTTP 200). The guard-clause Err test alone kills both mutant classes. Adding a happy path would require either spawning an axum mock on hardcoded port 8000 (flaky) or refactoring the function to accept a base URL (out of scope). The existing `is_delivery_active` unit tests (`delivery_tests.rs:9-25`) already cover the state-matrix.
- **No parameterized tests** over all inactive states. `is_delivery_active` is unit-tested per state; the integration test only needs to prove the guard path is wired up correctly.
- **No refactor of the production code** — leave `format!("http://{}:8000", instance.ipv4)` as-is.

## Risks

- **Hostname "unreachable.invalid"**: relies on DNS returning NXDOMAIN fast. The `.invalid` TLD is reserved by RFC 2606 exactly for this purpose — safe. Reqwest surfaces this as a connection error within ms, no 10s timeout hit.
- **Port 8000 collision on dev laptop**: if a developer runs `cargo mutants` locally and has something on port 8000 returning 200, the `!`-deleted mutant path could succeed. The test would still see the original code fire the guard → Err → pass. The mutant only matters during mutation testing, which runs in CI sandbox with no bound 8000. Acceptable.
