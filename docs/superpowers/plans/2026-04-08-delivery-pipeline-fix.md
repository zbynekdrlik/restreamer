# Delivery Pipeline Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix YouTube streaming degradation caused by S3 migration — eliminate LIST per chunk, add pre-fetch buffer, harden CI gates.

**Architecture:** Three changes: (1) S3 key format `{evt}/{seq}.bin` with duration in S3 object metadata, eliminating LIST; (2) producer-consumer pipeline with 10-chunk pre-fetch buffer decoupling S3 from ffmpeg; (3) CI hardening with second health gate, bitrateLow check, and OBS scene selection.

**Tech Stack:** Rust, rust-s3 0.35.1, tokio mpsc channels, PowerShell (CI), OBS WebSocket

**Spec:** `docs/superpowers/specs/2026-04-08-delivery-pipeline-fix-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `Cargo.toml` | Modify | Version bump (workspace) |
| `src-tauri/Cargo.toml` | Modify | Version bump |
| `src-tauri/tauri.conf.json` | Modify | Version bump |
| `leptos-ui/Cargo.toml` | Modify | Version bump |
| `crates/rs-endpoint/src/s3.rs` | Modify | New key format, upload with metadata |
| `crates/rs-endpoint/src/uploader.rs` | Modify | Update `chunk_key()` call site |
| `crates/rs-endpoint/tests/uploader_integration.rs` | Modify | Update key format assertions |
| `crates/rs-delivery/src/s3_fetch.rs` | Modify | Direct GET, HEAD for duration, no LIST |
| `crates/rs-delivery/src/db.rs` | Modify | Remove `parse_chunk_key()` (dead code) |
| `crates/rs-delivery/src/endpoint_task.rs` | Modify | Producer-consumer pipeline refactor |
| `crates/rs-delivery/src/endpoint_task_tests.rs` | Modify | Update tests for new pipeline |
| `.github/workflows/ci.yml` | Modify | Second health gate, bitrateLow, OBS scene |

---

### Task 0: Version Bump

**Files:**
- Modify: `Cargo.toml:24`
- Modify: `src-tauri/Cargo.toml:3`
- Modify: `src-tauri/tauri.conf.json:4`
- Modify: `leptos-ui/Cargo.toml:3`

- [ ] **Step 1: Bump version 0.3.25 → 0.3.26 in all four files**

`Cargo.toml` line 24: `version = "0.3.25"` → `version = "0.3.26"`
`src-tauri/Cargo.toml` line 3: `version = "0.3.25"` → `version = "0.3.26"`
`src-tauri/tauri.conf.json` line 4: `"version": "0.3.25"` → `"version": "0.3.26"`
`leptos-ui/Cargo.toml` line 3: `version = "0.3.25"` → `version = "0.3.26"`

- [ ] **Step 2: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.26"
```

---

### Task 1: S3 Key Format Change — Upload Side (#99a)

**Files:**
- Modify: `crates/rs-endpoint/src/s3.rs:73-78`
- Modify: `crates/rs-endpoint/src/uploader.rs:141`
- Test: `crates/rs-endpoint/src/s3.rs` (inline tests)
- Test: `crates/rs-endpoint/tests/uploader_integration.rs`

- [ ] **Step 1: Write the failing test — new key format**

In `crates/rs-endpoint/src/s3.rs`, replace the existing test:

```rust
#[test]
fn chunk_key_format() {
    let key = S3Client::chunk_key("evt-123", 1);
    assert_eq!(key, "evt-123/1.bin");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p rs-endpoint chunk_key_format`
Expected: FAIL — `chunk_key` still takes 3 parameters.

- [ ] **Step 3: Update `chunk_key()` — remove duration_ms from signature and key**

In `crates/rs-endpoint/src/s3.rs`, change:

```rust
/// Generate an S3 key for a chunk file.
/// Format: `{event_id}/{sequence_number}.bin`
/// Duration is stored as S3 object metadata, not in the key.
pub fn chunk_key(event_identifier: &str, sequence_number: i64) -> String {
    format!("{event_identifier}/{sequence_number}.bin")
}
```

- [ ] **Step 4: Write the failing test — upload with metadata**

Add to the test module in `crates/rs-endpoint/src/s3.rs`:

```rust
#[test]
fn upload_chunk_key_is_simple() {
    // Verify chunk key doesn't contain duration or event suffix
    let key = S3Client::chunk_key("sunday-service-2026", 42);
    assert_eq!(key, "sunday-service-2026/42.bin");
    // Verify key is always constructable from just event_id + seq
    assert!(key.ends_with(".bin"));
    assert!(!key.contains("_")); // No underscore segments
}
```

- [ ] **Step 5: Run test to verify it passes (both tests)**

Run: `cargo test -p rs-endpoint chunk_key`
Expected: PASS — both `chunk_key_format` and `upload_chunk_key_is_simple` pass.

- [ ] **Step 6: Add `upload_chunk()` method with S3 metadata**

In `crates/rs-endpoint/src/s3.rs`, add a new method to `S3Client`:

```rust
/// Upload a chunk file to S3 with duration metadata.
/// Sets `x-amz-meta-duration-ms` header so the VPS can read duration
/// from GET/HEAD responses without needing S3 LIST.
pub async fn upload_chunk(
    &self,
    local_path: &Path,
    event_id: &str,
    seq: i64,
    duration_ms: i64,
) -> Result<(), EndpointError> {
    let key = Self::chunk_key(event_id, seq);
    // Clone bucket to set per-request metadata without leaking to other calls
    let mut upload_bucket = (*self.bucket).clone();
    upload_bucket.add_header("x-amz-meta-duration-ms", &duration_ms.to_string());

    let mut file = tokio::fs::File::open(local_path)
        .await
        .map_err(|e| EndpointError::Io(e.to_string()))?;

    let metadata = file
        .metadata()
        .await
        .map_err(|e| EndpointError::Io(e.to_string()))?;
    let file_size = metadata.len();

    tracing::debug!(
        "Uploading to s3://{}/{} ({file_size} bytes, duration={duration_ms}ms)",
        upload_bucket.name, key,
    );

    let response = upload_bucket
        .put_object_stream(&mut file, &key)
        .await
        .map_err(|e| EndpointError::S3(format!("upload failed: {e}")))?;

    if response.status_code() >= 300 {
        return Err(EndpointError::S3(format!(
            "upload returned status {}",
            response.status_code(),
        )));
    }

    tracing::info!("Uploaded {key} ({file_size} bytes, duration={duration_ms}ms)");
    Ok(())
}
```

- [ ] **Step 7: Update the uploader call site**

In `crates/rs-endpoint/src/uploader.rs`, around line 141, change:

```rust
// Old:
let s3_key = S3Client::chunk_key(&event_id, chunk.sequence_number, chunk.duration_ms);
// ...
.upload_file(Path::new(&chunk.chunk_file_path), &s3_key)

// New:
match s3
    .upload_chunk(
        Path::new(&chunk.chunk_file_path),
        &event_id,
        chunk.sequence_number,
        chunk.duration_ms,
    )
    .await
```

The full upload retry block should become:

```rust
let mut uploaded = false;
for attempt in 0..MAX_RETRIES {
    match s3
        .upload_chunk(
            Path::new(&chunk.chunk_file_path),
            &event_id,
            chunk.sequence_number,
            chunk.duration_ms,
        )
        .await
    {
        Ok(()) => {
            uploaded = true;
            break;
        }
        Err(e) => {
            if attempt + 1 < MAX_RETRIES {
                let delay = RETRY_BASE_DELAY
                    .saturating_mul(1 << attempt.min(5))
                    .min(RETRY_MAX_DELAY);
                warn!(
                    "S3 upload failed for chunk {} (attempt {}/{}): {e}, retrying in {:.0}s",
                    chunk.id, attempt + 1, MAX_RETRIES, delay.as_secs_f64()
                );
                tokio::time::sleep(delay).await;
            } else {
```

Note: The rest of the error handling block after the `else` is unchanged.

- [ ] **Step 8: Update integration test key assertion**

In `crates/rs-endpoint/tests/uploader_integration.rs`, the test asserts the key format. Find any assertion that checks the old key format like `"evt-123/1_2100_evt-123.bin"` and update it to `"evt-123/1.bin"`. Also update the comment at lines 5-7:

```rust
//! - S3 upload with correct key format (`{event_id}/{sequence_number}.bin`)
```

The mock S3 server captures `last_key` — check if any test asserts on that value and update the expected format. The mock server's route `/{bucket}/{*key}` handles both formats.

- [ ] **Step 9: Run all rs-endpoint tests**

Run: `cargo test -p rs-endpoint`
Expected: PASS — all tests pass with new key format.

- [ ] **Step 10: Run `cargo fmt`**

Run: `cargo fmt --all --check`
Expected: No formatting issues.

- [ ] **Step 11: Commit**

```bash
git add crates/rs-endpoint/src/s3.rs crates/rs-endpoint/src/uploader.rs crates/rs-endpoint/tests/uploader_integration.rs
git commit -m "feat: S3 key format {evt}/{seq}.bin with duration metadata (#99)"
```

---

### Task 2: S3 Key Format Change — Delivery Side (#99a)

**Files:**
- Modify: `crates/rs-delivery/src/s3_fetch.rs`
- Modify: `crates/rs-delivery/src/db.rs:58-77` (remove `parse_chunk_key`)
- Test: `crates/rs-delivery/src/s3_fetch.rs` (inline test)
- Test: `crates/rs-delivery/src/db.rs` (inline tests)

- [ ] **Step 1: Update `fetch_chunk_with_meta()` — direct GET, parse duration from headers**

Replace the entire `fetch_chunk_with_meta` method in `crates/rs-delivery/src/s3_fetch.rs`:

```rust
/// Fetch a chunk with metadata. Uses direct GET (no LIST needed).
/// Duration is read from the `x-amz-meta-duration-ms` response header
/// set by the uploader.
pub async fn fetch_chunk_with_meta(
    &self,
    chunk_id: i64,
) -> Result<Option<ChunkData>, S3FetchError> {
    let key = format!("{}/{}.bin", self.event_identifier, chunk_id);

    match self.bucket.get_object(&key).await {
        Ok(response) if response.status_code() == 200 => {
            let duration_ms = response
                .headers()
                .get("x-amz-meta-duration-ms")
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            Ok(Some(ChunkData {
                data: response.to_vec(),
                duration_ms,
            }))
        }
        Ok(response) if response.status_code() == 404 => Ok(None),
        Ok(response) => Err(S3FetchError::Fetch(format!(
            "status {}",
            response.status_code()
        ))),
        Err(e) => {
            let err_str = e.to_string();
            if err_str.contains("404") || err_str.contains("NoSuchKey") {
                Ok(None)
            } else {
                Err(S3FetchError::Fetch(err_str))
            }
        }
    }
}
```

- [ ] **Step 2: Add `head_chunk_duration()` — HEAD for buffer fill probing**

Add this method to `S3Fetcher` in `crates/rs-delivery/src/s3_fetch.rs`:

```rust
/// Get chunk duration without downloading data. Uses S3 HEAD request.
/// Returns the duration_ms from the `x-amz-meta-duration-ms` metadata header.
/// Used by buffer fill probing to avoid downloading entire chunks just to read duration.
pub async fn head_chunk_duration(
    &self,
    chunk_id: i64,
) -> Result<Option<i64>, S3FetchError> {
    let key = format!("{}/{}.bin", self.event_identifier, chunk_id);
    match self.bucket.head_object(&key).await {
        Ok((head, 200)) => {
            // rust-s3 strips "x-amz-meta-" prefix: "x-amz-meta-duration-ms" → key "duration-ms"
            let duration_ms = head
                .metadata
                .as_ref()
                .and_then(|m| m.get("duration-ms"))
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0);
            Ok(Some(duration_ms))
        }
        Ok((_, 404)) => Ok(None),
        Ok((_, code)) => Err(S3FetchError::Fetch(format!("HEAD status {code}"))),
        Err(e) => {
            let err_str = e.to_string();
            if err_str.contains("404") || err_str.contains("NoSuchKey") {
                Ok(None)
            } else {
                Err(S3FetchError::Fetch(err_str))
            }
        }
    }
}
```

- [ ] **Step 3: Update `fetch_chunk()` — direct GET, no LIST**

Replace `fetch_chunk` in `crates/rs-delivery/src/s3_fetch.rs`:

```rust
/// Fetch chunk data by sequential ID. Returns None if not found (404).
/// Uses direct GET — key is always constructable as `{event_id}/{chunk_id}.bin`.
pub async fn fetch_chunk(&self, chunk_id: i64) -> Result<Option<Vec<u8>>, S3FetchError> {
    let key = format!("{}/{}.bin", self.event_identifier, chunk_id);
    match self.bucket.get_object(&key).await {
        Ok(response) if response.status_code() == 200 => Ok(Some(response.to_vec())),
        Ok(response) if response.status_code() == 404 => Ok(None),
        Ok(response) => Err(S3FetchError::Fetch(format!(
            "status {}",
            response.status_code()
        ))),
        Err(e) => {
            let err_str = e.to_string();
            if err_str.contains("404") || err_str.contains("NoSuchKey") {
                Ok(None)
            } else {
                Err(S3FetchError::Fetch(err_str))
            }
        }
    }
}
```

- [ ] **Step 4: Update `ChunkFetcher` impl for `S3Fetcher` — use HEAD for duration**

In `crates/rs-delivery/src/endpoint_task.rs`, update the `ChunkFetcher` impl:

```rust
impl ChunkFetcher for S3Fetcher {
    async fn fetch_chunk(&self, chunk_id: i64) -> Result<Option<Vec<u8>>, String> {
        S3Fetcher::fetch_chunk(self, chunk_id)
            .await
            .map_err(|e| e.to_string())
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        S3Fetcher::head_chunk_duration(self, chunk_id)
            .await
            .map_err(|e| e.to_string())
    }
}
```

- [ ] **Step 5: Remove `parse_chunk_key()` from db.rs — now dead code**

In `crates/rs-delivery/src/db.rs`, delete the `parse_chunk_key` function (lines 58-77) and its tests (lines 100-118). The function is no longer used — duration comes from S3 metadata headers, not from parsing the key.

Verify no other callers exist:

```bash
grep -rn "parse_chunk_key" crates/rs-delivery/src/
```

Expected: only the definition and `s3_fetch.rs:79` (which we already removed in Step 1).

- [ ] **Step 6: Update s3_fetch test**

In `crates/rs-delivery/src/s3_fetch.rs`, update the test:

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn chunk_key_format() {
        // New format: no duration in key, just {event_id}/{seq}.bin
        let key = format!("{}/{}.bin", "evt-123", 42);
        assert_eq!(key, "evt-123/42.bin");
    }
}
```

- [ ] **Step 7: Run all rs-delivery tests**

Run: `cargo test -p rs-delivery`
Expected: PASS — all tests pass.

- [ ] **Step 8: Run `cargo fmt`**

Run: `cargo fmt --all --check`
Expected: No formatting issues.

- [ ] **Step 9: Commit**

```bash
git add crates/rs-delivery/src/s3_fetch.rs crates/rs-delivery/src/db.rs crates/rs-delivery/src/endpoint_task.rs
git commit -m "feat: delivery uses direct GET, HEAD for duration — no LIST (#99)"
```

---

### Task 3: Producer-Consumer Pipeline (#99b)

**Files:**
- Modify: `crates/rs-delivery/src/endpoint_task.rs:262-639`
- Test: `crates/rs-delivery/src/endpoint_task_tests.rs`

This is the largest task. The `endpoint_loop` function is refactored from a sequential fetch-normalize-write loop into a producer-consumer pipeline with a bounded channel.

- [ ] **Step 1: Add `PrefetchedChunk` type, channel constant, and update `ChunkFetcher` trait**

At the top of `crates/rs-delivery/src/endpoint_task.rs`, after the existing constants, add:

```rust
/// Number of chunks to pre-fetch ahead of the consumer.
/// At ~2s per chunk, this gives ~20s of buffer against S3 latency spikes.
const PREFETCH_BUFFER_SIZE: usize = 10;

/// A chunk fetched from S3, ready for the consumer to normalize and write.
struct PrefetchedChunk {
    chunk_id: i64,
    data: Vec<u8>,
    duration_ms: i64,
}
```

Add `fetch_chunk_with_meta` to the `ChunkFetcher` trait so the producer can get data+duration in one S3 GET:

```rust
pub trait ChunkFetcher: Send + Sync {
    fn fetch_chunk(
        &self,
        chunk_id: i64,
    ) -> impl std::future::Future<Output = Result<Option<Vec<u8>>, String>> + Send;

    fn fetch_chunk_with_meta(
        &self,
        chunk_id: i64,
    ) -> impl std::future::Future<Output = Result<Option<(Vec<u8>, i64)>, String>> + Send;

    fn chunk_duration_ms(
        &self,
        chunk_id: i64,
    ) -> impl std::future::Future<Output = Result<Option<i64>, String>> + Send;
}
```

Update the `ChunkFetcher` impl for `S3Fetcher`:

```rust
impl ChunkFetcher for S3Fetcher {
    async fn fetch_chunk(&self, chunk_id: i64) -> Result<Option<Vec<u8>>, String> {
        S3Fetcher::fetch_chunk(self, chunk_id)
            .await
            .map_err(|e| e.to_string())
    }

    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        match S3Fetcher::fetch_chunk_with_meta(self, chunk_id).await {
            Ok(Some(cd)) => Ok(Some((cd.data, cd.duration_ms))),
            Ok(None) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        S3Fetcher::head_chunk_duration(self, chunk_id)
            .await
            .map_err(|e| e.to_string())
    }
}
```

Update `MockFetcher` in `endpoint_task_tests.rs` to implement the new trait method:

```rust
async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
    let map = self.chunks.lock().await;
    Ok(map.get(&chunk_id).map(|data| (data.clone(), self.duration_ms_per_chunk)))
}
```

- [ ] **Step 2: Write the producer task**

Add this function to `crates/rs-delivery/src/endpoint_task.rs`:

```rust
/// Producer task: fetches chunks from S3 and sends them into the channel.
/// Runs ahead of the consumer, absorbing S3 latency into the buffer.
async fn producer_task<F: ChunkFetcher>(
    fetcher: F,
    tx: tokio::sync::mpsc::Sender<PrefetchedChunk>,
    start_chunk_id: i64,
    mut stop_rx: watch::Receiver<bool>,
    stats: Stats,
) {
    let mut chunk_id = start_chunk_id;
    let mut s3_backoff_secs: u64 = S3_BACKOFF_BASE_SECS;
    let mut consecutive_misses: u32 = 0;

    loop {
        if *stop_rx.borrow() {
            break;
        }

        // Use fetch_chunk_with_meta to get data+duration in ONE S3 GET
        match fetcher.fetch_chunk_with_meta(chunk_id).await {
            Ok(Some((data, duration_ms))) => {
                consecutive_misses = 0;
                s3_backoff_secs = S3_BACKOFF_BASE_SECS;

                let chunk = PrefetchedChunk {
                    chunk_id,
                    data,
                    duration_ms,
                };

                // Send into channel — blocks if buffer is full (backpressure)
                tokio::select! {
                    result = tx.send(chunk) => {
                        if result.is_err() {
                            // Consumer dropped — stop producing
                            tracing::info!("Producer: consumer gone, stopping");
                            break;
                        }
                    }
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() { break; }
                    }
                }

                chunk_id += 1;
            }
            Ok(None) => {
                consecutive_misses += 1;

                // Skip-ahead probing after too many misses
                if consecutive_misses >= MAX_CHUNK_MISS_COUNT {
                    let mut found = false;
                    for offset in 1..=SKIP_AHEAD_PROBE {
                        let probe_id = chunk_id + offset;
                        if let Ok(Some(_)) = fetcher.fetch_chunk(probe_id).await {
                            tracing::info!(from = chunk_id, to = probe_id, "Producer: skipping ahead");
                            chunk_id = probe_id;
                            consecutive_misses = 0;
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        let mut s = stats.lock().await;
                        s.stall_reason = Some("chunk_gap".to_string());
                        s.consecutive_chunk_misses = consecutive_misses;
                        drop(s);
                        consecutive_misses = 0; // Reset so we probe again
                    }
                } else {
                    let mut s = stats.lock().await;
                    s.consecutive_chunk_misses = consecutive_misses;
                }

                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() { break; }
                    }
                }
            }
            Err(e) => {
                tracing::error!(chunk_id, backoff = s3_backoff_secs, "Producer S3 error: {e}");
                let mut s = stats.lock().await;
                s.last_error = Some(e);
                drop(s);
                tokio::time::sleep(std::time::Duration::from_secs(s3_backoff_secs)).await;
                s3_backoff_secs = (s3_backoff_secs * 2).min(S3_BACKOFF_MAX_SECS);
            }
        }
    }

    tracing::info!("Producer task stopped");
}
```

- [ ] **Step 3: Write the consumer task**

Add this function to `crates/rs-delivery/src/endpoint_task.rs`:

```rust
/// Consumer task: pulls pre-fetched chunks from the channel, normalizes FLV,
/// writes to ffmpeg. Never touches S3 — zero network I/O in the write path.
async fn consumer_task<P: OutputProcessFactory>(
    mut rx: tokio::sync::mpsc::Receiver<PrefetchedChunk>,
    factory: P,
    ep_cfg: EndpointConfig,
    mut stop_rx: watch::Receiver<bool>,
    stats: Stats,
) {
    let alias = ep_cfg.alias.clone();
    let service_type: ServiceType = match ep_cfg.service_type.parse() {
        Ok(st) => st,
        Err(e) => {
            tracing::error!(alias = %alias, "Unknown service type '{}': {e}", ep_cfg.service_type);
            return;
        }
    };

    let mut flv_normalizer = FlvStreamNormalizer::new();
    let mut proc: Option<Box<dyn OutputProcess>> = None;
    let mut consecutive_ffmpeg_failures: u32 = 0;
    let mut circuit_trips: u32 = 0;
    let mut consecutive_write_failures: u32 = 0;
    let mut last_heartbeat = std::time::Instant::now();

    loop {
        if *stop_rx.borrow() {
            break;
        }

        // Periodic heartbeat
        if last_heartbeat.elapsed() >= std::time::Duration::from_secs(ENDPOINT_HEARTBEAT_SECS) {
            let s = stats.lock().await;
            tracing::info!(
                alias = %alias,
                chunk_id = s.current_chunk_id,
                ffmpeg_alive = proc.as_mut().is_some_and(|p| p.is_alive()),
                "Consumer heartbeat"
            );
            drop(s);
            last_heartbeat = std::time::Instant::now();
        }

        // Ensure ffmpeg is running
        if !proc.as_mut().is_some_and(|p| p.is_alive()) {
            if proc.is_some() {
                let mut s = stats.lock().await;
                s.ffmpeg_restart_count += 1;
                if let Some(ref mut p) = proc {
                    s.ffmpeg_last_stderr = p.last_stderr_line();
                }
                drop(s);
                let delay = match consecutive_ffmpeg_failures {
                    0 => 1,
                    1 => 3,
                    _ => 5,
                };
                tracing::warn!(alias = %alias, delay, "ffmpeg died, restarting");
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
                flv_normalizer = FlvStreamNormalizer::new();
            }

            match factory.spawn(service_type, &ep_cfg.stream_key, &alias) {
                Ok(new_proc) => {
                    tracing::info!(alias = %alias, "ffmpeg started");
                    proc = Some(new_proc);
                    consecutive_ffmpeg_failures = 0;
                    let mut s = stats.lock().await;
                    s.consecutive_ffmpeg_failures = 0;
                    if s.stall_reason.as_deref() == Some("ffmpeg_crash_loop") {
                        s.stall_reason = None;
                    }
                }
                Err(e) => {
                    consecutive_ffmpeg_failures += 1;
                    let mut s = stats.lock().await;
                    s.consecutive_ffmpeg_failures = consecutive_ffmpeg_failures;
                    s.last_error = Some(e.clone());

                    if consecutive_ffmpeg_failures >= MAX_FFMPEG_RESTARTS {
                        circuit_trips += 1;
                        let cooldown = (30 * 2u64.pow(circuit_trips.min(4) - 1)).min(300);
                        tracing::error!(alias = %alias, "ffmpeg circuit breaker #{circuit_trips}, cooldown {cooldown}s");
                        s.stall_reason = Some("ffmpeg_crash_loop".to_string());
                        drop(s);
                        tokio::select! {
                            _ = tokio::time::sleep(std::time::Duration::from_secs(cooldown)) => {}
                            _ = stop_rx.changed() => { if *stop_rx.borrow() { break; } }
                        }
                        consecutive_ffmpeg_failures = 0;
                        let mut s = stats.lock().await;
                        s.consecutive_ffmpeg_failures = 0;
                    } else {
                        drop(s);
                        tracing::error!(alias = %alias, "Failed to spawn ffmpeg: {e}");
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                    continue;
                }
            }
        }

        // Pull next chunk from the pre-fetch buffer
        let chunk = tokio::select! {
            c = rx.recv() => c,
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() { break; }
                continue;
            }
        };

        let chunk = match chunk {
            Some(c) => c,
            None => {
                // Channel closed — producer is done
                tracing::info!(alias = %alias, "Producer channel closed, stopping");
                break;
            }
        };

        if circuit_trips > 0 {
            circuit_trips = 0;
            tracing::info!(alias = %alias, "ffmpeg circuit breaker reset");
        }

        {
            let mut s = stats.lock().await;
            s.consecutive_chunk_misses = 0;
            if s.stall_reason.as_deref() == Some("chunk_gap") {
                s.stall_reason = None;
            }
        }

        let processed = flv_normalizer.normalize(&chunk.data);

        if let Some(ref mut p) = proc {
            let write_result = tokio::time::timeout(
                std::time::Duration::from_secs(WRITE_TIMEOUT_SECS),
                p.write(&processed),
            )
            .await;

            match write_result {
                Ok(Ok(())) => {
                    consecutive_write_failures = 0;
                    let mut s = stats.lock().await;
                    s.bytes_processed_total += processed.len() as u64;
                    s.current_chunk_id = chunk.chunk_id;
                    s.chunks_processed += 1;
                }
                Ok(Err(e)) => {
                    consecutive_write_failures += 1;
                    tracing::warn!(alias = %alias, chunk_id = chunk.chunk_id, "ffmpeg write failed: {e}");
                    let mut s = stats.lock().await;
                    s.last_error = Some(e);
                    s.ffmpeg_restart_count += 1;
                    drop(s);
                    if let Some(mut p) = proc.take() {
                        p.kill().await;
                    }
                    if consecutive_write_failures >= MAX_WRITE_FAILURES_PER_CHUNK {
                        consecutive_write_failures = 0;
                        flv_normalizer = FlvStreamNormalizer::new();
                    }
                    continue;
                }
                Err(_) => {
                    consecutive_write_failures += 1;
                    tracing::error!(alias = %alias, chunk_id = chunk.chunk_id, "ffmpeg write timed out");
                    let mut s = stats.lock().await;
                    s.last_error = Some("write_timeout".to_string());
                    s.stall_reason = Some("write_timeout".to_string());
                    s.ffmpeg_restart_count += 1;
                    drop(s);
                    if let Some(mut p) = proc.take() {
                        p.kill().await;
                    }
                    if consecutive_write_failures >= MAX_WRITE_FAILURES_PER_CHUNK {
                        consecutive_write_failures = 0;
                        flv_normalizer = FlvStreamNormalizer::new();
                    }
                    continue;
                }
            }
        }
    }

    if let Some(mut p) = proc {
        p.kill().await;
    }
    tracing::info!(alias = %alias, "Consumer task stopped");
}
```

- [ ] **Step 4: Refactor `endpoint_loop` to use producer-consumer**

Replace the body of `endpoint_loop` in `crates/rs-delivery/src/endpoint_task.rs` (keep the function signature unchanged):

```rust
pub async fn endpoint_loop<F: ChunkFetcher + 'static, P: OutputProcessFactory + 'static>(
    fetcher: F,
    factory: P,
    ep_cfg: EndpointConfig,
    start_chunk_id: i64,
    delivery_delay_ms: u64,
    mut stop_rx: watch::Receiver<bool>,
    stats: Stats,
) {
    let alias = ep_cfg.alias.clone();

    // Buffer fill: wait for enough duration before starting delivery
    if delivery_delay_ms > 0 {
        let mut accum_ms: u64 = 0;
        let mut probe_id = start_chunk_id;
        tracing::info!(alias = %alias, delivery_delay_ms, "Waiting for duration-based buffer fill");
        loop {
            if *stop_rx.borrow() {
                return;
            }
            match fetcher.chunk_duration_ms(probe_id).await {
                Ok(Some(dur_ms)) => {
                    accum_ms += dur_ms.max(0) as u64;
                    probe_id += 1;
                    if accum_ms >= delivery_delay_ms {
                        tracing::info!(alias = %alias, accum_ms, probe_id, "Buffer filled");
                        break;
                    }
                }
                Ok(None) => {
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                        _ = stop_rx.changed() => { if *stop_rx.borrow() { return; } }
                    }
                }
                Err(e) => {
                    tracing::warn!(alias = %alias, "Buffer fill fetch error: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        }
    }

    // Launch producer-consumer pipeline
    let (tx, rx) = tokio::sync::mpsc::channel::<PrefetchedChunk>(PREFETCH_BUFFER_SIZE);

    let producer_stop = stop_rx.clone();
    let producer_stats = stats.clone();
    let producer = tokio::spawn(producer_task(
        fetcher,
        tx,
        start_chunk_id,
        producer_stop,
        producer_stats,
    ));

    let consumer_stop = stop_rx.clone();
    let consumer = tokio::spawn(consumer_task(
        rx,
        factory,
        ep_cfg,
        consumer_stop,
        stats,
    ));

    // Wait for both tasks. If either finishes, stop the other.
    tokio::select! {
        _ = producer => {
            tracing::info!(alias = %alias, "Producer finished");
        }
        _ = consumer => {
            tracing::info!(alias = %alias, "Consumer finished");
        }
        _ = stop_rx.changed() => {}
    }

    tracing::info!(alias = %alias, "Endpoint task stopped");
}
```

Note: the `+ 'static` bounds are needed because `tokio::spawn` requires `'static`. The existing `ChunkFetcher` and `OutputProcessFactory` traits already require `Send + Sync` so this should work. If the mock types in tests need updating, add `'static` to them as well.

- [ ] **Step 5: Update existing tests**

The existing tests in `endpoint_task_tests.rs` test `endpoint_loop` through the same public interface — they provide `MockFetcher` and `MockProcessFactory`. Since the function signature is unchanged, most tests should pass without modification.

However, the producer-consumer pipeline introduces `tokio::spawn` which requires `'static` bounds. The `MockFetcher` and `MockProcessFactory` already use `Arc` internally and implement `Send + Sync`, so this should work.

Key tests to verify still pass:
- `test_processes_sequential_chunks` — chunks 1-5 processed in order
- `test_stops_on_signal` — clean shutdown
- `test_restarts_ffmpeg_on_death` — ffmpeg restart count
- `test_chunk_gap_skip_ahead` — skip-ahead probing
- `test_drought_mode_stops_ffmpeg_and_recovers` — drought recovery
- `test_write_timeout_kills_ffmpeg` — write timeout handling
- `test_processes_100_sequential_chunks` — throughput test

The `drought_mode` logic has moved: skip-ahead probing is now in the producer, and the consumer gets chunks from the channel (never sees missing chunks). The `drought_mode` re-buffering logic from the old loop is no longer needed — the producer handles gap detection and the buffer fill logic at startup is unchanged.

The `test_drought_mode_stops_ffmpeg_and_recovers` test may need adjustment because the consumer no longer has drought mode — it just blocks on the channel. If the producer enters a chunk gap, no chunks reach the consumer. The consumer stays alive waiting on the channel. When chunks resume, the producer sends them and the consumer processes them. The test should be updated to verify this new behavior: chunks stop → consumer blocks → chunks resume → consumer processes them.

Run: `cargo test -p rs-delivery`
Fix any test failures that arise from the refactor. The key invariants are:
1. Chunks are processed in order
2. Stop signal causes clean shutdown
3. ffmpeg restart logic works
4. Skip-ahead probing works (in producer now)
5. Write timeout handling works (in consumer)

- [ ] **Step 6: Run `cargo fmt`**

Run: `cargo fmt --all --check`
Expected: No formatting issues.

- [ ] **Step 7: Commit**

```bash
git add crates/rs-delivery/src/endpoint_task.rs crates/rs-delivery/src/endpoint_task_tests.rs
git commit -m "feat: producer-consumer pipeline with pre-fetch buffer (#99)"
```

---

### Task 4: CI — OBS Scene Selection (#98)

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add "Set OBS scene to test" step**

Add this step in `.github/workflows/ci.yml` BEFORE the existing "Set OBS encoder bitrate to 12000 for CI" step (line 1487). The scene must be set before bitrate, because changing scene may affect encoder settings.

```yaml
      - name: Set OBS scene to 'test' for CI
        id: obs-scene
        shell: powershell
        run: |
          # Set OBS to 'test' scene (drone flight, high-motion) for YouTube E2E quality testing.
          # Uses OBS WebSocket on stream.lan.
          $wsHost = $env:OBS_WS_HOST
          $wsPort = $env:OBS_WS_PORT

          $ws = New-Object System.Net.WebSockets.ClientWebSocket
          $uri = [Uri]"ws://${wsHost}:${wsPort}"
          $cts = New-Object System.Threading.CancellationTokenSource
          $cts.CancelAfter(10000)
          $ws.ConnectAsync($uri, $cts.Token).GetAwaiter().GetResult()

          function Send-OBSMessage($ws, $message) {
            $bytes = [System.Text.Encoding]::UTF8.GetBytes($message)
            $segment = [ArraySegment[byte]]::new($bytes, 0, $bytes.Length)
            $cts2 = New-Object System.Threading.CancellationTokenSource
            $cts2.CancelAfter(10000)
            $ws.SendAsync($segment, [System.Net.WebSockets.WebSocketMessageType]::Text, $true, $cts2.Token).GetAwaiter().GetResult()
          }

          function Receive-OBSMessage($ws) {
            $buffer = New-Object byte[] 65536
            $result = ""
            do {
              $segment = [ArraySegment[byte]]::new($buffer, 0, $buffer.Length)
              $cts3 = New-Object System.Threading.CancellationTokenSource
              $cts3.CancelAfter(10000)
              $recv = $ws.ReceiveAsync($segment, $cts3.Token).GetAwaiter().GetResult()
              $result += [System.Text.Encoding]::UTF8.GetString($buffer, 0, $recv.Count)
            } while (-not $recv.EndOfMessage)
            return $result | ConvertFrom-Json
          }

          # Read Hello
          $hello = Receive-OBSMessage $ws

          # Identify (no auth needed on LAN)
          $identify = @{ op = 1; d = @{ rpcVersion = 1 } } | ConvertTo-Json -Depth 5
          Send-OBSMessage $ws $identify
          $identified = Receive-OBSMessage $ws

          # Get current scene
          $getScene = @{ op = 6; d = @{ requestType = "GetCurrentProgramScene"; requestId = "get-scene" } } | ConvertTo-Json -Depth 5
          Send-OBSMessage $ws $getScene
          $sceneResp = Receive-OBSMessage $ws
          $origScene = $sceneResp.d.responseData.sceneName
          Write-Host "Original scene: $origScene"
          "ORIG_SCENE=$origScene" | Out-File -FilePath $env:GITHUB_OUTPUT -Append

          if ($origScene -ne "test") {
            # Set scene to 'test'
            $setScene = @{ op = 6; d = @{ requestType = "SetCurrentProgramScene"; requestId = "set-scene"; requestData = @{ sceneName = "test" } } } | ConvertTo-Json -Depth 5
            Send-OBSMessage $ws $setScene
            $setResp = Receive-OBSMessage $ws
            Write-Host "Scene set to 'test'"
            Start-Sleep -Seconds 2
          } else {
            Write-Host "Scene already 'test', no change needed"
          }

          $ws.CloseAsync([System.Net.WebSockets.WebSocketCloseStatus]::NormalClosure, "", [System.Threading.CancellationToken]::None).GetAwaiter().GetResult()
```

- [ ] **Step 2: Add "Restore OBS scene" step in cleanup section**

Add this step near the existing "Restore OBS encoder bitrate" step (around line 3333), in the `if: always()` cleanup section:

```yaml
      - name: Restore OBS scene
        if: always()
        shell: powershell
        run: |
          $origScene = "${{ steps.obs-scene.outputs.ORIG_SCENE }}"
          if (-not $origScene -or $origScene -eq "test") {
            Write-Host "No scene restore needed (original was 'test' or not captured)"
            return
          }
          try {
            $wsHost = $env:OBS_WS_HOST
            $wsPort = $env:OBS_WS_PORT
            $ws = New-Object System.Net.WebSockets.ClientWebSocket
            $uri = [Uri]"ws://${wsHost}:${wsPort}"
            $cts = New-Object System.Threading.CancellationTokenSource
            $cts.CancelAfter(10000)
            $ws.ConnectAsync($uri, $cts.Token).GetAwaiter().GetResult()

            function Send-OBSMessage($ws, $message) {
              $bytes = [System.Text.Encoding]::UTF8.GetBytes($message)
              $segment = [ArraySegment[byte]]::new($bytes, 0, $bytes.Length)
              $cts2 = New-Object System.Threading.CancellationTokenSource
              $cts2.CancelAfter(10000)
              $ws.SendAsync($segment, [System.Net.WebSockets.WebSocketMessageType]::Text, $true, $cts2.Token).GetAwaiter().GetResult()
            }

            function Receive-OBSMessage($ws) {
              $buffer = New-Object byte[] 65536
              $result = ""
              do {
                $segment = [ArraySegment[byte]]::new($buffer, 0, $buffer.Length)
                $cts3 = New-Object System.Threading.CancellationTokenSource
                $cts3.CancelAfter(10000)
                $recv = $ws.ReceiveAsync($segment, $cts3.Token).GetAwaiter().GetResult()
                $result += [System.Text.Encoding]::UTF8.GetString($buffer, 0, $recv.Count)
              } while (-not $recv.EndOfMessage)
              return $result | ConvertFrom-Json
            }

            $hello = Receive-OBSMessage $ws
            $identify = @{ op = 1; d = @{ rpcVersion = 1 } } | ConvertTo-Json -Depth 5
            Send-OBSMessage $ws $identify
            $identified = Receive-OBSMessage $ws

            $setScene = @{ op = 6; d = @{ requestType = "SetCurrentProgramScene"; requestId = "restore-scene"; requestData = @{ sceneName = $origScene } } } | ConvertTo-Json -Depth 5
            Send-OBSMessage $ws $setScene
            $setResp = Receive-OBSMessage $ws
            Write-Host "Scene restored to '$origScene'"

            $ws.CloseAsync([System.Net.WebSockets.WebSocketCloseStatus]::NormalClosure, "", [System.Threading.CancellationToken]::None).GetAwaiter().GetResult()
          } catch {
            Write-Host "WARNING: Failed to restore OBS scene: $_"
          }
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "feat: CI sets OBS scene to 'test' for YouTube E2E (#98)"
```

---

### Task 5: CI — bitrateLow Gate (#96) + Second Health Gate (#95)

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add bitrateLow check to the first health gate**

In `.github/workflows/ci.yml`, in the "GATE: Verify YouTube stream health is 'good'" step (around line 2742), after the `health == "good"` success block (the `return` on line 2748), add a bitrateLow check inside the success branch, BEFORE the `return`:

Replace the success block:

```powershell
              if ($response.stream_receiving -and $healthValue -eq "good") {
                # Also check configurationIssues for bitrateLow
                $hasBitrateLow = $false
                if ($response.streams) {
                  foreach ($s in $response.streams) {
                    if ($s.configuration_issues) {
                      foreach ($issue in $s.configuration_issues) {
                        if ($issue -match "bitrateLow") {
                          $hasBitrateLow = $true
                          Write-Host "  WARN: configurationIssues contains bitrateLow despite good health"
                        }
                      }
                    }
                  }
                }
                if ($hasBitrateLow) {
                  throw "FAILED: YouTube reports bitrateLow in configurationIssues despite health='good'"
                }
                Write-Host ""
                Write-Host "=========================================="
                Write-Host "  YOUTUBE STREAM HEALTH: GOOD (no bitrateLow)"
                Write-Host "  Matches OBS direct quality."
                Write-Host "=========================================="
                return
              }
```

- [ ] **Step 2: Add second health gate after resilience tests**

In `.github/workflows/ci.yml`, add a new step AFTER the "Restreamer crash recovery test" step (after line 3124 "=== Restreamer Crash Recovery Test PASSED ===") and BEFORE the "Stop OBS stream" step:

```yaml
      - name: "GATE: Second YouTube health check after resilience tests"
        if: success()
        shell: powershell
        timeout-minutes: 2
        run: |
          # STRICT GATE: After resilience tests (OBS disconnect, network disconnect, crash),
          # the stream has been running 10+ minutes. Any degradation would be visible now.
          $maxRetries = 5
          $retryDelay = 10

          for ($i = 1; $i -le $maxRetries; $i++) {
            Write-Host "--- Second gate attempt ${i}/${maxRetries} ---"
            try {
              $response = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/youtube/status" -TimeoutSec 30

              $healthValue = ""
              if ($response.streams) {
                foreach ($s in $response.streams) {
                  Write-Host "  Stream '$($s.title)': health=$($s.health_status)"
                  if ($s.stream_status -eq "active") {
                    $healthValue = $s.health_status
                  }
                  if ($s.configuration_issues -and $s.configuration_issues.Count -gt 0) {
                    foreach ($issue in $s.configuration_issues) {
                      Write-Host "    ISSUE: $issue"
                    }
                  }
                }
              }

              if ($response.stream_receiving -and $healthValue -eq "good") {
                # Check bitrateLow
                $hasBitrateLow = $false
                if ($response.streams) {
                  foreach ($s in $response.streams) {
                    if ($s.configuration_issues) {
                      foreach ($issue in $s.configuration_issues) {
                        if ($issue -match "bitrateLow") {
                          $hasBitrateLow = $true
                        }
                      }
                    }
                  }
                }
                if ($hasBitrateLow) {
                  throw "SECOND GATE FAILED: bitrateLow after resilience tests"
                }
                Write-Host ""
                Write-Host "=========================================="
                Write-Host "  SECOND GATE PASSED: health=good, no bitrateLow"
                Write-Host "=========================================="
                return
              }

              if ($healthValue -and $healthValue -ne "good") {
                Write-Host "  Health='${healthValue}' (need 'good'). Waiting..."
              }
            } catch {
              Write-Host "  Request failed: $_"
            }

            if ($i -lt $maxRetries) {
              Start-Sleep -Seconds $retryDelay
            }
          }

          throw "SECOND GATE FAILED: YouTube health is '${healthValue}' after resilience tests. Stream quality degraded during operation."
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "feat: CI bitrateLow gate + second health gate after resilience (#95, #96)"
```

---

### Task 6: Local Checks, Push, Monitor CI

**Files:** None (verification only)

- [ ] **Step 1: Run local checks**

```bash
cargo fmt --all --check
```

Fix any issues before proceeding.

- [ ] **Step 2: Push to dev**

```bash
git push origin dev
```

- [ ] **Step 3: Monitor CI**

```bash
gh run list --limit 3
# Wait, then:
gh run view <run-id>
```

ALL jobs must pass — lint, test, build, E2E, deploy, YouTube E2E with both health gates.

If any job fails: `gh run view <run-id> --log-failed` — investigate, fix, and push again.

- [ ] **Step 4: Create PR**

```bash
gh pr create --title "fix: delivery pipeline redesign — eliminate S3 LIST, add pre-fetch buffer (#94)" --body "$(cat <<'EOF'
## Summary
- S3 key format changed from `{evt}/{seq}_{dur}_{evt}.bin` to `{evt}/{seq}.bin` — eliminates LIST per chunk
- Duration stored as S3 object metadata (`x-amz-meta-duration-ms`) — GET returns data + duration in one request
- Buffer fill probing uses HEAD (metadata only, no data download)
- Delivery pipeline refactored to producer-consumer with 10-chunk pre-fetch buffer (~20s)
- CI: OBS scene set to "test" (high-motion drone flight) for YouTube E2E
- CI: Second YouTube health gate after resilience tests (strict: 5 retries, 10s)
- CI: bitrateLow in configurationIssues now fails CI

Closes #94, closes #95, closes #96, closes #98, closes #99
(#97 already implemented)

## Test plan
- [ ] S3 upload uses new key format with metadata header
- [ ] VPS fetches chunks with direct GET (no LIST)
- [ ] Buffer fill uses HEAD requests (no data download)
- [ ] Pre-fetch buffer absorbs S3 latency spikes
- [ ] YouTube health stays "good" through entire E2E test
- [ ] Second health gate passes after resilience tests
- [ ] No bitrateLow in configurationIssues
- [ ] All existing E2E tests pass

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Monitor PR CI run**

All jobs must pass. The E2E YouTube test is the ultimate verification.

- [ ] **Step 6: Verify PR is mergeable**

```bash
gh api repos/zbynekdrlik/restreamer/pulls/NUMBER --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

Expected: `mergeable: true`, `mergeable_state: "clean"`.

---

### Verification Checklist

1. **S3 key format**: `cargo test -p rs-endpoint chunk_key` passes with new format
2. **Delivery GET**: no LIST calls in `s3_fetch.rs` — direct GET only
3. **HEAD for duration**: `head_chunk_duration` uses HEAD, not GET
4. **Pre-fetch buffer**: producer-consumer pipeline with 10-chunk bounded channel
5. **CI OBS scene**: "test" scene selected before YouTube E2E
6. **CI bitrateLow**: both health gates check configurationIssues
7. **CI second gate**: strict gate (5 retries) after resilience tests
8. **All tests pass**: `cargo test -p rs-endpoint && cargo test -p rs-delivery`
9. **CI green**: all jobs including YouTube E2E pass
10. **PR mergeable**: clean, no conflicts
