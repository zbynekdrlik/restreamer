# Buffer Rescue Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When the delivery buffer empties during an outage, switch endpoints to a looped rescue video with countdown overlay until the buffer refills to 120s. Also play the rescue video during initial warmup.

**Architecture:** All rescue logic lives in rs-delivery (VPS-side). A new `rescue.rs` module handles rescue ffmpeg spawning, countdown file writing, and buffer monitoring. The consumer task becomes a state machine with Warmup/Normal/Rescue/Recovering states. The rescue video URL flows from DB → orchestrator → VPS `/api/init` payload.

**Tech Stack:** Rust, tokio, ffmpeg (drawtext filter), SQLite (migration V14), Leptos (dashboard), Playwright (E2E)

**Spec:** `docs/superpowers/specs/2026-04-12-buffer-rescue-mode-design.md`

---

## File Structure

| File | Action | Responsibility |
|------|--------|---------------|
| `Cargo.toml` (root) | Modify | Version bump |
| `src-tauri/Cargo.toml` | Modify | Version bump |
| `src-tauri/tauri.conf.json` | Modify | Version bump |
| `leptos-ui/Cargo.toml` | Modify | Version bump |
| `crates/rs-core/src/db/mod.rs` | Modify | Migration V14 (add rescue_video_url column) |
| `crates/rs-core/src/db/v2.rs` | Modify | Update DB functions to include rescue_video_url |
| `crates/rs-core/src/models.rs` | Modify | Add rescue_video_url to StreamingEvent, delivery_mode to metrics |
| `crates/rs-delivery/src/rescue.rs` | Create | Rescue ffmpeg spawning, countdown file, buffer monitor |
| `crates/rs-delivery/src/rescue_tests.rs` | Create | Unit tests for rescue module |
| `crates/rs-delivery/src/endpoint_task.rs` | Modify | State machine (DeliveryMode), wire rescue into consumer |
| `crates/rs-delivery/src/api.rs` | Modify | Add rescue_video_url to InitRequest, delivery_mode to status |
| `crates/rs-delivery/src/main.rs` | Modify | Add `mod rescue;` |
| `crates/rs-api/src/delivery.rs` | Modify | Pass rescue_video_url in /api/init payload |
| `crates/rs-api/src/stream_handlers.rs` | Modify | Add rescue_video_url to UpdateEventRequest |
| `leptos-ui/src/api.rs` | Modify | Add rescue_video_url to API types, delivery_mode to metrics |
| `leptos-ui/src/ws.rs` | Modify | Add delivery_mode to WsEndpointMetrics |
| `leptos-ui/src/store.rs` | Modify | Add delivery_mode to EndpointData |
| `leptos-ui/src/components/settings.rs` | Modify | Add rescue video URL field to event settings |
| `leptos-ui/src/components/operator_dashboard.rs` | Modify | Show delivery mode badge on endpoint cards |
| `e2e/mock-api.js` | Modify | Add rescue_video_url to mock event responses |
| `.github/workflows/ci.yml` | Modify | Add mutation testing exclusions for rescue module |

---

### Task 0: Version Bump

**Files:**
- Modify: `Cargo.toml:24`
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml`

- [ ] **Step 1: Bump version 0.3.35 → 0.3.36 in all four files**

`Cargo.toml` line 24: `version = "0.3.35"` → `version = "0.3.36"`
`src-tauri/Cargo.toml`: `version = "0.3.35"` → `version = "0.3.36"`
`src-tauri/tauri.conf.json`: `"version": "0.3.35"` → `"version": "0.3.36"`
`leptos-ui/Cargo.toml`: `version = "0.3.35"` → `version = "0.3.36"`

- [ ] **Step 2: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.36"
```

---

### Task 1: Database Migration V14 — Add rescue_video_url Column

**Files:**
- Modify: `crates/rs-core/src/db/mod.rs:64-78` (migrations array)
- Test: `crates/rs-core/src/db/tests.rs`

- [ ] **Step 1: Write failing test**

Add to `crates/rs-core/src/db/tests.rs`:

```rust
#[tokio::test]
async fn migration_v14_rescue_video_url_column_exists() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();

    // Verify column exists by inserting with rescue_video_url
    sqlx::query("INSERT INTO streaming_events (name, received_bytes, receiving_activated, delivering_activated, rescue_video_url) VALUES ('test', 0, 0, 0, 'https://example.com/rescue.mp4')")
        .execute(&pool)
        .await
        .unwrap();

    let row = sqlx::query("SELECT rescue_video_url FROM streaming_events WHERE name = 'test'")
        .fetch_one(&pool)
        .await
        .unwrap();
    let url: Option<String> = row.get("rescue_video_url");
    assert_eq!(url, Some("https://example.com/rescue.mp4".to_string()));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p rs-core migration_v14 -- --nocapture`
Expected: FAIL — column `rescue_video_url` does not exist

- [ ] **Step 3: Add migration V14**

In `crates/rs-core/src/db/mod.rs`, after the `MIGRATION_V13_SQL` constant (around line 320), add:

```rust
const MIGRATION_V14_SQL: &str = r#"
ALTER TABLE streaming_events ADD COLUMN rescue_video_url TEXT
"#;
```

And update the migrations array (around line 78) to include:

```rust
(14, MIGRATION_V14_SQL),
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p rs-core migration_v14 -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add crates/rs-core/src/db/mod.rs crates/rs-core/src/db/tests.rs
git commit -m "feat: add migration V14 — rescue_video_url column (#62)"
```

---

### Task 2: Update Models and DB Functions for rescue_video_url

**Files:**
- Modify: `crates/rs-core/src/models.rs:20-29` (StreamingEvent struct)
- Modify: `crates/rs-core/src/db/mod.rs:377-397` (get_streaming_event_by_id)
- Modify: `crates/rs-core/src/db/v2.rs:443-456` (update_streaming_event)
- Modify: `crates/rs-core/src/models.rs:181-197` (DeliveryEndpointMetrics)
- Test: `crates/rs-core/src/db/tests.rs`

- [ ] **Step 1: Write failing test for rescue_video_url in StreamingEvent**

Add to `crates/rs-core/src/db/tests.rs`:

```rust
#[tokio::test]
async fn update_event_rescue_video_url() {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    create_streaming_event(&pool, "rescue-test").await.unwrap();
    let events = list_streaming_events(&pool).await.unwrap();
    let id = events[0].id;

    // Initially null
    let evt = get_streaming_event_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(evt.rescue_video_url, None);

    // Update with rescue video URL
    update_streaming_event(&pool, id, "rescue-test", None, Some("https://example.com/rescue.mp4".to_string()))
        .await
        .unwrap();
    let evt = get_streaming_event_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(evt.rescue_video_url, Some("https://example.com/rescue.mp4".to_string()));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p rs-core update_event_rescue_video_url -- --nocapture`
Expected: FAIL — `rescue_video_url` not a field on StreamingEvent

- [ ] **Step 3: Add rescue_video_url to StreamingEvent**

In `crates/rs-core/src/models.rs`, add to the `StreamingEvent` struct (after `created_from`):

```rust
pub rescue_video_url: Option<String>,
```

- [ ] **Step 4: Update get_streaming_event_by_id to read rescue_video_url**

In `crates/rs-core/src/db/mod.rs`, function `get_streaming_event_by_id` (line 377):

Change the SQL query to:
```rust
"SELECT id, name, received_bytes, receiving_activated, delivering_activated, cache_delay_secs, created_from, rescue_video_url
 FROM streaming_events WHERE id = ?1"
```

And add to the struct construction:
```rust
rescue_video_url: r.get("rescue_video_url"),
```

Also update ALL other functions that construct `StreamingEvent` from rows:
- `get_streaming_event` (around line 155)
- `list_streaming_events` (around line 170)
- Any other function returning `StreamingEvent`

Search for all `StreamingEvent {` constructions and add `rescue_video_url: r.get("rescue_video_url")` (or `rescue_video_url: None` for tests creating the struct directly).

- [ ] **Step 5: Update update_streaming_event to accept rescue_video_url**

In `crates/rs-core/src/db/v2.rs`, change the function signature:

```rust
pub async fn update_streaming_event(
    pool: &SqlitePool,
    id: i64,
    name: &str,
    cache_delay_secs: Option<i64>,
    rescue_video_url: Option<String>,
) -> Result<()> {
    sqlx::query("UPDATE streaming_events SET name = ?1, cache_delay_secs = ?2, rescue_video_url = ?3 WHERE id = ?4")
        .bind(name)
        .bind(cache_delay_secs)
        .bind(&rescue_video_url)
        .bind(id)
        .execute(pool)
        .await?;
    Ok(())
}
```

- [ ] **Step 6: Fix all callers of update_streaming_event**

In `crates/rs-api/src/stream_handlers.rs`, the `update_event` handler (line 260) currently calls:
```rust
db::update_streaming_event(&state.pool, id, new_name, new_delay)
```
Change to:
```rust
let new_rescue_url = req.rescue_video_url.clone().or(existing.rescue_video_url.clone());
db::update_streaming_event(&state.pool, id, new_name, new_delay, new_rescue_url)
```

Also add `rescue_video_url` to `UpdateEventRequest` in stream_handlers.rs (line 233):
```rust
#[derive(Deserialize)]
pub struct UpdateEventRequest {
    pub name: Option<String>,
    pub cache_delay_secs: Option<i64>,
    pub rescue_video_url: Option<String>,
}
```

- [ ] **Step 7: Add delivery_mode to DeliveryEndpointMetrics**

In `crates/rs-core/src/models.rs`, add to `DeliveryEndpointMetrics` (after `is_fast`):

```rust
#[serde(default)]
pub delivery_mode: Option<String>,
#[serde(default)]
pub rescue_eta_secs: Option<u64>,
```

- [ ] **Step 8: Fix all DeliveryEndpointMetrics constructions**

Search for all `DeliveryEndpointMetrics {` and add `delivery_mode: None, rescue_eta_secs: None` (or appropriate values). Key locations:
- `crates/rs-api/src/delivery.rs` (in `get_delivery_status`)
- `crates/rs-core/src/models.rs` (tests)

- [ ] **Step 9: Run tests to verify all pass**

Run: `cargo test -p rs-core -- --nocapture`
Expected: PASS

- [ ] **Step 10: Commit**

```bash
git add crates/rs-core/src/models.rs crates/rs-core/src/db/mod.rs crates/rs-core/src/db/v2.rs crates/rs-core/src/db/tests.rs crates/rs-api/src/stream_handlers.rs crates/rs-api/src/delivery.rs
git commit -m "feat: add rescue_video_url to event model and DB functions (#62)"
```

---

### Task 3: Rescue Module — ffmpeg Spawning and Countdown

**Files:**
- Create: `crates/rs-delivery/src/rescue.rs`
- Create: `crates/rs-delivery/src/rescue_tests.rs`
- Modify: `crates/rs-delivery/src/main.rs` (add `mod rescue;`)

This is the core new module. It manages rescue ffmpeg process lifecycle and countdown file writing.

- [ ] **Step 1: Write failing tests for rescue module**

Create `crates/rs-delivery/src/rescue_tests.rs`:

```rust
use super::rescue::*;

#[test]
fn build_rescue_ffmpeg_args_rtmp_endpoint() {
    let args = build_rescue_ffmpeg_args(
        "https://s3.example.com/rescue.mp4",
        "rtmps://live-api-s.facebook.com:443/rtmp/key123",
        "flv",
        "FB-Test",
    );
    // Must have -stream_loop -1 for infinite looping
    assert!(args.contains(&"-stream_loop".to_string()));
    assert!(args.contains(&"-1".to_string()));
    // Must have -re for real-time pacing
    assert!(args.contains(&"-re".to_string()));
    // Must have drawtext filter with textfile and reload=1
    let vf_idx = args.iter().position(|a| a == "-vf").unwrap();
    let vf_val = &args[vf_idx + 1];
    assert!(vf_val.contains("drawtext="));
    assert!(vf_val.contains("reload=1"));
    assert!(vf_val.contains("/tmp/rescue_FB-Test.txt"));
    // Must output to the endpoint URL
    assert!(args.last().unwrap().contains("facebook.com"));
}

#[test]
fn build_rescue_ffmpeg_args_hls_endpoint() {
    let args = build_rescue_ffmpeg_args(
        "https://s3.example.com/rescue.mp4",
        "https://a.upload.youtube.com/http_upload_hls?cid=key123&copy=0&file=out1248.ts",
        "hls",
        "YT-Test",
    );
    // HLS output must have hls-specific flags
    assert!(args.iter().any(|a| a == "hls"));
    assert!(args.iter().any(|a| a == "PUT"));
}

#[test]
fn format_countdown_warmup() {
    let text = format_countdown_text(DeliveryMode::Rescue { reason: RescueReason::Warmup }, 95);
    assert_eq!(text, "Stream starting ~ 1m 35s");
}

#[test]
fn format_countdown_buffer_empty() {
    let text = format_countdown_text(DeliveryMode::Rescue { reason: RescueReason::BufferEmpty }, 30);
    assert_eq!(text, "Stream recovering ~ 30s");
}

#[test]
fn format_countdown_zero() {
    let text = format_countdown_text(DeliveryMode::Rescue { reason: RescueReason::Warmup }, 0);
    assert_eq!(text, "Stream starting soon");
}

#[test]
fn format_countdown_normal_mode_empty() {
    let text = format_countdown_text(DeliveryMode::Normal, 120);
    assert_eq!(text, "");
}
```

- [ ] **Step 2: Create rescue.rs module skeleton**

Create `crates/rs-delivery/src/rescue.rs`:

```rust
//! Rescue mode: plays a looped video with countdown overlay when the
//! delivery buffer is empty (warmup or outage recovery).
use rs_ffmpeg::ServiceType;

/// Fixed buffer refill target before resuming normal delivery (seconds).
pub const RESCUE_REFILL_TARGET_SECS: u64 = 120;

/// Seconds of producer stall (no new chunks) before entering rescue mode.
pub const RESCUE_STALL_THRESHOLD_SECS: u64 = 30;

/// Delivery mode state machine.
#[derive(Debug, Clone, PartialEq)]
pub enum DeliveryMode {
    /// Normal chunk delivery.
    Normal,
    /// Playing rescue video (warmup or buffer empty).
    Rescue { reason: RescueReason },
}

#[derive(Debug, Clone, PartialEq)]
pub enum RescueReason {
    /// Initial buffer fill — stream hasn't started yet.
    Warmup,
    /// Buffer drained during an outage.
    BufferEmpty,
}

/// Build ffmpeg arguments for the rescue video loop with drawtext overlay.
///
/// `rescue_video_url`: URL of the looped video (S3, HTTP, etc.)
/// `endpoint_url`: The target endpoint URL (RTMP URL or HLS upload URL)
/// `output_format`: "flv" for RTMP, "hls" for YouTube HLS
/// `alias`: Endpoint alias (used for countdown file path)
pub fn build_rescue_ffmpeg_args(
    rescue_video_url: &str,
    endpoint_url: &str,
    output_format: &str,
    alias: &str,
) -> Vec<String> {
    let countdown_path = countdown_file_path(alias);
    let drawtext = format!(
        "drawtext=textfile={}:reload=1:fontsize=48:fontcolor=white:x=(w-tw)/2:y=h-80:borderw=2:bordercolor=black",
        countdown_path
    );

    let mut args = vec![
        "-stream_loop".into(), "-1".into(),
        "-re".into(),
        "-i".into(), rescue_video_url.to_string(),
        "-vf".into(), drawtext,
        "-c:v".into(), "libx264".into(),
        "-preset".into(), "ultrafast".into(),
        "-c:a".into(), "aac".into(),
        "-b:a".into(), "128k".into(),
    ];

    match output_format {
        "hls" => {
            args.extend_from_slice(&[
                "-f".into(), "hls".into(),
                "-hls_segment_type".into(), "mpegts".into(),
                "-hls_list_size".into(), "5".into(),
                "-hls_time".into(), "2".into(),
                "-hls_flags".into(), "delete_segments".into(),
                "-start_number".into(), "0".into(),
                "-method".into(), "PUT".into(),
                "-flags".into(), "+cgop".into(),
                "-muxdelay".into(), "0".into(),
                "-muxpreload".into(), "0".into(),
                "-reset_timestamps".into(), "1".into(),
                endpoint_url.to_string(),
            ]);
        }
        _ => {
            // RTMP/RTMPS output (FLV)
            args.extend_from_slice(&[
                "-f".into(), "flv".into(),
                "-flvflags".into(), "no_duration_filesize".into(),
                endpoint_url.to_string(),
            ]);
        }
    }

    args
}

/// Format the countdown text for the rescue video overlay.
pub fn format_countdown_text(mode: DeliveryMode, eta_secs: u64) -> String {
    match mode {
        DeliveryMode::Normal => String::new(),
        DeliveryMode::Rescue { reason } => {
            let prefix = match reason {
                RescueReason::Warmup => "Stream starting",
                RescueReason::BufferEmpty => "Stream recovering",
            };
            if eta_secs == 0 {
                format!("{prefix} soon")
            } else if eta_secs >= 60 {
                let mins = eta_secs / 60;
                let secs = eta_secs % 60;
                format!("{prefix} ~ {mins}m {secs}s")
            } else {
                format!("{prefix} ~ {eta_secs}s")
            }
        }
    }
}

/// Path to the countdown text file for a given endpoint alias.
pub fn countdown_file_path(alias: &str) -> String {
    let safe_alias = alias.replace([' ', '/', '\\'], "_");
    format!("/tmp/rescue_{safe_alias}.txt")
}

/// Write the countdown text to the file. Called periodically by the producer.
pub fn write_countdown_file(alias: &str, text: &str) {
    let path = countdown_file_path(alias);
    if let Err(e) = std::fs::write(&path, text) {
        tracing::warn!(alias, path, "Failed to write countdown file: {e}");
    }
}

/// Clean up the countdown file when rescue mode ends.
pub fn cleanup_countdown_file(alias: &str) {
    let path = countdown_file_path(alias);
    let _ = std::fs::remove_file(&path);
}

/// Determine the output format string based on service type.
pub fn output_format_for_service(service_type: ServiceType) -> &'static str {
    match service_type {
        ServiceType::YtHls => "hls",
        _ => "flv",
    }
}

/// Build the endpoint URL for a given service type and stream key.
/// Mirrors the logic in rs_ffmpeg::build_ffmpeg_args but returns the URL
/// without the ffmpeg wrapping.
pub fn endpoint_url_for_service(service_type: ServiceType, stream_key: &str) -> String {
    match service_type {
        ServiceType::YtHls => format!(
            "https://a.upload.youtube.com/http_upload_hls?cid={stream_key}&copy=0&file=out1248.ts"
        ),
        ServiceType::YtRtmp => format!("rtmp://a.rtmp.youtube.com/live2/{stream_key}"),
        ServiceType::Facebook => format!("rtmps://live-api-s.facebook.com:443/rtmp/{stream_key}"),
        ServiceType::Vimeo => format!("rtmps://rtmp-global.cloud.vimeo.com:443/live/{stream_key}"),
        ServiceType::Instagram => {
            format!("rtmps://live-upload.instagram.com:443/rtmp/{stream_key}")
        }
        ServiceType::TestFile => {
            let output_dir = std::env::var("RESTREAMER_TEST_OUTPUT_DIR")
                .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().to_string());
            let safe = stream_key.replace([' ', '/'], "_");
            format!("{output_dir}/restreamer_rescue_{safe}.flv")
        }
    }
}
```

- [ ] **Step 3: Add mod declaration in main.rs**

In `crates/rs-delivery/src/main.rs`, add after `mod s3_fetch;`:
```rust
pub mod rescue;
```

- [ ] **Step 4: Add test module declaration in rescue.rs**

At the bottom of `crates/rs-delivery/src/rescue.rs`:
```rust
#[cfg(test)]
#[path = "rescue_tests.rs"]
mod tests;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p rs-delivery rescue -- --nocapture`
Expected: PASS (all 6 tests)

- [ ] **Step 6: Commit**

```bash
git add crates/rs-delivery/src/rescue.rs crates/rs-delivery/src/rescue_tests.rs crates/rs-delivery/src/main.rs
git commit -m "feat: add rescue module — ffmpeg args, countdown, state types (#62)"
```

---

### Task 4: Wire Rescue Mode into Endpoint Task

**Files:**
- Modify: `crates/rs-delivery/src/endpoint_task.rs`
- Modify: `crates/rs-delivery/src/api.rs`

This is the core integration task. The `endpoint_loop` and `consumer_task` gain rescue mode awareness.

- [ ] **Step 1: Add rescue_video_url to InitRequest and AppState**

In `crates/rs-delivery/src/api.rs`, add to `InitRequest` (after `delivery_delay_ms`):
```rust
#[serde(default)]
pub rescue_video_url: Option<String>,
```

In `crates/rs-delivery/src/main.rs`, add to `AppState`:
```rust
pub rescue_video_url: RwLock<Option<String>>,
```

In `AppState::new()`, add:
```rust
rescue_video_url: RwLock::new(None),
```

In `init_endpoints` handler (api.rs, after line 160 where delivery_delay_ms is stored):
```rust
*state.rescue_video_url.write().await = req.rescue_video_url.clone();
```

- [ ] **Step 2: Add delivery_mode and rescue_eta_secs to EndpointStats**

In `crates/rs-delivery/src/endpoint_task.rs`, add to `EndpointStats` (after `restart_history`):
```rust
pub delivery_mode: String,
pub rescue_eta_secs: Option<u64>,
```

Update `EndpointStats` Default impl — the derive(Default) will set `delivery_mode` to `""`. Instead, initialize it to `"warmup"` in the `EndpointHandle::spawn` constructor (line 252):
```rust
let stats: Stats = Arc::new(Mutex::new(EndpointStats {
    current_chunk_id: start_chunk_id,
    delivery_mode: "warmup".to_string(),
    ..Default::default()
}));
```

- [ ] **Step 3: Add delivery_mode and rescue_eta_secs to status response**

In `crates/rs-delivery/src/api.rs`, add to `EndpointStatusEntry` (after `restart_history`):
```rust
pub delivery_mode: String,
#[serde(skip_serializing_if = "Option::is_none")]
pub rescue_eta_secs: Option<u64>,
```

In the `endpoint_status` handler, add to the `EndpointStatusEntry` construction:
```rust
delivery_mode: stats.delivery_mode.clone(),
rescue_eta_secs: stats.rescue_eta_secs,
```

- [ ] **Step 4: Add shared buffer state for producer→consumer communication**

In `crates/rs-delivery/src/endpoint_task.rs`, add a shared atomic for the producer to report buffer duration:

```rust
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

/// Shared buffer state between producer and consumer for rescue mode.
pub struct BufferState {
    /// Estimated buffer duration in ms (chunks available on S3 ahead of consumer).
    pub buffer_duration_ms: AtomicU64,
    /// Whether the producer is actively finding new chunks (vs stalled).
    pub producer_active: AtomicBool,
}

impl BufferState {
    pub fn new() -> Self {
        Self {
            buffer_duration_ms: AtomicU64::new(0),
            producer_active: AtomicBool::new(true),
        }
    }
}
```

Add `use std::sync::atomic::AtomicBool;` at the top (after the existing `use std::sync::Arc;`).

- [ ] **Step 5: Pass rescue_video_url and BufferState through endpoint_loop**

Update `EndpointHandle::spawn` to accept `rescue_video_url: Option<String>`:

```rust
pub fn spawn(
    ep_cfg: EndpointConfig,
    s3_cfg: S3Config,
    event_identifier: String,
    start_chunk_id: i64,
    delivery_delay_ms: u64,
    rescue_video_url: Option<String>,
) -> Self {
```

Create `BufferState` in `spawn` and pass it to both `producer_task` and `consumer_task` via `endpoint_loop`:

```rust
let buffer_state = Arc::new(BufferState::new());
```

Update `endpoint_loop` signature to accept `rescue_video_url: Option<String>` and `buffer_state: Arc<BufferState>`.

Pass `buffer_state.clone()` to both `producer_task` and `consumer_task`.

- [ ] **Step 6: Update producer_task to track buffer duration**

In `producer_task`, add a `buffer_state: Arc<BufferState>` parameter. After a successful chunk fetch+send (line 354):

```rust
// Update buffer duration estimate for rescue mode
let current_buffer = buffer_state.buffer_duration_ms.load(AtomicOrdering::Relaxed);
buffer_state.buffer_duration_ms.store(
    current_buffer.saturating_add(duration_ms.max(0) as u64),
    AtomicOrdering::Relaxed,
);
buffer_state.producer_active.store(true, AtomicOrdering::Relaxed);
```

On chunk miss (consecutive_chunk_misses reaching a threshold, e.g. 15 = ~30s):
```rust
if consecutive_chunk_misses >= 15 {
    buffer_state.producer_active.store(false, AtomicOrdering::Relaxed);
}
```

- [ ] **Step 7: Replace the initial buffer-fill wait in endpoint_loop with rescue warmup**

The current `endpoint_loop` has a buffer-fill wait loop (lines 789-819). Replace it with rescue mode logic:

When `rescue_video_url` is `Some(url)` and `delivery_delay_ms > 0` and `!ep_cfg.is_fast`:
1. Start in `Rescue { reason: Warmup }` mode
2. Spawn rescue ffmpeg immediately
3. The buffer-fill wait still probes S3 but now runs ALONGSIDE the rescue ffmpeg
4. When buffer reaches `delivery_delay_ms`, kill rescue ffmpeg, transition to Normal

When `rescue_video_url` is `None`:
1. Keep the existing buffer-fill wait behavior (no rescue ffmpeg)

- [ ] **Step 8: Add rescue mode detection to consumer_task**

In `consumer_task`, add a 30-second timeout to the `rx.recv()` call. If the channel is empty for 30s AND `rescue_video_url` is Some AND the producer is stalled:

```rust
// Pull next chunk with 30s timeout for rescue detection
let chunk = tokio::select! {
    maybe_chunk = rx.recv() => {
        match maybe_chunk {
            Some(c) => c,
            None => {
                tracing::info!(alias = %alias, "Consumer: producer gone, stopping");
                break;
            }
        }
    }
    _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
        if rescue_video_url.is_some() && !buffer_state.producer_active.load(AtomicOrdering::Relaxed) {
            // Enter rescue mode
            // Kill current ffmpeg, spawn rescue ffmpeg
            // Wait for buffer to refill to RESCUE_REFILL_TARGET_SECS
            // Then kill rescue ffmpeg and continue normal loop
            // ... (see detailed implementation below)
        }
        continue;
    }
    _ = stop_rx.changed() => {
        if *stop_rx.borrow() { break; }
        continue;
    }
};
```

The rescue mode inner loop:
1. Kill current chunk ffmpeg (`proc.take()` → kill)
2. Spawn rescue ffmpeg via `Command::new("ffmpeg").args(build_rescue_ffmpeg_args(...))`
3. Update stats: `delivery_mode = "rescue"`, `rescue_eta_secs = Some(120)`
4. Write initial countdown file
5. Loop every 5s:
   a. Read `buffer_state.buffer_duration_ms`
   b. If buffer >= `RESCUE_REFILL_TARGET_SECS * 1000`: kill rescue ffmpeg, break to Normal
   c. Calculate ETA, update countdown file, update stats
   d. Check stop_rx
6. Clean up countdown file
7. Update stats: `delivery_mode = "normal"`, `rescue_eta_secs = None`
8. Reset `flv_normalizer` for fresh ffmpeg

- [ ] **Step 9: Update all callers of EndpointHandle::spawn**

In `api.rs` `init_endpoints` (line 173):
```rust
let handle = EndpointHandle::spawn(
    ep_cfg.clone(),
    s3_config.clone(),
    req.event_identifier.clone(),
    start_id,
    req.delivery_delay_ms,
    req.rescue_video_url.clone(),
);
```

In `api.rs` `add_endpoint` (line 343):
```rust
let rescue_video_url = state.rescue_video_url.read().await.clone();
let handle = EndpointHandle::spawn(
    req.endpoint.clone(),
    s3_config,
    event_identifier,
    start_id,
    delivery_delay_ms,
    rescue_video_url,
);
```

- [ ] **Step 10: Run tests**

Run: `cargo test -p rs-delivery -- --nocapture`
Expected: All existing tests pass. Some may need minor updates for the new `rescue_video_url` parameter (pass `None` in existing test calls).

- [ ] **Step 11: Commit**

```bash
git add crates/rs-delivery/src/endpoint_task.rs crates/rs-delivery/src/api.rs crates/rs-delivery/src/main.rs
git commit -m "feat: wire rescue mode state machine into endpoint delivery loop (#62)"
```

---

### Task 5: Orchestrator — Pass rescue_video_url to VPS

**Files:**
- Modify: `crates/rs-api/src/delivery.rs:430-455` (init_body construction)

- [ ] **Step 1: Write failing test**

In `crates/rs-api/src/router_tests.rs`, add:

```rust
#[tokio::test]
async fn update_event_rescue_video_url() {
    let state = test_state().await;
    db::create_streaming_event(&state.pool, "rescue-url-test")
        .await
        .unwrap();
    let events = db::list_streaming_events(&state.pool).await.unwrap();
    let id = events[0].id;
    let app = build_router(state.clone());

    let resp = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .method("PATCH")
                .uri(&format!("/api/v1/events/{id}"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(
                    serde_json::json!({
                        "rescue_video_url": "https://example.com/rescue.mp4"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let evt = db::get_streaming_event_by_id(&state.pool, id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        evt.rescue_video_url,
        Some("https://example.com/rescue.mp4".to_string())
    );
}
```

- [ ] **Step 2: Add rescue_video_url to init_body in delivery.rs**

In `crates/rs-api/src/delivery.rs`, in the `poll_and_init` method, where `init_body` is constructed (around line 430), add `rescue_video_url` to the JSON:

```rust
let init_body = serde_json::json!({
    "endpoints": endpoints.iter().map(|ep| {
        let ep_start = resume_pos.as_ref()
            .and_then(|rp| rp.get(&ep.alias).copied())
            .unwrap_or(start_chunk_id);
        serde_json::json!({
            "alias": ep.alias,
            "service_type": ep.service_type,
            "stream_key": ep.stream_key,
            "is_fast": ep.is_fast,
            "chunk_format": chunk_format,
            "start_chunk_id": ep_start,
        })
    }).collect::<Vec<_>>(),
    "s3_config": {
        "bucket": self.config.s3.bucket,
        "region": self.config.s3.region,
        "endpoint": self.config.s3.endpoint,
        "access_key_id": "from-env",
        "secret_access_key": "from-env",
    },
    "event_identifier": event_name,
    "start_chunk_id": start_chunk_id,
    "delivery_delay_ms": target_delay_ms,
    "rescue_video_url": event.rescue_video_url,
});
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p rs-api -- --nocapture`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add crates/rs-api/src/delivery.rs crates/rs-api/src/router_tests.rs
git commit -m "feat: orchestrator passes rescue_video_url to VPS init (#62)"
```

---

### Task 6: Frontend — Rescue Video URL in Event Settings

**Files:**
- Modify: `leptos-ui/src/api.rs` (UpdateEventRequest, DeliveryEndpointResponse, ListEvent)
- Modify: `leptos-ui/src/ws.rs` (WsEndpointMetrics)
- Modify: `leptos-ui/src/store.rs` (EndpointData)
- Modify: `leptos-ui/src/components/settings.rs` (rescue video URL input)
- Modify: `leptos-ui/src/components/operator_dashboard.rs` (delivery mode badge)
- Modify: `e2e/mock-api.js` (add rescue_video_url to mock responses)

- [ ] **Step 1: Add rescue_video_url to frontend API types**

In `leptos-ui/src/api.rs`:

Add to `UpdateEventRequest` (around line 546):
```rust
#[serde(skip_serializing_if = "Option::is_none")]
pub rescue_video_url: Option<String>,
```

Add to whichever struct represents events in the list (search for `cache_delay_secs` in api.rs to find the `ListEvent` or equivalent struct):
```rust
#[serde(default)]
pub rescue_video_url: Option<String>,
```

Add to `DeliveryEndpointResponse` (around line 570):
```rust
#[serde(default)]
pub delivery_mode: Option<String>,
#[serde(default)]
pub rescue_eta_secs: Option<u64>,
```

- [ ] **Step 2: Add delivery_mode to WS and store types**

In `leptos-ui/src/ws.rs`, add to the WsEndpointMetrics struct (after `is_fast`):
```rust
#[serde(default)]
delivery_mode: Option<String>,
#[serde(default)]
rescue_eta_secs: Option<u64>,
```

And in the mapping where `EndpointData` is constructed from WS data, pass through:
```rust
delivery_mode: ep.delivery_mode.clone(),
rescue_eta_secs: ep.rescue_eta_secs,
```

In `leptos-ui/src/store.rs`, add to `EndpointData` (after `is_fast`):
```rust
pub delivery_mode: Option<String>,
pub rescue_eta_secs: Option<u64>,
```

- [ ] **Step 3: Add rescue video URL input to event settings**

In `leptos-ui/src/components/settings.rs`, find the event settings section where `cache_delay_secs` input is rendered. Add a similar input for rescue_video_url:

```rust
// Rescue video URL input
<div class="setting-row">
    <label>"Rescue Video URL"</label>
    <input
        type="text"
        placeholder="https://s3.example.com/rescue-video.mp4"
        prop:value=move || {
            store.events_list.get().iter()
                .find(|e| e.id == id)
                .and_then(|e| e.rescue_video_url.clone())
                .unwrap_or_default()
        }
        on:change=move |ev| {
            let val = event_target_value(&ev);
            let url = if val.trim().is_empty() { None } else { Some(val) };
            let eid = id;
            spawn_local(async move {
                let req = api::UpdateEventRequest {
                    rescue_video_url: url,
                    ..Default::default()
                };
                let _ = api::update_event(eid, &req).await;
                if let Ok(events) = api::list_events().await {
                    store.events_list.set(events);
                }
            });
        }
    />
</div>
```

- [ ] **Step 4: Add delivery mode badge to endpoint cards**

In `leptos-ui/src/components/operator_dashboard.rs`, in the endpoint card rendering (around line 560-580), add a delivery mode badge:

After the existing stall_reason display (around line 631):

```rust
{move || {
    let ep = ep_data.get();
    ep.delivery_mode.clone().and_then(|mode| {
        let (badge_class, label) = match mode.as_str() {
            "warmup" => ("endpoint-mode-warmup", "WARMUP"),
            "rescue" => ("endpoint-mode-rescue", "RESCUE"),
            "recovering" => ("endpoint-mode-recovering", "RECOVERING"),
            _ => return None,
        };
        let eta = ep.rescue_eta_secs.map(|s| {
            if s >= 60 { format!(" ~{}m {}s", s / 60, s % 60) }
            else { format!(" ~{s}s") }
        }).unwrap_or_default();
        Some(view! {
            <span class=badge_class>{format!("{label}{eta}")}</span>
        })
    })
}}
```

- [ ] **Step 5: Add CSS for delivery mode badges**

In the stylesheet (find via grep for `endpoint-anomaly` CSS class), add:

```css
.endpoint-mode-warmup {
    background: #3b82f6;
    color: white;
    padding: 2px 6px;
    border-radius: 4px;
    font-size: 0.75rem;
    margin-left: 8px;
}
.endpoint-mode-rescue {
    background: #f59e0b;
    color: white;
    padding: 2px 6px;
    border-radius: 4px;
    font-size: 0.75rem;
    margin-left: 8px;
}
.endpoint-mode-recovering {
    background: #eab308;
    color: white;
    padding: 2px 6px;
    border-radius: 4px;
    font-size: 0.75rem;
    margin-left: 8px;
}
```

- [ ] **Step 6: Update mock-api.js**

In `e2e/mock-api.js`, add `rescue_video_url: null` to mock event responses and `delivery_mode: "normal"`, `rescue_eta_secs: null` to mock delivery endpoint responses.

- [ ] **Step 7: Run cargo fmt**

```bash
cargo fmt --all --check
```

- [ ] **Step 8: Commit**

```bash
git add leptos-ui/src/api.rs leptos-ui/src/ws.rs leptos-ui/src/store.rs leptos-ui/src/components/settings.rs leptos-ui/src/components/operator_dashboard.rs e2e/mock-api.js
git commit -m "feat: dashboard rescue video config + delivery mode badges (#62)"
```

---

### Task 7: E2E Test — Rescue Video URL Configuration

**Files:**
- Modify: `e2e/frontend.spec.ts`

- [ ] **Step 1: Add E2E test for rescue video URL setting**

Add to `e2e/frontend.spec.ts`:

```typescript
test('rescue video URL can be set on event', async ({ page }) => {
  const consoleMessages: string[] = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error' || msg.type() === 'warning') {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.goto('/');
  // Navigate to settings
  await page.click('[data-testid="settings-link"]');
  await page.waitForSelector('.event-settings');

  // Find the rescue video URL input
  const rescueInput = page.locator('input[placeholder*="rescue"]');
  await expect(rescueInput).toBeVisible();

  // Set a rescue video URL
  await rescueInput.fill('https://example.com/rescue.mp4');
  await rescueInput.press('Tab'); // trigger change event

  // Wait for save
  await page.waitForTimeout(1000);

  // Reload and verify persisted
  await page.reload();
  await page.waitForSelector('.event-settings');
  const value = await page.locator('input[placeholder*="rescue"]').inputValue();
  expect(value).toBe('https://example.com/rescue.mp4');

  expect(consoleMessages).toEqual([]);
});
```

- [ ] **Step 2: Run E2E test locally to verify**

```bash
cd e2e && npx playwright test --config=playwright-frontend.config.ts -g "rescue video"
```

- [ ] **Step 3: Commit**

```bash
git add e2e/frontend.spec.ts
git commit -m "test: E2E test for rescue video URL configuration (#62)"
```

---

### Task 8: Rescue Mode Unit Tests

**Files:**
- Modify: `crates/rs-delivery/src/rescue_tests.rs` (add more tests)
- Create: `crates/rs-delivery/src/endpoint_task_rescue_tests.rs`

- [ ] **Step 1: Add state machine transition tests**

Create `crates/rs-delivery/src/endpoint_task_rescue_tests.rs`:

```rust
//! Tests for rescue mode state machine in the endpoint delivery loop.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::Mutex as TokioMutex;

// Reuse MockFetcher and MockProcess from endpoint_task_tests.rs
// (the test modules share the same super:: scope)

/// Helper: Create a MockFetcher with delayed chunks — chunks appear after a delay.
struct DelayedMockFetcher {
    chunks: Arc<TokioMutex<std::collections::HashMap<i64, Vec<u8>>>>,
    duration_ms_per_chunk: i64,
    /// Chunks below this ID are available immediately; above are delayed.
    available_up_to: Arc<AtomicI64>,
}

impl ChunkFetcher for DelayedMockFetcher {
    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        let up_to = self.available_up_to.load(Ordering::Relaxed);
        if chunk_id > up_to {
            return Ok(None);
        }
        let map = self.chunks.lock().await;
        Ok(map
            .get(&chunk_id)
            .map(|data| (data.clone(), self.duration_ms_per_chunk)))
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        let up_to = self.available_up_to.load(Ordering::Relaxed);
        if chunk_id > up_to {
            return Ok(None);
        }
        let map = self.chunks.lock().await;
        if map.contains_key(&chunk_id) {
            Ok(Some(self.duration_ms_per_chunk))
        } else {
            Ok(None)
        }
    }
}

#[tokio::test]
async fn endpoint_stats_start_in_warmup_mode() {
    let stats: Stats = Arc::new(Mutex::new(EndpointStats {
        current_chunk_id: 1,
        delivery_mode: "warmup".to_string(),
        ..Default::default()
    }));
    let s = stats.lock().await;
    assert_eq!(s.delivery_mode, "warmup");
}

#[tokio::test]
async fn buffer_state_tracks_duration() {
    let bs = BufferState::new();
    assert_eq!(bs.buffer_duration_ms.load(Ordering::Relaxed), 0);
    bs.buffer_duration_ms.store(5000, Ordering::Relaxed);
    assert_eq!(bs.buffer_duration_ms.load(Ordering::Relaxed), 5000);
}

#[tokio::test]
async fn fast_endpoints_skip_rescue_mode() {
    // Fast endpoints have delivery_delay_ms = 0, so they never enter warmup
    let stats: Stats = Arc::new(Mutex::new(EndpointStats {
        current_chunk_id: 1,
        delivery_mode: "normal".to_string(),
        ..Default::default()
    }));
    let s = stats.lock().await;
    // Fast endpoints go straight to "normal"
    assert_eq!(s.delivery_mode, "normal");
}
```

- [ ] **Step 2: Add test module declaration in endpoint_task.rs**

At the bottom of `endpoint_task.rs`, after the existing test modules:
```rust
#[cfg(test)]
#[path = "endpoint_task_rescue_tests.rs"]
mod rescue_tests;
```

- [ ] **Step 3: Run tests**

Run: `cargo test -p rs-delivery rescue -- --nocapture`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add crates/rs-delivery/src/endpoint_task_rescue_tests.rs crates/rs-delivery/src/endpoint_task.rs
git commit -m "test: add rescue mode state machine unit tests (#62)"
```

---

### Task 9: CI Updates — Mutation Testing Exclusions

**Files:**
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Add mutation testing exclusions for rescue module**

The rescue module interacts with external processes (ffmpeg) and the filesystem (countdown files). Add exclusions similar to the delivery orchestrator methods:

Find the `cargo mutants` command in ci.yml and add:
```yaml
--exclude-re "rescue::" \
--exclude-re "endpoint_url_for_service" \
```

- [ ] **Step 2: Add rescue_video_url to CI GATE**

If there's a CI GATE step that tests the delivery endpoint, add `rescue_video_url` to the mock init payload.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add rescue module mutation testing exclusions (#62)"
```

---

### Task 10: Push, Monitor CI, Create PR

- [ ] **Step 1: Run local checks**

```bash
cargo fmt --all --check
```

- [ ] **Step 2: Push to dev**

```bash
git push origin dev
```

- [ ] **Step 3: Monitor CI**

```bash
gh run list --limit 3
# Wait for terminal state
gh run view <run-id>
```

All jobs must pass. If any fail, investigate with `gh run view <run-id> --log-failed`, fix, and push again.

- [ ] **Step 4: Create PR**

```bash
gh pr create --title "feat: buffer rescue mode — looped video during outage (#62)" --body "$(cat <<'EOF'
## Summary
- Add rescue mode to VPS delivery: when buffer empties, play a looped rescue video with countdown overlay
- Rescue video also plays during initial warmup (buffer filling)
- Per-event rescue_video_url configured in dashboard settings
- Dashboard shows delivery mode badges (WARMUP/RESCUE/RECOVERING)
- Fixed 120s buffer refill target before resuming normal delivery
- ffmpeg drawtext filter with reload=1 for seamless countdown updates

Closes #62

## Test plan
- [ ] Unit tests: rescue module (ffmpeg args, countdown format, state types)
- [ ] Unit tests: state machine transitions (warmup/normal/rescue/recovering)
- [ ] Unit tests: buffer state tracking
- [ ] E2E: rescue video URL persistence in event settings
- [ ] CI: all jobs green including mutation testing
- [ ] Manual: verify rescue video plays during initial warmup
- [ ] Manual: verify rescue → normal transition when buffer fills

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Monitor PR CI**

All jobs must pass.

- [ ] **Step 6: Verify PR is mergeable**

```bash
gh api repos/zbynekdrlik/restreamer/pulls/NUMBER --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

---

## Verification

1. **Unit tests:** rescue module tests + state machine tests pass
2. **E2E:** rescue video URL configured and persisted via dashboard
3. **CI:** all jobs green (lint, test, build, E2E, mutation testing)
4. **Dashboard:** delivery mode badges visible on endpoint cards
5. **Init payload:** rescue_video_url flows from DB → orchestrator → VPS
6. **Status API:** delivery_mode and rescue_eta_secs in VPS status response
