# Pure-Rust RTMP Push Design (#103)

**Status:** Approved (brainstorming complete 2026-04-27).
**Issue:** [#103 — Replace ffmpeg subprocess with pure-Rust RTMP push](https://github.com/zbynekdrlik/restreamer/issues/103)
**Author:** Claude (CAWE) + Zbynek Drlik
**Scope:** Single-feature spec, 4-PR rollout. PR boundaries enumerated in §6.

---

## 1. Problem

`rs-delivery` pushes S3 chunks to YouTube/Facebook by spawning an external `ffmpeg` subprocess (`-c copy -f flv ${url}`). After 9.5 hours of overnight streaming on 2026-04-10, the dashboard reported:

| Endpoint | chunks | cache | ffmpeg restarts |
|---|---|---|---|
| YT NLCH 4K | 17252 | 262s | 30 |
| FB-Zbynek | 16890 | 0s | **524** |
| FB-NewLevel | 16943 | 0s | **471** |
| Kiko | 17249 | 262s | 33 |
| Control Stream SNV | 17355 | 32s | 43 |
| YT NLW 4k | 17245 | 262s | 37 |
| YT KS-BB 4K | 17252 | 262s | 30 |

**Root cause:** Each ffmpeg restart resets the published RTMP timestamp (PTS) to 0. The xiu RTMP server feeding chunks uses absolute timestamps that grow forever, so the new ffmpeg sees a "9.5h-behind real time" stream and races as fast as the upstream will accept until it catches up. That race manifests as the catch-up storm and triggers more upstream rejections, which trigger more restarts. FB-Zbynek hit a stale stream-key cliff that ffmpeg surfaces only via stderr, so the system never escalated and quietly burned 471 restarts over hours.

**Cascading symptoms** all trace to the same root:

- Cache overshoot (#133): cache settled at 262s instead of 120s target — feedback loop is impossible across an external process boundary.
- "ffmpeg stdin closed" errors: ~1 per 15-20 min on every endpoint, opaque cause, only visible via stderr.
- Slow dead-endpoint detection: FB stream-key invalidation went undetected for 9.5h.
- Divergent chunk counts: FB consumers advance faster than YT because failed-write chunks are skipped instantly with no upstream feedback.

## 2. Goal

Replace the ffmpeg subprocess in the delivery binary with an in-process, pure-Rust RTMP push pipeline. After full rollout (PR 4):

- No ffmpeg subprocess in the delivery binary.
- No `ffmpeg restart` counter; replaced by `reconnect_count` (network-driven only).
- Cache delay holds at target ±5s for the entire duration of a multi-hour stream.
- Stale stream keys (FB invalidation, YT key rotation) are detected as `PublishRejected { code: "NetStream.Publish.BadName" }` within 60s, surfaced to the dashboard, recorded in the audit log.
- 4-hour autonomous-agent soak on stream.lan with full multi-endpoint setup completes with `reconnect_count` in single digits (network-driven only) per endpoint. The agent (CAWE) is the operator: it drives OBS via MCP, watches the dashboard via Playwright, captures audit-log evidence, and posts the PR comment.
- Media payload (H.264 NALUs and AAC frames) is byte-identical to source FLV across the wire.

## 3. Non-Goals

- Transcoding. The pipeline is `-c copy` today; the new pipeline is also passthrough.
- Replacing ffmpeg as the FLV-tag parser. We use the existing `xflv` crate (already a transitive dep through xiu) for FLV demux of incoming chunks.
- Replacing xiu on the input side. xiu RTMP server in `rs-inpoint` stays.
- New transport protocols (SRT, HLS push, WebRTC). Stays RTMP/RTMPS.

## 4. Architecture

```
TODAY:
S3 chunk → flv_normalizer → ffmpeg subprocess (-c copy -f flv) → RTMP destination
                                ↑ stderr parsing, restart counter, race-to-catch-up

AFTER PR 1 (per-endpoint switch via config):
                          ┌─→ FfmpegProcess (legacy)            ─→ RTMP
S3 chunk → flv_normalizer ┤
                          └─→ RtmpPusher (rs-rtmp-push, new)    ─→ RTMP
                              ↑ in-process, monotonic TS, typed errors

AFTER PR 4 (ffmpeg removed):
S3 chunk → flv_normalizer → RtmpPusher → RTMP destination
```

### 4.1 New crate: `crates/rs-rtmp-push`

Single responsibility: take a stream of FLV bytes plus an output URL, push to the destination RTMP server, signal back when the connection drops with a typed error.

Public API (kept tight):

```rust
pub struct RtmpPusher {
    url: String,
    config: PusherConfig,
    state: PusherState,
}

pub struct PusherConfig {
    pub timeout_ms: u64,                 // default 30000
    pub backoff: BackoffSchedule,        // see §5.3
}

pub enum PushEvent {
    Connected,
    BytesPushed { tag_count: u32, last_ts_ms: u64 },
    Reconnecting { reason: PushError, after: Duration },
    Stopped { reason: PushError },
}

pub enum PushError {
    HandshakeFailed(io::Error),
    ConnectRejected { code: String, description: String },
    PublishRejected { code: String, description: String },
    RemoteClosed(io::Error),
    Timeout,
    IoError(io::Error),
    LocalCancel,
}

impl RtmpPusher {
    pub fn new(url: String, config: PusherConfig) -> Self;
    pub async fn push_flv_bytes(&mut self, bytes: &[u8]) -> Result<(), PushError>;
    pub fn last_output_ts_ms(&self) -> u64;
    pub fn reconnect_count(&self) -> u32;
    pub async fn close(&mut self);
    pub fn events(&mut self) -> tokio::sync::mpsc::Receiver<PushEvent>;
}
```

Internally, `RtmpPusher` owns a `xiu::rtmp::session::client_session::ClientSession` configured with `ClientSessionType::Push` and a `xiu::rtmp::chunk::packetizer::ChunkPacketizer` for outbound RTMP chunk packetization. The handshake/connect/createStream/publish state machine is xiu's; we drive it through `ClientSession::run()`.

### 4.2 Data flow inside `RtmpPusher::push_flv_bytes`

1. Parse incoming FLV bytes into `FlvTag` values via `xflv::demuxer`. Each S3 chunk is a self-contained FLV file (header + AAC/AVC sequence headers + media tags).
2. For each tag, rewrite `tag.timestamp_ms = state.last_output_ts_ms + (tag.timestamp_ms - chunk_first_tag_ts)`. Update `state.last_output_ts_ms = tag.timestamp_ms` after rewrite.
3. Hand each tag to xiu's session: `session.publish_tag(tag).await`, which packetizes via `ChunkPacketizer` and writes to the TCP socket.
4. On any I/O error, classify into `PushError`, set `state.session = None`, return `Err(PushError)`. The caller (endpoint task) treats this exactly as it treats `FfmpegProcess` death today: emit audit record, sleep backoff, retry.

### 4.3 Integration with `rs-delivery::endpoint_task`

`endpoint_task.rs` consumer task pulls a `PrefetchedChunk` from the same channel as today, runs `flv_normalizer.normalize()`, then branches:

```rust
match endpoint.config.pusher {
    PusherKind::Ffmpeg => ffmpeg_process.write(&normalized_bytes).await,
    PusherKind::Rust => rtmp_pusher.push_flv_bytes(&normalized_bytes).await,
}
```

Both paths surface their errors as the same shape (a single `Result<(), EndpointPushError>`) so the existing reconnect/audit/backoff logic above the branch is unchanged.

## 5. The Fix (reconnect + monotonic timestamps)

### 5.1 Where state lives

**Inside the pusher** (per `RtmpPusher` instance):

```rust
struct PusherState {
    last_output_ts_ms: u64,          // monotonic across reconnects, never resets
    reconnect_count: u32,             // surfaced as dashboard metric
    session: Option<ClientSession>,   // None while between connections
}
```

**Inside the caller** (per endpoint task in `rs-delivery::endpoint_task`):

```rust
// Same fields the endpoint task owns today for ffmpeg restarts. Reused for the rust pusher.
consecutive_errors: u32,             // for class-aware backoff
last_error_class: Option<PushErrorKind>,  // resets when class changes
last_clean_run_started_at: Instant,  // for the 60s reset rule
```

The pusher owns transport-level state (session + monotonic TS). The caller owns retry-policy state (backoff + class history). This mirrors today's split between `FfmpegProcess` (transport) and `EndpointRestartState` (retry policy).

### 5.2 The actual loop (caller-driven, mirrors today's ffmpeg path)

```
endpoint task loop:
1. Call pusher.push_flv_bytes(&bytes).
   - On first call (state.session == None): pusher internally opens TCP, runs handshake,
     sends NetConnection.connect, sends createStream, sends publish(stream_key, "live"),
     and on success caches the session. Then writes tags. Returns Ok.
   - On subsequent calls with cached session: pusher just rewrites timestamps and writes tags.
2. For every tag written, pusher does:
     tag.timestamp = state.last_output_ts_ms + (tag.timestamp - chunk_first_tag_ts)
     state.last_output_ts_ms = tag.timestamp
   Packetize via xiu ChunkPacketizer, write to socket.
3. On any error inside pusher (handshake, connect rejection, publish rejection, socket error,
   timeout): pusher classifies into PushError, drops state.session = None,
   increments state.reconnect_count, returns Err(PushError).
4. Caller catches the error, emits audit record, sleeps backoff(error_class, consecutive_errors)
   per §5.3, retries the loop.
5. If pusher.push_flv_bytes returned Ok and we've been Ok for ≥ 60s since the last error,
   caller resets consecutive_errors = 0 (mirrors today's `endpoint_task.rs:511` rule).
```

**Why this fixes the bug:** Each ffmpeg restart today sends a fresh `publish` with PTS=0, but the upstream has already received PTS up to (say) 9,500,000ms. The client's `-re` rate limiter now thinks it's 9,500,000ms behind real time and pushes as fast as the upstream will accept until it catches up. With monotonic output TS, upstream sees a continuous stream, no client-side catch-up, and a single dropped TCP connection costs ~1 reconnect, not a chain reaction.

### 5.3 Error classification table

| `PushError` variant | Backoff floor | Behavior |
|---|---|---|
| `HandshakeFailed` | 5s | upstream maybe up but slow, retry quickly |
| `ConnectRejected` | 30s, exp ×2, cap 300s | auth-ish, slow down |
| `PublishRejected { code: "NetStream.Publish.BadName" }` | 30s fixed | invalid stream key — operator must fix; do NOT escalate exponential, but flag in audit log at severity=error |
| `PublishRejected` (other code) | 30s, exp ×2, cap 300s | unexpected upstream rejection |
| `RemoteClosed` | 30s, exp ×2, cap 300s | matches today's YoutubeRtmpClosed/RemoteBrokenPipe |
| `Timeout` | 10s fixed | matches today's NetworkTimeout |
| `IoError` | 15s fixed | matches today's Unknown |
| `LocalCancel` | n/a | we initiated, no retry |

`consecutive_errors` resets to 0 when (a) class changes, (b) connection survives ≥ 60s. Exponential factor of 2, capped at 300s, mirrors today's `ffmpeg_reason::reconnect_floor()` behavior.

### 5.4 Dead-endpoint detection

`PublishRejected { code: "NetStream.Publish.BadName" }` is the proper signal for "stream key invalid". On first occurrence, emit a structured audit record:

```rust
AuditEvent::EndpointDead {
    endpoint_name: String,
    reason: "invalid_stream_key",
    push_error_code: String,        // "NetStream.Publish.BadName"
    push_error_description: String, // upstream-provided reason
    severity: Severity::Error,
}
```

The dashboard reads this audit variant and surfaces a red banner per dead endpoint. The endpoint is NOT auto-disabled (operator decision: rotating the stream key is operator work).

### 5.5 Audit log

`AuditEvent::RtmpPush { ts, endpoint_name, reconnect_count, last_error: Option<PushError>, last_output_ts_ms, lifetime_ms, backoff_ms }` is added as a new variant on the existing audit log enum. Dashboard `ffmpeg_restart_count` is replaced by `reconnect_count` in the per-endpoint stats.

## 6. Migration plan (4 PRs)

| PR | Title | Scope | Behavior change | Validation |
|---|---|---|---|---|
| **PR 1** | `feat: add rs-rtmp-push crate (#103)` | New crate `rs-rtmp-push` with `RtmpPusher`, `PushError`, audit variant. New `PusherKind` config field on Endpoint with `#[serde(default)]` and `#[default] Ffmpeg`. Endpoint task branches on `endpoint.config.pusher`. Default = Ffmpeg. Unit tests + RTMP integration test against a local xiu RtmpServer. Playwright E2E with one endpoint switched to `pusher: "rust"`. | None on existing endpoints. Existing `config.json` files parse and behave unchanged. | CI green, all existing tests pass, new crate's unit + integration tests pass, existing `e2e-obs-youtube-test` flipped to `pusher: "rust"` passes. |
| **PR 2** | `chore: flip FB-Zbynek to Rust pusher (#103)` | Single-line config change in stream.lan deploy script: set `pusher: "rust"` on FB-Zbynek endpoint only. No code changes. | FB-Zbynek uses new pusher. All other endpoints unchanged (still on ffmpeg). | **Agent (CAWE) runs an autonomous 4h+ soak** on stream.lan via MCP + Playwright (see §7.5). Acceptance: FB-Zbynek `reconnect_count` ≤ 5, no `PublishRejected`, FB Studio reports stream healthy throughout, no race-to-catch-up. Agent posts captured data as a comment on PR 3 before requesting merge of PR 3. |
| **PR 3** | `feat: flip remaining endpoints to Rust pusher (#103)` | Config change setting `pusher: "rust"` on all other endpoints. ffmpeg path remains in code so we can flip back if any endpoint misbehaves. | All endpoints use new pusher. ffmpeg subprocess no longer spawned. | Agent runs another autonomous 4h+ soak covering all endpoints. Same acceptance criteria as PR 2 but matrix-wide. |
| **PR 4** | `chore: delete rs-ffmpeg crate (#103)` | Delete `crates/rs-ffmpeg`, the FfmpegProcess code path, the `PusherKind` enum (replaced by always-Rust), the `ffmpeg_restart_count` field (replaced by `reconnect_count`), ffmpeg-specific audit variants. Update deploy scripts to no longer require ffmpeg.exe. | Configs with `"pusher": "ffmpeg"` now fail to parse with a startup error pointing at this PR's release notes. | CI green, dashboard shows new metric names, fresh stream.lan install no longer needs ffmpeg.exe. **PR 4 must NOT be merged until ≥ 2 weeks of clean operation under PR 3, including at least one live event.** |

### 6.1 Rollback paths

- After PR 2: revert the config change on stream.lan, FB-Zbynek goes back to ffmpeg without redeploy if config hot-reload covers Endpoint changes (verify in PR 1 testing; if not, a quick redeploy is acceptable).
- After PR 3: same — flip per-endpoint config and rs-delivery picks ffmpeg again on next session.
- After PR 4: only path back is `git revert`. This is why PR 4 requires the 2-week soak.

## 7. Testing

### 7.1 Unit tests (`crates/rs-rtmp-push/src/`)

Run on every push, fast.

| Test | What it verifies |
|---|---|
| `handshake_completes_with_local_xiu_server` | Spin up xiu `RtmpServer` on `127.0.0.1:0`; push handshake-only; assert server-side session reaches connected state. |
| `publish_rejected_on_invalid_stream_key` | xiu test server returns `NetStream.Publish.BadName`; pusher surfaces `PushError::PublishRejected { code: "NetStream.Publish.BadName" }` within 5s. |
| `monotonic_ts_across_reconnect` | Push 2 chunks (TS 0..1000, 1000..2000), kill connection mid-flight, reconnect, push 1 more chunk (chunk-internal TS 0..1000). Assert wire-captured tag stream has TS 0, 1000, 2000... continuing past 2000, never resetting. |
| `media_payload_byte_identical_to_source` | Source FLV file → push → capture wire bytes → extract VIDEODATA + AUDIODATA bodies → SHA256 == source FLV's tag bodies. |
| `backoff_exponential_on_remote_close` | Mock TCP close repeatedly; assert sleep durations match floor table (5, 10, 20, 40, 80, 160, 300, 300s — capped at 300s, per §5.3). |
| `consecutive_errors_resets_after_60s_clean_run` | Survive 60s, force a class change, assert `consecutive_errors == 0`. |
| `audit_record_emitted_per_reconnect` | Each reconnect produces an audit record with reason class, lifetime, last_output_ts_ms. |
| `local_cancel_does_not_retry` | `pusher.close()` returns immediately, no reconnect attempt. |
| `bad_name_logs_severity_error_once` | First `PublishRejected { code: "BadName" }` logs at severity=error; subsequent identical errors within the same session log at severity=warn (deduplication). |

### 7.2 Integration test (`crates/rs-rtmp-push/tests/local_xiu_loopback.rs`)

Spin up xiu `RtmpServer` on a random port, the test harness acts as both publisher (`RtmpPusher`) and subscriber (raw FLV recorder). Push the existing 30s test FLV from `e2e/test-data/`, record what comes out the other side, assert byte-for-byte media-payload equality. Same harness used for the monotonic-TS test.

### 7.3 Playwright E2E (`e2e/rust-pusher.spec.ts`)

Existing `e2e-streaming-test` harness, one endpoint switched to `pusher: "rust"`. Asserts: dashboard shows `chunks_processed > 0` within 90s of init, `reconnect_count == 0` over the 5-min test window, audit log has at least one `BytesPushed` event.

### 7.4 E2E OBS-to-YouTube (`ci.yml :: e2e-obs-youtube-test`)

Existing job, no structural changes. PR 1 flips the test config's pusher to `rust`. Acceptance same as today's gate (init within 180s, `chunks_processed > 0` within 90s, YouTube reception verified via `liveStreams.list` `streamStatus == "active"`).

### 7.5 Autonomous-agent soak (gate for PR 2 → PR 3, and PR 3 → PR 4)

The agent (CAWE) is the operator. The user does NOT run this — autonomous-verification rule (`~/devel/airuleset/modules/core/autonomous-verification.md`) applies. Documented as a runbook step in `docs/operator-runbook.md` so the procedure is auditable, but the agent executes it.

**Pre-flight (the agent does these before starting):**

1. Confirm no live event is in progress on stream.lan. Live events are checked via the dashboard `event_status` field; if `LIVE`, the agent waits and retries — soak NEVER preempts a live event (`feedback_no_deploy_during_live` memory).
2. Confirm a soak window of ≥ 5 hours is available before the next scheduled live event. If not, the agent waits for a free window and reports the wait time.
3. Confirm OBS is reachable via MCP (`mcp__win-stream-snv__ListProcesses` filter "obs64") and OBS WebSocket responds.
4. Confirm Hetzner VPS quota allows spinning up the delivery instance for the soak duration.

**Run (autonomous):**

1. Edit stream.lan `C:\ProgramData\Restreamer\config.json` via MCP (`mcp__win-stream-snv__FileWrite`) to set `pusher: "rust"` on the target endpoint(s) — FB-Zbynek for PR 2, all endpoints for PR 3.
2. Restart `Restreamer.exe` on stream.lan via MCP (graceful taskkill + Start-ScheduledTask `RestreamerGUI`). Verify the process comes back in user session, dashboard returns 200 within 30s.
3. Start OBS streaming via MCP (`mcp__win-stream-snv__App` to launch OBS) and the OBS WebSocket MCP server (`mcp__win-stream-snv__Shell` invoking obs-websocket commands or the OBS MCP server directly per `reference_obs_mcp_server` memory).
4. Open the dashboard in Playwright (`mcp__plugin_playwright_playwright__browser_navigate` to the streamsnv URL).
5. **For 4h+ continuously**: every 5 minutes, take a Playwright snapshot of the dashboard, read per-endpoint `reconnect_count` and `chunks_processed`, query `/api/v1/audit?event_id=<soak-event>&limit=500` for new audit records since last poll, append all data to a soak log file under `/tmp/soak-<timestamp>/`. Log file is structured JSON, one record per poll.
6. **Watchdogs (any of these → abort and surface the failure):**
   - Any endpoint shows `reconnect_count > 10` cumulative.
   - Any endpoint emits `PushError::PublishRejected` with code `NetStream.Publish.BadName`.
   - Dashboard shows BUFFERING > 60s on any endpoint (catch-up storm regression).
   - FB/YT Studio (queried via existing `/api/v1/youtube/status` and the equivalent FB query) reports `streamStatus != "active"` for > 2 minutes.
7. Stop OBS via MCP, stop Restreamer, revert config to `pusher: "ffmpeg"` (return stream.lan to safe state regardless of soak outcome).

**Post-run (autonomous):**

1. Aggregate the soak log into a single Markdown report: per-endpoint reconnect_count, audit-log summary, dashboard screenshots at start/middle/end, total duration, watchdog events (if any).
2. Post the Markdown report as a comment on the next PR (PR 3 for the FB-Zbynek soak, PR 4 prerequisite for the all-endpoints soak).
3. **If acceptance fails**, do NOT merge the next PR. Open a new issue describing the regression, file it under repo, and revert the config change. The rollout pauses until the regression is fixed.

**Acceptance criteria** (per soak window):

- Target endpoint(s) `reconnect_count` ≤ 5 (single-digit, network-driven only).
- No `PushError::PublishRejected` events.
- FB/YT Studio shows `streamStatus = "active"` for ≥ 99.5% of the soak duration.
- No catch-up storms (cache delay never exceeds target by > 30s).
- Audit log clean (no `endpoint_dead` events, no severity=error from rs-rtmp-push).

### 7.6 Mutation testing

`rs-rtmp-push` enters the `cargo-mutants` matrix from day 1 — no `--exclude-re` opt-out. The unit tests above are designed to kill obvious mutations (`>` vs `>=`, off-by-one on TS rewrite, wrong floor mapping). If mutants survive, tighten assertions same as we did for `rescue_tests.rs` in PR #105.

### 7.7 Out of scope for this spec's tests

- 4-hour unattended CI soak — too long, too expensive. Agent-driven soak (§7.5) is the gate.
- Cross-endpoint stress (7 endpoints simultaneously, 4h+) — same reason. Agent-driven soak covers it.

## 8. Open risks

- **xiu RTMP client side under real load:** xiu's binary uses `relay::push_client::PushClient` for relaying RTMP, but the client session has had less production exposure than the server side. Mitigation: PR 2 soak on FB-Zbynek (worst-affected endpoint) before flipping anything else.
- **AMF status code variability:** YouTube and FB may return different `code` strings for "stream key invalid" than xiu's local test server. Mitigation: the audit record logs the raw upstream code/description, so the operator sees the actual string. Backoff is keyed on `code.starts_with("NetStream.Publish.")` so we tolerate variants.
- **Hot-reload of `pusher` field:** existing config hot-reload may not pick up Endpoint changes. Verify in PR 1 testing; if not supported, the rollback path is "edit config + restart rs-delivery", which costs ~10s of downtime.
- **xflv demux of malformed chunks:** if a chunk is corrupted (S3 partial fetch), today ffmpeg complains via stderr and we restart. The new path needs to surface `xflv` parse errors as `PushError::IoError` (or a new `PushError::MalformedInput`) with the audit record showing the offset/byte. Mitigation: existing chunk-integrity checks in `flv_normalizer` already cover this; we inherit them.

## 9. Dashboards & metrics impact

The `ffmpeg_restart_count` field on `EndpointStats` is replaced by `reconnect_count` in PR 4. PRs 1-3 expose BOTH fields:
- `ffmpeg_restart_count` (legacy, only meaningful for endpoints on `pusher: "ffmpeg"`)
- `reconnect_count` (new, only meaningful for endpoints on `pusher: "rust"`)

Dashboard renders whichever is non-zero, labeling it appropriately. PR 4 deletes `ffmpeg_restart_count` and renames `reconnect_count` to be the single source of truth.

---

**Decisions locked during brainstorming:**

1. RTMP library: xiu `ClientSession` direct (Push mode).
2. Ship strategy: 4 PRs, side-by-side per-endpoint flag, FB-Zbynek first.
3. Reconnect TS policy: continue where we left off (monotonic across reconnects).
4. Equivalence criterion: media-payload (H.264 NALUs + AAC frames) byte-identical to source FLV.
5. Rescue boundary: keep current FLV-byte boundary (rescue.rs unchanged).
6. Soak: agent-driven (autonomous) 4h+ gate before PR 3 and before PR 4. The agent is the operator (autonomous-verification rule).
7. Error classes: new small `PushError` enum (HandshakeFailed, ConnectRejected, PublishRejected, RemoteClosed, Timeout, IoError, LocalCancel).
