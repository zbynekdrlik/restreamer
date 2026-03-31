# Network Resilience Hardening — Design Spec

**Date:** 2026-03-31
**Trigger:** Manual ethernet cable disconnect test revealed multiple failure modes

## Problem Statement

A 20-second ethernet cable disconnect on stream.lan caused:

1. **Delivery VPS ffmpeg crash loop**: The "e2e rtmp" endpoint accumulated 10,511 ffmpeg restarts with only 1,150 chunks processed, falling 21 minutes behind. Root cause: ffmpeg has no RTMP output reconnection flags — when the output connection drops, ffmpeg dies, gets restarted, and the cycle repeats.
2. **No VPS-side diagnostic logs accessible**: Diagnosing the incident required guesswork because rs-delivery has no `/api/logs` endpoint. SSH access to ad-hoc Hetzner VPSes is unreliable.
3. **RTMP tray URL uses LAN IP**: "Copy RTMP URL" uses `best_lan_ip()` which resolves to a LAN address (e.g., 10.77.9.204). Since OBS runs on the same machine as Restreamer, a cable disconnect breaks the OBS→Restreamer connection unnecessarily. Should always be 127.0.0.1.
4. **RTMP inpoint has no auto-restart**: If xiu's RTMP server crashes (e.g., from corrupted TCP state after network event), `run_inpoint_loop` exits permanently. No automatic recovery.
5. **Cache bar prediction broadcasts zero local pending chunks**: When the VPS is unreachable, the prediction mode hardcodes `local_buffer_chunks: 0` and `s3_queue_chunks: 0` (lib.rs:410-411) instead of querying actual pending chunk counts.
6. **No automated test for disconnect/recovery**: The only way to verify resilience is manual cable-pulling. CI has no simulation of network outages.

## Evidence (from incident logs)

**stream.lan timeline (UTC 2026-03-31):**
- 18:04:01 — Delivery VPS status checks start failing (cable disconnect)
- 18:06:31 — S3 upload fails for chunk 224260 (connection reset, os error 10054)
- 18:07:27 — RTMP stream drops (xiu net io error)
- 18:07:34 — OBS reconnects (7 seconds), new RTMP session established
- 18:08:31 — S3 uploads fail with DNS resolution error (os error 11001)
- 18:09:33 — Recovery: S3 uploads resume, backlogged chunks drain in ~6 seconds

**Delivery VPS state (post-incident):**
- e2e rtmp: 10,511 ffmpeg restarts, 1,150 chunks processed, 1,300s delay
- e2e hls: 0 ffmpeg restarts, 2,488 chunks processed, 18s delay

The HLS endpoint (YouTube HTTP PUT) survived because it uses HTTP with natural retry. The RTMP endpoint died because ffmpeg's RTMP output has no reconnection logic.

## Design

### Component 1: `/api/logs` endpoint on rs-delivery

**What:** Add an authenticated GET `/api/logs` endpoint to rs-delivery that returns the last N log lines from an in-memory ring buffer.

**How:**
- Add a `LogBuffer` struct: thread-safe ring buffer (VecDeque) holding the last 1,000 log entries (timestamp + level + message).
- Implement a custom `tracing` subscriber layer that pushes formatted log lines into the `LogBuffer`.
- Expose `GET /api/logs?limit=100&level=warn` with bearer auth.
- Response: JSON array of `{ timestamp, level, message }` objects.
- Default limit: 100. Max limit: 1,000. Optional `level` filter (error, warn, info, debug).

**Why ring buffer, not file:** Hetzner VPSes are ephemeral — no persistent storage that survives deletion. An in-memory ring buffer is sufficient for diagnostic queries during the VPS lifetime.

### Component 2: RTMP URL → 127.0.0.1

**What:** Change "Copy RTMP URL" in `src-tauri/src/tray.rs` to always use `127.0.0.1`.

**How:** Replace `best_lan_ip()` call on line 227 with `"127.0.0.1"`. Also update the menu item label on line 86.

**Dashboard URL stays unchanged** — it uses `best_lan_ip()` because other devices on the LAN need to access it.

### Component 3: RTMP inpoint auto-restart on crash

**What:** When the xiu RTMP server exits unexpectedly, automatically restart it with exponential backoff.

**How:** In `orchestrator.rs`, `run_inpoint_loop` line 345-351:
- Change `break false` (on unexpected server exit) to `break true` (trigger restart).
- Add crash counter with backoff: 1s, 2s, 4s, 8s... max 30s.
- Reset crash counter when a publisher successfully connects (detected via `inpoint_state.is_connected()` becoming true).
- After 10 consecutive crashes without any successful connection, log error and give up (break false).
- Broadcast `WsEvent::ActivityFeed` warning on each restart so the dashboard shows it.

### Component 4: Smarter ffmpeg restart in endpoint_task

**What:** ffmpeg's `-reconnect` flags are **input-only** (demuxer `.D.` flags) — they cannot reconnect RTMP output. When the RTMP output connection drops, ffmpeg will always die. The fix must be in rs-delivery's endpoint_task loop, not in ffmpeg flags.

**How:** Improve the restart logic in `endpoint_task.rs` to handle transient output failures gracefully:

1. **Distinguish chunk-starved death from output failure:**
   - If ffmpeg dies while `consecutive_chunk_misses > 0` (no data to write), it starved — don't count as ffmpeg failure. Just wait for chunks and restart ffmpeg when data resumes.
   - If ffmpeg dies while chunks are available (write error, stdin closed), that's an output failure — count toward circuit breaker.

2. **Don't restart ffmpeg during chunk drought:**
   - If no chunks have arrived for `MAX_CHUNK_MISS_COUNT` polls, kill ffmpeg proactively and enter "paused" state.
   - In paused state: keep polling S3 for chunks every 5s, but don't spawn ffmpeg.
   - When chunks reappear: reset `FlvStreamNormalizer`, spawn fresh ffmpeg, resume delivery.
   - This eliminates the 10,000+ restart cycle entirely — ffmpeg only runs when there's data to process.

3. **Faster ffmpeg recovery after transient output failure:**
   - Current: 3s sleep before restart on ffmpeg death.
   - New: 1s first retry, 3s second, 5s third, then circuit breaker. Most transient RTMP drops recover on first retry.

4. **Add `-flvflags +no_duration_filesize` to RTMP output args** in `build_flv_rtmp_args()` to prevent ffmpeg from seeking back to update FLV metadata on close (which fails on pipe output and can cause write errors).

### Component 5: Cache bar prediction — include local pending chunks

**What:** When the delivery VPS is unreachable (prediction mode), broadcast real local pending chunk counts instead of zeros.

**How:** In `rs-api/src/lib.rs`, the `Err(e)` branch of `poll_delivery_metrics` (lines 386-427):
- Query `db::get_pending_chunk_count_for_event()` for real pending count.
- Query `db::get_sent_chunk_count_for_event()` for real sent count.
- Broadcast these in `local_buffer_chunks` and `s3_queue_chunks` fields (currently hardcoded to 0 on lines 410-411).
- The frontend already displays these values in the pipeline view — this just provides real data during prediction mode.

### Component 6: E2E disconnect simulation tests

Three test levels, all automated in CI:

**Level 1: rs-delivery unit test — chunk drought + ffmpeg recovery**

Using the existing `MockFetcher` infrastructure in `endpoint_task_tests.rs`:
- Create a mock fetcher that returns `Ok(None)` for N seconds (simulating S3 drought), then resumes returning chunks.
- Verify: endpoint recovers after chunks resume, `ffmpeg_restart_count` stays below a threshold (e.g., < 5 for a 30s drought), endpoint reaches `alive: true` after recovery, `stall_reason` clears after recovery.

**Level 2: rs-delivery unit test — ffmpeg reconnect survival**

- Create a mock output process that simulates write failures for N seconds then recovers.
- Verify: endpoint survives without entering circuit breaker, chunks continue processing after write recovery.

**Level 3: Playwright E2E — cache bar prediction during simulated outage**

Add a test API endpoint `POST /api/v1/_test/s3-block` that makes the S3 uploader reject all uploads for N seconds:
- The endpoint sets an `AtomicBool` flag checked by `ChunkUploader::upload_batch()`.
- When the flag is set, `upload_batch()` returns an error for each chunk (simulating network failure) without actually attempting S3.
- The flag auto-clears after the specified duration.

Playwright test flow:
1. Start event, begin streaming (ffmpeg test source → RTMP → Restreamer).
2. Wait for cache bar to show buffering/streaming with stable progress.
3. `POST /api/v1/_test/s3-block { "duration_secs": 15 }` — block S3 uploads for 15 seconds.
4. Assert: `local_buffer_chunks` increases (chunks piling up locally).
5. Assert: if VPS also becomes unreachable (simulated via ws-broadcast), cache bar enters prediction mode, shows draining.
6. Wait for S3 block to expire (15s).
7. Assert: pending chunks drain (uploads resume), cache bar recovers toward target.
8. Assert: no console errors, clean browser console.

## Files Changed

| File | Action | Component |
|------|--------|-----------|
| `crates/rs-delivery/src/log_buffer.rs` | Create | 1 |
| `crates/rs-delivery/src/main.rs` | Modify | 1 |
| `crates/rs-delivery/src/api.rs` | Modify | 1 |
| `src-tauri/src/tray.rs` | Modify | 2 |
| `crates/rs-runtime/src/orchestrator.rs` | Modify | 3 |
| `crates/rs-ffmpeg/src/lib.rs` | Modify | 4 (flvflags) |
| `crates/rs-delivery/src/endpoint_task.rs` | Modify | 4 (drought pause, smarter restart) |
| `crates/rs-api/src/lib.rs` | Modify | 5 |
| `crates/rs-endpoint/src/uploader.rs` | Modify | 6 (s3-block flag) |
| `crates/rs-api/src/test_handlers.rs` | Modify | 6 (test endpoint) |
| `crates/rs-delivery/src/endpoint_task_tests.rs` | Modify | 6 (unit tests) |
| `e2e/frontend.spec.ts` | Modify | 6 (Playwright test) |

## Out of Scope

- Automatic S3 chunk cleanup (separate concern, tracked separately)
- Delivery VPS auto-recreation on prolonged health failure (existing health monitor handles 3/3 threshold)
- OBS reconnection behavior (OBS handles this internally, logs confirmed 7s reconnect)
