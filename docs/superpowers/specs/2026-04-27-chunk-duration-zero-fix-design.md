# Chunk Duration Zero — Fix Design

**Date:** 2026-04-27
**Status:** Approved
**Severity:** P0 — production stream delivery is broken on `dev` = `main` = v0.3.72

## Summary

A regression introduced by the audio chipmunk fix (PR #144, commit `3281961`) corrupts chunk-duration tracking. Every chunk row in `chunk_records` gets `duration_ms = 0`, which cascades into a 15-minute Restreamer-side wait, an incorrect `start_chunk_id = 1` in the VPS init payload, and an indefinite warmup spin on the VPS — visible to the operator as "BUFFERING" forever with `S3 → VPS: NNN queued → 0 delivered`.

This spec fixes the regression, adds the CI gate that should have caught it, and hardens two adjacent failure modes that let the bug stay invisible.

## Context

### Symptoms observed 2026-04-27 on stream.lan (manual operator test)

- Restreamer v0.3.72 deployed and running, RTMP ingest connected (16+ min stable, 9.1 Mbps, 192 GB cumulative).
- Hetzner VPS #739 (91.99.55.159, cpx22, nbg1) created, `running`, `rs-delivery` 0.3.72 answering `/api/health`.
- Delivery `/api/init` returned `status: ok, endpoints_started: 1`.
- **15-minute gap** between `vps_ready` (08:25:52 UTC) and `delivery_init_sent` (08:40:53 UTC).
- Dashboard stuck at `BUFFERING 00:20:33+`, `S3 → VPS: 616 queued → 0 delivered`, endpoint `chunks_processed: 0`, `current_chunk_id: 1`, `delivery_mode: warmup`.
- Restreamer audit log clean — no errors. VPS `/api/logs` only has 4 INFO lines from 25 min ago plus 684× hyper-trace `"shouldn't retry!"` noise.
- VPS-side log uploaded to S3 at `delivery-logs/rs-delivery-evt9278.log` confirms the warmup loop is silent — it sees `Ok(None)` from `chunk_duration_ms(1)` and sleeps without incrementing `probe_id`.

### Root cause

In `crates/rs-inpoint/src/flv_chunker.rs`:
- `chunk_first_ts` (set in `write_chunk_header`, line 358) and `chunk_last_ts` (updated in `write_video`, line 224) both come from `current_session_ts(&mut inner)` — wall-clock based, ms since session start.
- After PR #144, `write_audio` (line 282) was changed to `inner.chunk_last_ts = ts` where `ts = timestamp` — the xiu RTMP audio timestamp, which starts at 0 and grows with audio PTS (small values for the first ~minute of stream).
- Audio frames fire more often than video keyframes. Every audio frame overwrites `chunk_last_ts` with a small xiu value, while `chunk_first_ts` stays at the larger wall-clock value.
- `duration_ms = chunk_last_ts - chunk_first_ts` underflows → falls to the `else { 0 }` branch (line 446 — comment claims "u32 wrap after 49 days" but real cause is mixed time domains).
- Every `chunk_records` row: `duration_ms = 0`. Confirmed via `sqlite3` on stream.lan: 1128 rows for event 9278, all `duration_ms = 0`.

### Cascading effects of `duration_ms = 0`

1. `db::get_sent_duration_ms(event_id)` returns 0. The orchestrator wait loop in `crates/rs-api/src/delivery.rs:463-491` waits up to `max_wait_secs = 900`, never satisfies `sent_ms >= wait_target_ms`, times out → 15-minute delay.
2. `db::compute_target_start_chunk` (`crates/rs-core/src/db/mod.rs:451`) walks back from latest `sent=1` row accumulating `duration_ms = 0` until it has scanned all 10 000 rows. Returns the OLDEST sequence_number it walked, which is 1. Init payload sends `start_chunk_id: 1`.
3. rs-delivery on VPS calls `chunk_duration_ms(1)`. The S3 uploader prunes chunks immediately after upload (`crates/rs-endpoint/src/uploader.rs:402`), so chunk 1 has been gone for hours. Fetcher returns `Ok(None)`.
4. `crates/rs-delivery/src/rescue.rs:425` — `run_warmup_loop`'s `Ok(None)` branch sleeps 2 s without incrementing `probe_id`. Spins forever, silently.

## Design

Five small commits, TDD strict, in this order. Each commit's failing test is committed before the implementation that satisfies it.

### Commit 0 — version bump

`Cargo.toml`, `src-tauri/Cargo.toml`, `src-tauri/tauri.conf.json`, `leptos-ui/Cargo.toml`: 0.3.72 → 0.3.73. Standalone commit per `version-bumping` ruleset.

### Commit 1 — failing chunker test exposes the regression

Add unit test in `crates/rs-inpoint/src/flv_chunker.rs` `mod tests`:

`chunk_duration_tracks_video_wall_span_not_audio_xiu_ts` — runs a realistic frame sequence, drives the chunker through one full chunk, asserts `PendingChunkWrite.duration_ms` is approximately the wall-clock span between the two video keyframes (not 0, not the audio xiu_ts).

Concrete shape:
1. write_video (keyframe, ts ignored — uses current_session_ts)
2. write_audio (timestamp=20)
3. write_audio (timestamp=43)
4. tokio sleep 100 ms
5. write_audio (timestamp=66)
6. tokio sleep 100 ms
7. write_video (keyframe — flushes chunk)

Assert `pending.duration_ms` is in `[150, 350]` ms. With current code on `dev` it returns 0.

Also delete the existing test `audio_uses_xiu_timestamp_not_wall_clock` (introduced in PR #144) — its assertion `chunk_last_ts == 42` after two audio writes encodes the bug as a test. Replace with a parsing-based test that asserts the FLV audio tag bytes carry timestamp=42 (the xiu PTS still flows through to the FLV stream — that part of #142 is correct and stays intact).

Commit message: `test(inpoint): assert chunk duration tracks video wall span (#NN)` where NN is the issue number from Commit 0a (filed first via `gh issue create`).

### Commit 2 — fix the regression

`crates/rs-inpoint/src/flv_chunker.rs` `write_audio`: remove the line `inner.chunk_last_ts = ts;`.

Update the doc comment on `write_audio` to record the separation: "Audio FLV tags carry xiu RTMP timestamps for correct decoder PTS (chipmunk fix from #142). `chunk_last_ts` is owned by `write_video` only — it tracks the wall-clock span used for chunk duration accounting. The two timestamp domains MUST NOT cross."

After this commit, both new tests pass: the duration test from Commit 1 returns ~200 ms instead of 0, and the FLV-tag-byte test still proves the audio PTS is preserved.

`cargo fmt --all --check` only locally — no `cargo build`/`test`/`clippy` per `ci-push-discipline`.

Commit message: `fix(inpoint): audio frames must not overwrite chunk_last_ts (#NN)`.

### Commit 3 — VPS warmup loop hardening (defense in depth)

`crates/rs-delivery/src/rescue.rs` `run_warmup_loop`. Failing test first: `warmup_skips_forward_when_chunk_missing_for_n_seconds` using a mock `ChunkFetcher` that returns `Ok(None)` for chunk_id=1 but `Ok(Some(2000))` for chunk_id=5+. Assert: warmup completes (returns `false`) within ~30 s of wall-time-equivalent ticks, advancing past the missing chunk.

Then implementation: track consecutive `Ok(None)` count; after N=30 (≈60 s real time, since each Ok(None) sleeps 2 s), `tracing::warn!` once with the stuck chunk_id, then `probe_id += 1` and reset the counter. The warning lines surface in `/api/logs` and the S3-uploaded log so a future stuck warmup is diagnosable from outside.

The same hardening applies to the consumer task's main fetch loop if it has an analogous silent-`Ok(None)` branch. Audit `crates/rs-delivery/src/endpoint_task.rs` for the equivalent pattern; if present, fix in the same commit.

Commit message: `fix(delivery): warmup advances past missing chunks instead of spinning silently (#NN)`.

### Commit 4 — CI gate that would have caught the regression

`.github/workflows/ci.yml`, `e2e-obs-youtube-test` job. Two new GATE steps, hard-fail:

1. **GATE: delivery_init_sent within 180s of vps_ready.** Polls `/api/v1/audit?event_id=<id>&limit=200`, parses for `vps_ready` and `delivery_init_sent` rows, asserts `(delivery_init_sent.ts - vps_ready.ts) <= 180s`. Catches the 15-minute wait regression.
2. **GATE: chunks_processed > 0 within 90s of delivery_init_response=ok.** Polls `/api/v1/delivery/status?event_id=<id>` until `endpoint_details[0].chunks_processed > 0`, time-bounded. Catches the warmup deadlock and any future "init succeeds but no chunks flow" regression.

Both gates are pure E2E assertions on existing instrumentation — no new code paths, just verifying the system makes forward progress. Failure modes are clearly distinguishable from the audit-log content.

ASCII-only PowerShell strings (per `feedback_no_unicode_in_ci_scripts` memory). No em-dashes, no `—`.

Commit message: `ci: gate OBS-to-YouTube E2E on init latency and chunk progression (#NN)`.

### Commit 5 — defend `compute_target_start_chunk` against zero-duration data

`crates/rs-core/src/db/upload_tests.rs`. Failing test first: `compute_target_start_chunk_returns_latest_when_all_durations_zero` — inserts 100 chunk_records with `duration_ms = 0` and `sent = 1`, asserts the function returns the LATEST sequence_number (or `Err`), NOT the oldest.

Then implementation in `crates/rs-core/src/db/mod.rs:451`: when `accum` is still 0 after walking all rows (or all rows have `duration_ms = 0`), return latest seq instead of oldest. This converts a silent data-corruption-induced deadlock into "VPS starts at live-edge with empty buffer" — degraded UX but not a hang.

Add a TRACE-level WARN log: `compute_target_start_chunk: all sent chunks have duration_ms=0; using latest as start_chunk_id (event N)` so future occurrences are visible.

Commit message: `fix(db): compute_target_start_chunk falls back to latest on zero-duration data (#NN)`.

## Operator action (immediate, manual)

Stop the broken VPS instance #739 from the dashboard's "Stop Delivering" button. The fix in this PR will NOT recover the currently-stuck delivery; the only path is to stop the instance, redeploy v0.3.73 once shipped, and start a fresh delivery.

## Acceptance criteria

1. `cargo fmt --all --check` clean.
2. CI green on the dev push, ALL jobs (mutation testing, coverage, build, e2e-streaming, e2e-obs-youtube, deploy-stream-lan).
3. Both new CI gates (init latency, chunks_processed > 0) pass on the e2e-obs-youtube-test run.
4. PR is `mergeable: true` AND `mergeable_state: clean`.
5. Post-deploy on stream.lan: operator triggers a fresh OBS stream → Restreamer → Hetzner VPS → YouTube. Verify via Playwright (open `http://10.77.9.204:8910/`) that the dashboard transitions IDLE → STREAMING within 3 minutes of clicking Start Delivering, and `S3 → VPS: NNN queued → MMM delivered` shows MMM > 0 within 90 s of init.
6. SQL spot-check on stream.lan after a fresh stream:
   ```
   SELECT COUNT(*), AVG(duration_ms), MIN(duration_ms), MAX(duration_ms)
   FROM chunk_records WHERE streaming_event_id=<new_id> AND sent=1;
   ```
   Expect MIN > 0 (typical 1900-2100 ms for 2-second chunks).

## Out of scope

- rs-delivery log-promotion (TRACE → WARN/ERROR for hyper-pool noise). The dashboard already surfaces the runtime symptom; once Commits 2-3 land, debugging from outside is no longer the user-blocking failure mode. Tracked as follow-up if it bites again.
- Rewriting `compute_target_start_chunk` to use `MAX(sequence_number) WHERE sent = 1` directly. Commit 5 turns the failure into degraded UX rather than a hang; a deeper rewrite is YAGNI until that proves insufficient.
- Restreamer-side preflight that detects `duration_ms = 0` and aborts before sending init. Commit 1+2 prevents the data corruption at source; defense at the consumer side (Commit 5) is enough.

## Risks

- **Test 1 (Commit 1) may need wall-clock injection.** `current_session_ts` reads `Instant::now()`. The test must either accept ±100 ms slop or inject a clock. Prefer slop-tolerant assertion (`duration_ms in [150, 350]`) — same pattern as the existing `wall_clock_ts_monotonic_across_frames` test.
- **Commit 3 changes warmup behavior on real production.** If a brief S3 outage manifests as `Ok(None)`, we now advance past chunks faster. With N=30 (60 s) before skipping, this only kicks in on genuine deadlocks.
- **Commit 5 changes startup behavior in the corrupt-data case.** Without the fix, VPS hangs (current production behavior). With the fix, VPS starts at live-edge on zero-duration data — viewer sees a brief gap, but stream actually starts. Strictly better than today.

## Test inventory

| Commit | Test | Type | File |
|---|---|---|---|
| 1 | `chunk_duration_tracks_video_wall_span_not_audio_xiu_ts` | unit | `crates/rs-inpoint/src/flv_chunker.rs` |
| 1 | `audio_flv_tag_carries_xiu_timestamp` (replaces deleted test) | unit | `crates/rs-inpoint/src/flv_chunker.rs` |
| 3 | `warmup_skips_forward_when_chunk_missing_for_n_seconds` | unit | `crates/rs-delivery/src/rescue_tests.rs` |
| 4 | GATE init latency ≤180s | E2E (CI) | `.github/workflows/ci.yml` |
| 4 | GATE chunks_processed > 0 within 90s | E2E (CI) | `.github/workflows/ci.yml` |
| 5 | `compute_target_start_chunk_returns_latest_when_all_durations_zero` | unit | `crates/rs-core/src/db/upload_tests.rs` |

Total: 4 new unit tests, 2 new CI gates, 1 deleted test (the one that encoded the bug as a test).
