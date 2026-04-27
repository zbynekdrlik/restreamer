# Chunk Duration Zero Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop production stream delivery from hanging by removing the `chunk_records.duration_ms = 0` regression introduced in PR #144, plus add the CI gate that should have caught it and harden two adjacent failure modes.

**Architecture:** Five focused commits with TDD discipline (failing test before implementation for the production bug; combined test+impl for defense-in-depth changes). One PR from dev → main.

**Tech Stack:** Rust 2024 (rs-inpoint, rs-delivery, rs-core), Cargo workspace, GitHub Actions CI on self-hosted Windows runner, sqlx (SQLite), Hetzner Cloud, Hetzner Object Storage.

**Spec:** `docs/superpowers/specs/2026-04-27-chunk-duration-zero-fix-design.md` (committed at `a2c616b`).

---

## Context

### Branch state (verify before starting)

```bash
git fetch origin
git rev-parse origin/main   # should equal origin/dev
grep '^version' Cargo.toml | head -1   # 0.3.72
```

### What's broken (and what's not)

- `crates/rs-inpoint/src/flv_chunker.rs:282` writes `inner.chunk_last_ts = ts;` in `write_audio` where `ts = timestamp` (xiu RTMP, starts at 0). Audio frames overwrite `chunk_last_ts` with a small value while `chunk_first_ts` holds a larger wall-clock value → `duration_ms` underflows → falls to `else { 0 }` branch (line 446).
- DO NOT touch `write_video` (line 224 — same statement is correct because `ts` there is wall-clock).
- DO NOT touch `chunk_first_ts` (line 358).
- DO NOT touch `current_session_ts`, `session_start_wall_clock_ms`, `reset()`.
- DO NOT touch the duration_ms computation at line 442-447.

### Issue-number propagation

Task 1 creates a GitHub issue and prints the number. The orchestrator captures it from the subagent's report and substitutes it into the prompts for Tasks 3-7 (replace literal `#NN` with the actual `#<number>`).

### Constraints (every task)

- Local checks: `cargo fmt --all --check` only. NO `cargo build`, `cargo test`, `cargo clippy` locally per `ci-push-discipline`.
- TDD: failing-test commit BEFORE implementation commit for the chunker regression (Tasks 3 → 4). Defense-in-depth tasks (5, 7) keep test+impl in one commit.
- One commit per task, never batch. The subagent does NOT push, compile, or run tests locally.
- ASCII-only PowerShell strings in CI YAML (no em-dashes — see memory `feedback_no_unicode_in_ci_scripts`).
- File size: `flv_chunker.rs` is currently ~970 lines, must stay under 1000.

---

## Task 1: File the GitHub issue (P0)

**Files:** none (no code change).

- [ ] **Step 1: Create the issue and capture the number**

```bash
ISSUE_URL=$(gh issue create \
  --title "chunk_records duration_ms=0 regression from PR #144 — production delivery hung" \
  --body "$(cat <<'EOF'
## Severity: P0 — production stream delivery is broken on dev = main = v0.3.72

## Symptom
Operator manual test on stream.lan 2026-04-27. RTMP ingest healthy, Hetzner VPS healthy, `delivery_init_response` returned ok, but dashboard stuck at `BUFFERING 00:20:33+`, `S3 → VPS: NNN queued → 0 delivered`. VPS endpoint shows `chunks_processed: 0`, `current_chunk_id: 1`, `delivery_mode: warmup`. Restreamer-side `delivery_init_sent` happened 15 minutes after `vps_ready`.

## Root cause
PR #144 (commit `3281961`) changed `write_audio` in `crates/rs-inpoint/src/flv_chunker.rs:282` to set `inner.chunk_last_ts = ts` where `ts = timestamp` (xiu RTMP audio timestamp, small values). `chunk_first_ts` still holds the larger wall-clock value from `write_chunk_header`. `duration_ms = chunk_last_ts - chunk_first_ts` underflows → falls to the `else { 0 }` branch → every chunk row gets `duration_ms = 0`.

SQL confirmation on stream.lan production DB:
```
SELECT COUNT(*), MIN(duration_ms), MAX(duration_ms)
FROM chunk_records WHERE streaming_event_id=9278 AND sent=1;
-- 1127, 0, 0
```

## Cascading effects
1. `db::get_sent_duration_ms` returns 0 → orchestrator wait loop (`crates/rs-api/src/delivery.rs:463-491`) times out at `max_wait_secs=900` → 15-min delay.
2. `db::compute_target_start_chunk` (`crates/rs-core/src/db/mod.rs:451`) walks back through 10 000 zero-duration rows → returns the OLDEST sequence_number = 1.
3. Init sends `start_chunk_id: 1`. Chunk 1 was pruned from S3 hours ago by the uploader.
4. `crates/rs-delivery/src/rescue.rs:425` `Ok(None)` branch sleeps 2s without incrementing `probe_id` → spins on chunk 1 forever, silently.

## Spec
`docs/superpowers/specs/2026-04-27-chunk-duration-zero-fix-design.md` (committed `a2c616b`).
EOF
)" 2>&1 | tail -1)
echo "Issue URL: $ISSUE_URL"
ISSUE_NUM=$(echo "$ISSUE_URL" | grep -oE '/issues/[0-9]+' | grep -oE '[0-9]+')
echo "ISSUE_NUM=$ISSUE_NUM"
```

- [ ] **Step 2: Report the issue number to the orchestrator**

The subagent's final report MUST include the line: `ISSUE_NUM=<number>` so the orchestrator can capture it for Tasks 3-7.

---

## Task 2: Version bump 0.3.72 → 0.3.73

**Files:**
- Modify: `Cargo.toml` (workspace version, top of file)
- Modify: `src-tauri/Cargo.toml`
- Modify: `src-tauri/tauri.conf.json`
- Modify: `leptos-ui/Cargo.toml`

- [ ] **Step 1: Bump all four files**

```bash
sed -i 's/^version = "0.3.72"$/version = "0.3.73"/' Cargo.toml
sed -i 's/^version = "0.3.72"$/version = "0.3.73"/' src-tauri/Cargo.toml
sed -i 's/"version": "0.3.72"/"version": "0.3.73"/' src-tauri/tauri.conf.json
sed -i 's/^version = "0.3.72"$/version = "0.3.73"/' leptos-ui/Cargo.toml
```

- [ ] **Step 2: Verify exactly four files changed and the value is right**

```bash
git diff --stat -- Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
grep -H '^version = "0.3.73"' Cargo.toml src-tauri/Cargo.toml leptos-ui/Cargo.toml
grep -H '"version": "0.3.73"' src-tauri/tauri.conf.json
```

Expected: 4 lines (one per file) with `0.3.73`.

- [ ] **Step 3: Local format check**

```bash
cargo fmt --all --check
```

Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml src-tauri/Cargo.toml src-tauri/tauri.conf.json leptos-ui/Cargo.toml
git commit -m "chore: bump version to 0.3.73"
```

---

## Task 3: Failing chunker test (TDD) + delete the test that encodes the bug

**Files:**
- Modify: `crates/rs-inpoint/src/flv_chunker.rs` — `mod tests` block (closes around line 970+, immediately before the file's final `}`)

- [ ] **Step 1: Delete the test that asserts the bug as truth**

In `crates/rs-inpoint/src/flv_chunker.rs`, delete the entire test starting at line 933:

```rust
    #[tokio::test]
    async fn audio_uses_xiu_timestamp_not_wall_clock() {
        // ... full body through closing brace ~line 968 ...
    }
```

Use `Edit` with the exact existing test text as `old_string` and an empty replacement (or remove the function entirely). This test was added by commit `f9d788c` in PR #144 and asserts `inner.chunk_last_ts == 42` after audio writes — that assertion encodes the regression we are fixing.

- [ ] **Step 2: Add the failing duration test**

Append this test inside `mod tests` (immediately before the closing `}` of `mod tests` at the bottom of the file):

```rust
    /// REGRESSION (PR #144): write_audio used to overwrite chunk_last_ts with
    /// the xiu RTMP timestamp (small values) while chunk_first_ts held the
    /// larger wall-clock value from write_chunk_header. The subtraction
    /// underflowed → duration_ms fell to the "wrapped around" else-branch and
    /// returned 0 for EVERY chunk. That cascaded into a 15-minute orchestrator
    /// wait, start_chunk_id=1 in the VPS init, and silent VPS warmup spin.
    ///
    /// This test runs a realistic frame sequence and asserts the resulting
    /// chunk's duration_ms reflects video wall-clock span, not audio xiu_ts.
    #[tokio::test]
    async fn chunk_duration_tracks_video_wall_span_not_audio_xiu_ts() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(FlvChunkSink::new(
            dir.path().to_path_buf(),
            Duration::from_millis(50),
        ));
        let mut rx = sink.subscribe();

        // Seed sequence headers so chunks form correctly.
        let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
        sink.write_video(0, &video_seq).await;
        let audio_seq = BytesMut::from(&[0xAF, 0x00, 0x12, 0x10][..]);
        sink.write_audio(0, &audio_seq).await;

        // First keyframe — anchors the session, starts chunk #0.
        let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
        sink.write_video(0, &keyframe).await;

        // Audio frames carry small xiu timestamps. Pre-fix, each of these
        // overwrites chunk_last_ts with the xiu value, breaking duration.
        let aac = BytesMut::from(&[0xAF, 0x01, 0x12, 0x34, 0x56][..]);
        sink.write_audio(20, &aac).await;
        sink.write_audio(43, &aac).await;

        // Wait so the next video keyframe's wall-clock stamp is meaningfully
        // later than the chunk's first frame.
        tokio::time::sleep(Duration::from_millis(100)).await;
        sink.write_audio(66, &aac).await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Second keyframe flushes the chunk (50ms min duration was hit).
        let keyframe2 = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xCC, 0xDD][..]);
        sink.write_video(0, &keyframe2).await;

        let chunk = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("chunk should be emitted within timeout")
            .expect("recv should succeed");

        // ~200 ms of wall-clock between first and second keyframes; allow
        // generous slop for CI scheduling jitter. Pre-fix this is 0.
        assert!(
            chunk.duration_ms >= 150 && chunk.duration_ms <= 350,
            "duration_ms must reflect video wall-clock span (~200ms), got {}",
            chunk.duration_ms
        );
    }

    /// Audio FLV tags must still carry the xiu RTMP timestamp in the FLV
    /// byte stream (PR #142 chipmunk fix preserved). This is the user-facing
    /// guarantee — chunk_last_ts is an internal accounting variable that
    /// MUST NOT be confused with what gets written into the audio tag.
    #[tokio::test]
    async fn audio_flv_tag_carries_xiu_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(FlvChunkSink::new(
            dir.path().to_path_buf(),
            Duration::from_secs(60),
        ));
        let mut rx = sink.subscribe();

        let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
        sink.write_video(0, &video_seq).await;
        let audio_seq = BytesMut::from(&[0xAF, 0x00, 0x12, 0x10][..]);
        sink.write_audio(0, &audio_seq).await;

        let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
        sink.write_video(0, &keyframe).await;

        // AAC payload tag with xiu timestamp 42.
        let aac = BytesMut::from(&[0xAF, 0x01, 0xDE, 0xAD][..]);
        sink.write_audio(42, &aac).await;

        sink.flush().await;

        let chunk = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("chunk should flush")
            .expect("recv should succeed");

        let bytes = std::fs::read(&chunk.path).unwrap();

        // Walk the FLV byte stream looking for an audio tag (type 0x08) whose
        // body starts with our AAC payload marker 0xAF 0x01 — that's the tag
        // we wrote (the audio sequence header has 0xAF 0x00). Read its
        // 32-bit timestamp (24 bits low + 8 bits upper) and assert it's 42.
        // FLV header is 9 bytes + 4 bytes "previous tag size 0".
        let mut offset = 9 + 4;
        let mut found = None;
        while offset + 11 <= bytes.len() {
            let tag_type = bytes[offset];
            let data_size = ((bytes[offset + 1] as u32) << 16)
                | ((bytes[offset + 2] as u32) << 8)
                | (bytes[offset + 3] as u32);
            let ts_low = ((bytes[offset + 4] as u32) << 16)
                | ((bytes[offset + 5] as u32) << 8)
                | (bytes[offset + 6] as u32);
            let ts_high = bytes[offset + 7] as u32;
            let ts = (ts_high << 24) | ts_low;

            let body_start = offset + 11;
            let body_end = body_start + data_size as usize;
            if tag_type == FLV_TAG_AUDIO
                && body_end <= bytes.len()
                && bytes.get(body_start) == Some(&0xAF)
                && bytes.get(body_start + 1) == Some(&0x01)
            {
                found = Some(ts);
                break;
            }
            offset = body_end + 4; // skip body + 4-byte previous-tag-size trailer
        }

        assert_eq!(
            found,
            Some(42),
            "audio FLV tag must carry xiu timestamp 42 in the byte stream"
        );
    }
```

- [ ] **Step 3: Local format check**

```bash
cargo fmt --all --check
```

Expected: no output.

- [ ] **Step 4: Verify the file is still under 1000 lines**

```bash
wc -l crates/rs-inpoint/src/flv_chunker.rs
```

Expected: < 1000 (deleted ~25 lines, added ~85, net +60 → ~1030; if over 1000, the subagent must STOP and report — file-size gate would fail).

If line count exceeds 1000: this is a real problem. The subagent should report it; the orchestrator will need to split the test module into a separate file. Do NOT silently push past 1000.

- [ ] **Step 5: Commit**

```bash
git add crates/rs-inpoint/src/flv_chunker.rs
git commit -m "test(inpoint): assert chunk duration tracks video wall span (#NN)"
```

The two added tests will FAIL on `dev`'s current code:
- `chunk_duration_tracks_video_wall_span_not_audio_xiu_ts` — fails because duration_ms returns 0 (the regression).
- `audio_flv_tag_carries_xiu_timestamp` — passes today (PR #144's audio PTS fix is intact). Included now to lock in that behavior so the next commit's `chunk_last_ts` removal doesn't accidentally regress audio PTS.

---

## Task 4: Fix the chunker (the actual production bug)

**Files:**
- Modify: `crates/rs-inpoint/src/flv_chunker.rs:282` (and surrounding doc comment)

- [ ] **Step 1: Delete the offending line and update the doc comment**

In `crates/rs-inpoint/src/flv_chunker.rs`, locate `pub async fn write_audio`. The current body around line 280-285 looks like:

```rust
            let ts = timestamp;
            inner.chunk_last_ts = ts;
            Self::write_tag(&mut inner, FLV_TAG_AUDIO, ts, data);
            None
```

Replace the body of the inner `pending = { ... }` block (the part starting from `let ts = timestamp;`) with:

```rust
            // Audio FLV tags carry the xiu RTMP timestamp directly so the
            // decoder gets correct PTS (chipmunk fix from #142). The
            // chunk_last_ts field is owned by write_video — it tracks the
            // wall-clock span used for chunk-duration accounting. The two
            // timestamp domains MUST NOT cross: writing the xiu value into
            // chunk_last_ts here corrupts the duration computation
            // (#142 follow-up regression).
            Self::write_tag(&mut inner, FLV_TAG_AUDIO, timestamp, data);
            None
```

Concretely: delete the two lines `let ts = timestamp;` and `inner.chunk_last_ts = ts;`, and pass `timestamp` directly to `write_tag`. Add the comment block above the call.

- [ ] **Step 2: Update the doc comment on `write_audio`**

The current doc comment (above `pub async fn write_audio`, around lines 252-261) says audio uses xiu timestamps directly to fix #142 chipmunk. APPEND one paragraph that records the time-domain separation. After the existing line `/// artefacts (chipmunk pitch + glitches).` add:

```rust
    ///
    /// Internally, this function writes the xiu timestamp into the FLV tag
    /// only — it must NOT touch `chunk_last_ts`, which is an accounting
    /// field owned by `write_video` for tracking wall-clock chunk duration.
    /// Mixing the two domains causes `duration_ms` to underflow and produce
    /// 0 for every chunk (#NN, regression introduced by PR #144).
```

- [ ] **Step 3: Local format check**

```bash
cargo fmt --all --check
```

Expected: no output.

- [ ] **Step 4: Confirm only one file changed and the diff is minimal**

```bash
git diff --stat
git diff crates/rs-inpoint/src/flv_chunker.rs
```

Expected: 1 file changed; the diff shows the two-line deletion in `write_audio`, the doc-comment addition, and nothing else.

- [ ] **Step 5: Commit**

```bash
git add crates/rs-inpoint/src/flv_chunker.rs
git commit -m "fix(inpoint): audio frames must not overwrite chunk_last_ts (#NN)"
```

After this commit, the test from Task 3 will pass (verified on CI in Task 8).

---

## Task 5: VPS warmup hardening (defense in depth)

**Files:**
- Modify: `crates/rs-delivery/src/rescue.rs` (run_warmup_loop, around lines 388-440)
- Modify: `crates/rs-delivery/src/rescue_tests.rs` (append new test at end of file)
- Audit: `crates/rs-delivery/src/endpoint_task.rs` for analogous silent-Ok(None) pattern

- [ ] **Step 1: Add a custom mock fetcher to rescue_tests.rs**

The existing `WarmupMockFetcher` in `crates/rs-delivery/src/rescue_tests.rs:210-241` only supports a single cutoff (returns Ok(None) for chunk_id > available_up_to). The new test needs a fetcher that returns Ok(None) for the FIRST chunk specifically but Ok(Some(dur)) for chunks N+. Add a new struct AFTER `WarmupMockFetcher`:

```rust
/// Mock fetcher for testing the "stuck on missing chunk" hardening:
/// returns Ok(None) for any chunk_id <= `gap_until`, then Ok(Some(dur))
/// for chunks above that. Models the production scenario where
/// start_chunk_id points at a pruned chunk but newer chunks exist.
struct GapMockFetcher {
    gap_until: i64,
    chunk_duration_ms: i64,
}

impl GapMockFetcher {
    fn new(gap_until: i64, chunk_duration_ms: i64) -> Self {
        Self {
            gap_until,
            chunk_duration_ms,
        }
    }
}

impl ChunkFetcher for GapMockFetcher {
    async fn fetch_chunk_with_meta(
        &self,
        _chunk_id: i64,
    ) -> Result<Option<(Vec<u8>, i64)>, String> {
        unreachable!("warmup loop only calls chunk_duration_ms")
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        if chunk_id <= self.gap_until {
            Ok(None)
        } else {
            Ok(Some(self.chunk_duration_ms))
        }
    }
}
```

- [ ] **Step 2: Add the failing test for skip-forward behavior**

Append to `crates/rs-delivery/src/rescue_tests.rs` (end of file):

```rust
/// Hardens warmup against the "start_chunk_id points at a pruned chunk"
/// failure mode. Pre-fix the Ok(None) branch slept 2s without incrementing
/// probe_id, so a missing chunk hung the warmup loop forever and silently.
/// Post-fix: after CONSECUTIVE_NONE_THRESHOLD consecutive Ok(None)s on the
/// same chunk, log one WARN and advance probe_id by 1.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn warmup_skips_forward_when_chunk_missing_for_n_seconds() {
    // chunks 1..=4 missing (pruned). chunks 5+ available, 50ms each.
    // Target 1000ms — should reach within ~20 chunks past chunk 5.
    let alias = unique_alias("skip-stuck");
    let fetcher = GapMockFetcher::new(4, 50);
    let ep_cfg = test_endpoint_config(&alias, false);

    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (_stop_tx, mut stop_rx) = watch::channel(false);

    // Start at chunk 1 (the "pruned" range).
    let stopped = crate::rescue::run_warmup_loop(
        &fetcher,
        &alias,
        &ep_cfg,
        1,
        1000,
        None, // no rescue video — keeps test simple
        &stats,
        &mut stop_rx,
    )
    .await;

    assert!(!stopped, "warmup must complete, not get stuck or be stopped");
    // Stats should reflect normal mode after warmup completion.
    let s = stats.lock().await;
    assert_eq!(s.delivery_mode, "normal", "warmup should hand off to normal");
}
```

- [ ] **Step 3: Modify run_warmup_loop in rescue.rs to track consecutive Nones and advance**

In `crates/rs-delivery/src/rescue.rs`, locate the `loop { ... }` inside `run_warmup_loop` (starts around line 387). The relevant `match fetcher.chunk_duration_ms(probe_id).await` branches are around lines 389-437.

Add a counter just BEFORE the loop's `let stopped = loop {` (around line 387). Find this block:

```rust
    let mut accum_ms: u64 = 0;
    let mut probe_id = start_chunk_id;
    tracing::info!(
        alias,
        delivery_delay_ms,
        "Warmup started — waiting for buffer target"
    );

    let stopped = loop {
```

Insert immediately before `let stopped = loop {`:

```rust
    // Hardening (#NN): if the same chunk_id returns Ok(None) for too
    // long, advance probe_id rather than spinning silently. Production
    // bug: when start_chunk_id is below S3 live-edge (chunks pruned),
    // the loop hung forever with no log output.
    const CONSECUTIVE_NONE_THRESHOLD: u32 = 30; // 30 × 2s sleep = ~60s
    let mut consecutive_none: u32 = 0;
    let mut stuck_chunk: i64 = probe_id;
```

Then in the `Ok(None)` branch (around line 425), replace:

```rust
            Ok(None) => {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() { break true; }
                    }
                }
            }
```

With:

```rust
            Ok(None) => {
                if probe_id == stuck_chunk {
                    consecutive_none += 1;
                } else {
                    stuck_chunk = probe_id;
                    consecutive_none = 1;
                }
                if consecutive_none >= CONSECUTIVE_NONE_THRESHOLD {
                    tracing::warn!(
                        alias,
                        stuck_chunk,
                        consecutive_none,
                        "Warmup stuck on missing chunk; advancing probe_id"
                    );
                    probe_id += 1;
                    consecutive_none = 0;
                    continue;
                }
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() { break true; }
                    }
                }
            }
```

In the `Ok(Some(_))` branch (around lines 391-422), reset the counter on a successful fetch. Locate the line `accum_ms += dur_ms.max(0) as u64;` and add immediately before it:

```rust
                consecutive_none = 0;
                stuck_chunk = probe_id;
```

- [ ] **Step 4: Audit endpoint_task.rs for the same anti-pattern**

```bash
grep -nE "Ok\(None\)|chunk_duration_ms" crates/rs-delivery/src/endpoint_task.rs
```

If there is an analogous `Ok(None) => sleep` branch in the consumer's main fetch loop that also fails to advance `chunk_id`, mirror the same hardening pattern (consecutive-None counter + advance + WARN log). If there is no such pattern (consumer is expected to wait for the live edge), do NOT modify it. Document the audit result in the commit message.

- [ ] **Step 5: Local format check**

```bash
cargo fmt --all --check
```

Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add crates/rs-delivery/src/rescue.rs crates/rs-delivery/src/rescue_tests.rs
git commit -m "fix(delivery): warmup advances past missing chunks instead of spinning (#NN)"
```

If endpoint_task.rs was also modified, include it in the `git add` and add a one-line note to the commit body: `Mirrors the same fix in endpoint_task.rs main fetch loop.`

---

## Task 6: CI gates that would have caught this regression

**Files:**
- Modify: `.github/workflows/ci.yml` — `e2e-obs-youtube-test` job (starts at line 1726)

- [ ] **Step 1: Locate the insertion point**

Open `.github/workflows/ci.yml` and find the existing GATE step in `e2e-obs-youtube-test` that polls `/api/v1/delivery/status` and prints the per-endpoint table (the `Endpoint '$($ep.alias)': chunks=$($ep.chunks_processed)` line near 3686). The two new GATE steps go IMMEDIATELY AFTER the step that proves `delivery_init_response status=ok` (look for the `delivery_init` keyword around lines 3500-3700) and BEFORE the existing per-endpoint stats print step. The orchestrator will inspect surrounding context to confirm the right insertion point — the goal is "after init succeeded, before the test moves on".

- [ ] **Step 2: Add GATE for init latency ≤ 180s from vps_ready**

Insert this YAML step:

```yaml
      - name: "GATE: delivery_init_sent within 180s of vps_ready"
        shell: powershell
        run: |
          # Hardens against a 15-minute orchestrator wait regression
          # (#NN). The wait loop in delivery.rs:463-491 must not
          # exceed ~180s in normal operation. If duration_ms data is
          # corrupt (regression that landed in PR #144) the wait
          # times out at max_wait_secs=900 -- this gate catches it.
          $ErrorActionPreference = "Stop"
          $events = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/events" -TimeoutSec 10
          $e2eEvent = $events | Where-Object { $_.name -eq "E2E-Test" } | Select-Object -First 1
          if (-not $e2eEvent) { throw "FAILED: no E2E-Test event" }
          $audit = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/audit?event_id=$($e2eEvent.id)&limit=200" -TimeoutSec 10
          $vpsReady = $audit.rows | Where-Object { $_.action -eq "vps_ready" } | Sort-Object ts -Descending | Select-Object -First 1
          $initSent = $audit.rows | Where-Object { $_.action -eq "delivery_init_sent" } | Sort-Object ts -Descending | Select-Object -First 1
          if (-not $vpsReady) { throw "FAILED: no vps_ready audit row for event $($e2eEvent.id)" }
          if (-not $initSent) { throw "FAILED: no delivery_init_sent audit row for event $($e2eEvent.id)" }
          $vpsReadyTs = [DateTime]::Parse($vpsReady.ts).ToUniversalTime()
          $initSentTs = [DateTime]::Parse($initSent.ts).ToUniversalTime()
          $delaySecs = ($initSentTs - $vpsReadyTs).TotalSeconds
          Write-Host "vps_ready=$($vpsReady.ts) delivery_init_sent=$($initSent.ts) delaySecs=$delaySecs"
          if ($delaySecs -gt 180) {
            throw "FAILED: delivery_init_sent took $delaySecs s after vps_ready (max 180s). Likely chunk_records.duration_ms regression -- see issue #NN."
          }
          Write-Host "=== Init latency GATE PASSED: $delaySecs s ==="
```

- [ ] **Step 3: Add GATE for chunks_processed > 0 within 90s**

Insert this YAML step IMMEDIATELY AFTER the init-latency gate above:

```yaml
      - name: "GATE: chunks_processed > 0 within 90s of init"
        shell: powershell
        run: |
          # Catches the silent VPS warmup spin (#NN). After init returns ok,
          # the VPS must start delivering chunks. If it doesn't, the dashboard
          # shows '0 delivered' indefinitely -- exactly the production failure
          # operator hit. Polls every 3s up to 90s.
          $ErrorActionPreference = "Stop"
          $events = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/events" -TimeoutSec 10
          $e2eEvent = $events | Where-Object { $_.name -eq "E2E-Test" } | Select-Object -First 1
          if (-not $e2eEvent) { throw "FAILED: no E2E-Test event" }
          $deadline = (Get-Date).AddSeconds(90)
          $progressed = $false
          $lastChunks = -1
          while ((Get-Date) -lt $deadline) {
            try {
              $status = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/delivery/status?event_id=$($e2eEvent.id)" -TimeoutSec 10
              if ($status.endpoint_details -and $status.endpoint_details.Count -gt 0) {
                $maxChunks = ($status.endpoint_details | Measure-Object -Property chunks_processed -Maximum).Maximum
                if ($maxChunks -ne $lastChunks) {
                  Write-Host "chunks_processed max across endpoints: $maxChunks"
                  $lastChunks = $maxChunks
                }
                if ($maxChunks -gt 0) {
                  $progressed = $true
                  break
                }
              }
            } catch {
              Write-Host "delivery/status poll error (will retry): $_"
            }
            Start-Sleep -Seconds 3
          }
          if (-not $progressed) {
            $finalStatus = Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/delivery/status?event_id=$($e2eEvent.id)" -TimeoutSec 10
            Write-Host "FINAL endpoint_details: $($finalStatus.endpoint_details | ConvertTo-Json -Depth 3)"
            throw "FAILED: chunks_processed stayed 0 for 90s after init. VPS warmup is stuck (likely start_chunk_id below S3 live-edge -- see issue #NN)."
          }
          Write-Host "=== chunks_processed GATE PASSED ==="
```

- [ ] **Step 4: Verify ASCII-only**

```bash
grep -nP '[^\x00-\x7F]' .github/workflows/ci.yml | head
```

Expected: no output. If the YAML editor inserted any unicode (em-dash, smart quotes), replace with ASCII before commit.

- [ ] **Step 5: Verify YAML syntax**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))" && echo "YAML OK"
```

Expected: `YAML OK`.

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: gate OBS-to-YouTube E2E on init latency and chunk progression (#NN)"
```

---

## Task 7: DB defense — `compute_target_start_chunk` falls back on zero-duration data

**Files:**
- Modify: `crates/rs-core/src/db/mod.rs:451-486` (the function body)
- Modify: `crates/rs-core/src/db/upload_tests.rs` (append a new test)

- [ ] **Step 1: Add the failing regression test**

Append to `crates/rs-core/src/db/upload_tests.rs` (after the existing `compute_target_start_chunk_*` tests around line 559):

```rust
/// Defense-in-depth (#NN): if every sent chunk has duration_ms = 0
/// (data corruption from the PR #144 regression), the function previously
/// walked all rows accumulating 0 and returned the OLDEST sequence_number.
/// The orchestrator then sent that as start_chunk_id to the VPS, which
/// pointed at a pruned chunk and hung warmup forever.
///
/// Post-fix: when the accumulator stays at 0 (every walked row has
/// duration_ms = 0), return the LATEST sequence_number so the VPS starts
/// at live-edge with an empty buffer. Degraded UX, but stream actually
/// starts instead of hanging.
#[tokio::test]
async fn compute_target_start_chunk_returns_latest_when_all_durations_zero() {
    let pool = setup_db_for_start_chunk().await;
    let event_id = upsert_streaming_event(&pool, "start-chunk-zero-dur")
        .await
        .unwrap();

    // 100 chunks all with duration_ms = 0 (the corruption pattern).
    for i in 1..=100 {
        let id = insert_chunk(
            &pool,
            event_id,
            &format!("/tmp/z{i}.ts"),
            1000,
            &format!("z{i}"),
            0, // <-- the corruption
        )
        .await
        .unwrap();
        set_chunk_sent(&pool, id).await.unwrap();
    }

    let start = db::compute_target_start_chunk(&pool, event_id, 12_000)
        .await
        .unwrap();
    assert_eq!(
        start, 100,
        "all-zero-duration data: must return latest seq (live-edge fallback), got {start}"
    );
}
```

- [ ] **Step 2: Modify `compute_target_start_chunk` in `crates/rs-core/src/db/mod.rs:451-486`**

Replace the body of the function. Current body:

```rust
pub async fn compute_target_start_chunk(
    pool: &SqlitePool,
    event_id: i64,
    target_ms: i64,
) -> Result<i64> {
    const MAX_WALK_ROWS: i64 = 10_000;

    let rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT sequence_number, duration_ms FROM chunk_records
         WHERE streaming_event_id = ?1 AND sent = 1
         ORDER BY sequence_number DESC
         LIMIT ?2",
    )
    .bind(event_id)
    .bind(MAX_WALK_ROWS)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(1);
    }

    let mut accum: i64 = 0;
    let mut start = rows[0].0; // latest seq as default
    for (seq, dur) in &rows {
        accum += dur;
        start = *seq;
        if accum >= target_ms {
            break;
        }
    }
    Ok(start)
}
```

New body:

```rust
pub async fn compute_target_start_chunk(
    pool: &SqlitePool,
    event_id: i64,
    target_ms: i64,
) -> Result<i64> {
    const MAX_WALK_ROWS: i64 = 10_000;

    let rows: Vec<(i64, i64)> = sqlx::query_as(
        "SELECT sequence_number, duration_ms FROM chunk_records
         WHERE streaming_event_id = ?1 AND sent = 1
         ORDER BY sequence_number DESC
         LIMIT ?2",
    )
    .bind(event_id)
    .bind(MAX_WALK_ROWS)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Ok(1);
    }

    let latest_seq = rows[0].0;
    let mut accum: i64 = 0;
    let mut start = latest_seq;
    for (seq, dur) in &rows {
        accum += dur;
        start = *seq;
        if accum >= target_ms {
            break;
        }
    }

    // Defense-in-depth (#NN): if every walked row has duration_ms = 0
    // (the PR #144 corruption pattern), the loop above walked all rows
    // and returned the OLDEST seq -- which the orchestrator then sends
    // as start_chunk_id to the VPS. That chunk has long been pruned
    // from S3, hanging warmup. Fall back to live-edge so delivery
    // starts (with empty buffer) instead of hanging.
    if accum == 0 {
        tracing::warn!(
            event_id,
            row_count = rows.len(),
            "compute_target_start_chunk: all sent chunks have duration_ms=0; using latest seq as start_chunk_id"
        );
        return Ok(latest_seq);
    }

    Ok(start)
}
```

- [ ] **Step 3: Local format check**

```bash
cargo fmt --all --check
```

Expected: no output.

- [ ] **Step 4: Confirm only two files changed**

```bash
git diff --stat
```

Expected: 2 files (`crates/rs-core/src/db/mod.rs`, `crates/rs-core/src/db/upload_tests.rs`).

- [ ] **Step 5: Commit**

```bash
git add crates/rs-core/src/db/mod.rs crates/rs-core/src/db/upload_tests.rs
git commit -m "fix(db): compute_target_start_chunk falls back to latest on zero-duration data (#NN)"
```

---

## Task 8: Push, monitor CI, create PR, post-deploy verify (orchestrator-only)

This task is performed by the orchestrator, NOT a subagent. Do not dispatch a subagent for it.

- [ ] **Step 1: Local sanity before push**

```bash
cargo fmt --all --check
git log --oneline origin/main..HEAD
```

Expected: 6 commits on top of origin/main:
1. `chore: bump version to 0.3.73`
2. `test(inpoint): assert chunk duration tracks video wall span (#NN)`
3. `fix(inpoint): audio frames must not overwrite chunk_last_ts (#NN)`
4. `fix(delivery): warmup advances past missing chunks instead of spinning (#NN)`
5. `ci: gate OBS-to-YouTube E2E on init latency and chunk progression (#NN)`
6. `fix(db): compute_target_start_chunk falls back to latest on zero-duration data (#NN)`

(Plus the spec commit `a2c616b` that was already on dev when work started.)

- [ ] **Step 2: Push to dev**

```bash
git push origin dev
```

- [ ] **Step 3: Monitor CI to terminal state (single background command, NOT a loop)**

```bash
RUN_ID=$(gh run list --branch dev --limit 1 --json databaseId --jq '.[0].databaseId')
echo "Monitoring run $RUN_ID"
# Single background sleep + view per ci-monitoring rule. Do NOT use /loop or CronCreate.
# Long enough that CI is likely terminal but short enough to react if it fails fast.
sleep 600 && gh run view "$RUN_ID" --json status,conclusion,jobs > /tmp/ci-status.json
```

When the background command returns, parse `/tmp/ci-status.json`. If `status` is not `completed`, repeat the sleep+view (use 300s for the second poll since the run has already been alive ~10 min). If `conclusion != success`, capture failed-job logs:

```bash
gh run view "$RUN_ID" --log-failed | head -200
```

Investigate the failure, fix it (one commit per fix, push, monitor again). Repeat until ALL jobs are green — including `e2e-obs-youtube-test` with both new GATE steps passing. Do NOT skip or rerun without root-cause analysis.

- [ ] **Step 4: Create the PR (only after dev CI is fully green)**

```bash
gh pr create --base main --head dev \
  --title "fix: chunk_records duration_ms=0 regression (#NN) -- production delivery hung" \
  --body "$(cat <<'EOF'
## Summary
- Fix the chunker regression from PR #144 that produced `duration_ms = 0` for every chunk_record, hanging production delivery for 15+ minutes (orchestrator wait timeout) followed by indefinite VPS warmup spin on a pruned `start_chunk_id = 1`.
- Add CI gates on `e2e-obs-youtube-test` that catch both symptoms (init-latency > 180s, chunks_processed = 0 after 90s).
- Harden VPS warmup loop to advance past missing chunks instead of silently spinning.
- Defend `compute_target_start_chunk` so future zero-duration corruption degrades to live-edge start instead of hanging.

Closes #NN.
Spec: `docs/superpowers/specs/2026-04-27-chunk-duration-zero-fix-design.md`.
Plan: `docs/superpowers/plans/2026-04-27-chunk-duration-zero-fix.md`.

## Test plan
- [ ] CI green (all jobs including mutation-testing, coverage, build, e2e-streaming, e2e-obs-youtube, deploy-stream-lan)
- [ ] New CI gates pass on `e2e-obs-youtube-test` (init latency, chunks_processed)
- [ ] Post-deploy on stream.lan: operator triggers fresh OBS stream, dashboard transitions IDLE -> STREAMING within 3 min, S3 -> VPS shows MMM > 0 within 90s
- [ ] SQL spot-check on stream.lan after fresh stream: `MIN(duration_ms) > 0` for new chunk_records

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5: Monitor PR CI to clean+mergeable**

```bash
PR_NUM=$(gh pr list --head dev --json number --jq '.[0].number')
sleep 600 && gh run list --branch dev --limit 3 --json databaseId,status,conclusion,name
gh api repos/zbynekdrlik/restreamer/pulls/$PR_NUM --jq '{mergeable: .mergeable, mergeable_state: .mergeable_state}'
```

Expected: all jobs `success`, `mergeable: true`, `mergeable_state: "clean"`. Do NOT report green if the state is `unstable` or any check is failing — see `autonomous-quality-discipline`. If a check fails, fix the root cause and push again.

- [ ] **Step 6: Post-deploy verification on stream.lan**

After the `deploy-stream-lan` job completes (it deploys v0.3.73 to stream.lan):

a. Verify the deployed binary version via MCP:

```
mcp__win-stream-snv__Shell:
  Get-Process Restreamer | Select-Object Id, FileVersion, SessionId, StartTime
```

Expected: `FileVersion: 0.3.73`, `SessionId: 1` (NOT 0), recent `StartTime`.

b. Verify the API responds:

```
mcp__win-stream-snv__Shell:
  Invoke-RestMethod -Uri "http://127.0.0.1:8910/api/v1/status" -TimeoutSec 5
```

c. Open the dashboard via Playwright at `http://10.77.9.204:8910/`. Take a full-page screenshot. Check browser console for zero errors/warnings.

d. Operator action required: ask the user to trigger a fresh OBS stream (they were already running a manual test, may already have it primed). Then verify via Playwright + MCP:
   - Dashboard transitions IDLE → STREAMING within 3 min of clicking Start Delivering
   - `S3 → VPS: NNN queued → MMM delivered` shows MMM > 0 within 90s of init
   - No "BUFFERING" header longer than 3 min after init

e. SQL spot-check on stream.lan once a fresh stream has been running for at least 60 seconds:

```
mcp__win-stream-snv__Shell:
  python -c "import sqlite3; c=sqlite3.connect('C:/ProgramData/Restreamer/restreamer.db'); print(c.execute('SELECT streaming_event_id, COUNT(*), MIN(duration_ms), AVG(duration_ms), MAX(duration_ms) FROM chunk_records WHERE sent=1 AND id > (SELECT MAX(id)-100 FROM chunk_records) GROUP BY streaming_event_id').fetchall())"
```

Expected: `MIN(duration_ms) > 0` (typical 1900-2100 ms for 2-second chunks). If MIN = 0, the fix did NOT fully take — investigate before declaring done.

- [ ] **Step 7: Send completion report**

Per `completion-report.md`. Format:

```
## ✅ Work Complete

**Audits & deploy:**
✅ CI: green
✅ /plan-check: 7/7 fulfilled
✅ /review: clean — 0 🔴 0 🟡
✅ Deploy: v0.3.73 on stream.lan, fresh OBS stream delivered MMM chunks within Ns of init, MIN(duration_ms)=Xms

**Plan steps:**
- Filed #NN with root-cause evidence
- Bumped version to 0.3.73
- Added failing chunker test + deleted PR #144's bug-encoding test
- Removed `chunk_last_ts = ts` from write_audio (the regression)
- Hardened VPS warmup to skip missing chunks after 60s
- Added two CI gates on e2e-obs-youtube-test (init latency, chunk progression)
- Defended compute_target_start_chunk against zero-duration data

**E2E test coverage:**
| Feature/Fix | E2E Test File | What It Verifies |
|---|---|---|
| Init latency budget | `.github/workflows/ci.yml` (e2e-obs-youtube-test) | delivery_init_sent within 180s of vps_ready |
| Chunk progression | `.github/workflows/ci.yml` (e2e-obs-youtube-test) | endpoint_details[0].chunks_processed > 0 within 90s of init |

---

**Goal:** Stop the dashboard from showing "BUFFERING" forever after you click Start Delivering, and put a CI test in place so the same regression cannot ship again.
**What changed:** The audio fix from PR #144 was overwriting an internal counter that's used to compute chunk durations, which made the orchestrator wait 15 minutes before initing the VPS and then hand it a chunk-id that was already deleted from S3. Audio frames no longer touch that counter. Two new CI assertions catch both halves of the failure if it ever returns.

**[restreamer] PR #<N>: fix: chunk_records duration_ms=0 regression -- production delivery hung**
<full PR URL> -- mergeable, clean
🌐 Dashboard: http://10.77.9.204:8910/
```

---

## Verification Checklist

| Acceptance criterion | How verified |
|---|---|
| `cargo fmt --all --check` clean | Local in every task; CI lint job |
| All CI jobs green | `gh run view <id>` after dev push and PR push |
| New init-latency GATE passes | CI `e2e-obs-youtube-test` job log shows "Init latency GATE PASSED" |
| New chunks_processed GATE passes | CI `e2e-obs-youtube-test` job log shows "chunks_processed GATE PASSED" |
| PR `mergeable: true` AND `mergeable_state: "clean"` | `gh api ... pulls/N --jq '{mergeable, mergeable_state}'` |
| Stream.lan binary v0.3.73 in user session | MCP `Get-Process Restreamer` shows FileVersion 0.3.73, SessionId 1 |
| Fresh stream: dashboard IDLE→STREAMING ≤ 3 min | Playwright snapshot of dashboard |
| Fresh stream: chunks_processed > 0 within 90s | MCP API call `/api/v1/delivery/status?event_id=...` |
| New chunks have MIN(duration_ms) > 0 | MCP SQL query on stream.lan |

---

## File-Level Diff Plan

| File | Change | Tasks |
|---|---|---|
| `Cargo.toml` | version 0.3.72 → 0.3.73 | 2 |
| `src-tauri/Cargo.toml` | version | 2 |
| `src-tauri/tauri.conf.json` | version | 2 |
| `leptos-ui/Cargo.toml` | version | 2 |
| `crates/rs-inpoint/src/flv_chunker.rs` | delete bug-test, add 2 new tests, delete `chunk_last_ts = ts` from `write_audio`, doc comment update | 3, 4 |
| `crates/rs-delivery/src/rescue.rs` | consecutive-None counter + skip-forward + WARN log | 5 |
| `crates/rs-delivery/src/rescue_tests.rs` | new GapMockFetcher + skip-forward test | 5 |
| `crates/rs-delivery/src/endpoint_task.rs` | mirror skip-forward pattern IF analogous code exists | 5 (audit) |
| `.github/workflows/ci.yml` | 2 new GATE steps in e2e-obs-youtube-test | 6 |
| `crates/rs-core/src/db/mod.rs` | accum==0 fallback to latest seq + WARN log | 7 |
| `crates/rs-core/src/db/upload_tests.rs` | new test for zero-duration fallback | 7 |

Total: 11 files. Net diff under ~250 lines (additions outnumber deletions).
