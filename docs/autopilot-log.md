# Autopilot Log

Terse per-issue record of autonomous autopilot work (decisions, SHAs, RED→GREEN tests, PRs).

---

## 2026-06-20 — #252 Crash-recovery: resume actively-delivering event on boot

- **Issue:** #252 (bug, P0) — restarting Restreamer.exe after a stream.lan crash did ZERO delivery resume (no delivery re-init / health-monitor re-arm / fast-cache repopulation). Operator's #1 production failure.
- **Validated still real:** ticket-validator STILL_VALID against dev v0.23.0 HEAD 858ff30f; confirmed `ServiceCore::run_with_signal` only called `resume_pending_grants`, zero delivery reconciliation at boot.
- **Version:** 0.23.0 → 0.24.0 (bump commit `b6efa542`; all 4 files + Cargo.lock).
- **RED→GREEN:**
  - RED `1b49fb85` — `crates/rs-api/src/crash_recovery_tests.rs:108` `boot_reconcile_reinits_delivering_event` (no-op stub → fails: no tracked task / empty fast-cache). Proven RED on dev2 (panic at the poll-handle assertion).
  - GREEN `a131ccdd` — `DeliveryOrchestrator::reconcile_delivery_on_boot` in new `crates/rs-api/src/delivery_recovery.rs`, wired into `ServiceCore::run_with_signal` after `resume_pending_grants`. Re-arms poll_and_init→monitor_delivery_health + repopulates endpoint_fast_cache. Extracted s3-wipe seam into `delivery_s3_wipe.rs` to keep `delivery.rs` < 1000-line cap.
  - Review-fix `075c8389` — code-review found 3 correctness bugs: (1) partial resume-seed → full backlog replay (violates strict-1x), (2) the resume_positions branch was dead code now newly-activated + lacks the #174 S3-existence guard, (3) double-spawn race vs operator Start-Delivering. Fix: resume at LIVE EDGE (do NOT seed resume_positions; poll_and_init takes its tested live-edge branch for every endpoint) + contains_key guard before poll_handle insert. Tests updated (assert resume_positions stays empty) + new `boot_reconcile_does_not_overwrite_existing_poll_handle`.
- **Decisions:** Live-edge resume chosen over per-endpoint position replay (the safe, strict-1x-correct behavior — re-pushing backlog gets streams killed by YT/FB). Local builds impossible on dev1 (OOM) → all compile/clippy/test verification on dev2 (rs-api+rs-runtime compile, clippy --all-targets -D warnings clean, 4 crash_recovery + 75 delivery tests pass).
- **CI:** push run `27878103703` SUCCESS (all jobs incl. Deploy to stream.lan, E2E OBS-YouTube **crash-recovery test**, E2E Streaming, E2E FB Push real-FB, E2E Gate). PR-event run `27878104852` SUCCESS.
- **PR:** #264 https://github.com/zbynekdrlik/restreamer/pull/264 (body `Closes #252`) — mergeable: true, mergeable_state: clean. NOT merged: this project is user-merge-only (memory rule "Never merge PRs - only user merges"; PRs #242-254 all merged by zbynekdrlik). Awaiting user merge.
- **Deploy:** dev-push CI already deployed v0.24.0 to stream.lan; verified — dashboard DOM `0.24.0-dev`, Restreamer.exe SessionId=1, API OK, boot-reconcile took correct no-op path for non-delivering current event 9316. (Crash-recovery E2E validated the resuming path on the live box.)
- **Follow-up filed:** #265 — extract shared `spawn_delivery_task` helper (3-way poll_and_init→monitor duplication) + drop dead `_cached_delivery` param.

## #255 — A/V re-anchor on OBS mid-stream republish (v0.25.0)
- Commits: bump 897d5540 → RED 6e664a4b (test republish skew) → GREEN 5a091917 (start_new_session + audio rebase + media_receiver wiring) → 480a07d1 (new_for_test fields)
- RED→GREEN tests: flv_chunker_tests.rs::republish_keeps_audio_and_video_aligned (RED 6e664a4b, GREEN 5a091917), start_new_session_preserves_chunk_index_and_sequence_headers; updated audio_flv_tag_carries_xiu_timestamp → audio_flv_tag_is_session_relative_and_preserves_xiu_deltas (session-relative audio, deltas preserved)
- Decision: audio rebased onto shared 0-based session epoch (audio_out = xiu_ts - audio_session_origin_xiu) re-captured on start_new_session()/backward-jump; video re-zeroes via session_start_wall_clock_ms=0; wired into media_receiver Publish boundary. Preserves #142 chipmunk fix (deltas untouched).
- CI: push run 27886472274 all-green (RED→GREEN proof on ubuntu+windows). PR pull_request run hit a stale-merge-ref race (built old head 5a091917 without new_for_test fix) — re-triggered.
- PR #266 (Closes #255).

## #257 feat(rtmp-push): A/V-skew detection + bounded recovery + symmetric reanchor (2026-06-21)
- Version: 0.26.0 -> 0.27.0 (commit debf0a73)
- RED 6f702e30: skew.rs::audio_lagging_video_by_25500ms_trips_recovery_after_debounce + pusher.rs::symmetric_reanchor_collapses_drifted_offset_to_zero (guard unwired -> propagates desync silently / per-track reanchor freezes offset)
- GREEN ccd5456d: SkewTracker (content-PTS shared-epoch metric), PushError::AvSkewExceeded{skew_ms}, PusherState::reanchor symmetric (max+1 both tracks), av_skew_ms telemetry PusherState->EndpointStats->VPS/api->host->leptos
- Fix 4190cf7c: av_skew_ms in rs-api integration-test live_metrics() literal (E0063 catch from pre-push review fork)
- New module crates/rs-rtmp-push/src/skew.rs (kept pusher.rs at 833 lines, under 1000 cap)
- PR #270 dev->main, Closes #257
- Decisions: content-PTS metric (NOT container a_out/v_out which is blind to the masked desync per 2026-06-19 telemetry); strict-1x recovery via clean reconnect only; debounce(3) + 60s rate-limit prevent reconnect thrash; one-track streams never trip (both_tracks_seen gate)

## #227 fix(ci): FB-push gate slow-vs-dead VPS boot + sustained cache-overshoot (folded into #258 PR #271, v0.28.0)
- Root cause: FB registration GATE (ci.yml ~5666) blindly waited a fixed 180s then HARD-FAILED; it polled ONLY endpoint_details for alias "e2e fb" and never read instance lifecycle, so a normal Hetzner boot tail (>180s) was reported as a hard gate failure (run 27931333149: polled 18x, VPS still booting). The #258 A/V gate (E2E OBS-YT) itself PASSED — only FB-push flaked.
- Fix 1 (registration gate): now reads $status.instance_status each poll. PASS when "e2e fb" registers; FAST-FAIL when instance_status in failed/deleted/stopping or row vanished after appearing (poll_and_init errored — delivery_handlers.rs:176 writes "failed"); KEEP WAITING while creating/booting/initializing/delivering up to a bounded 5-min absolute cap (typical 30-90s + Hetzner tail; OBS-YT sibling allows 15 min). Logs instance_status+elapsed each poll. timeout-minutes 4->7.
- Fix 2 (overshoot, ci.yml ~3436): init-phase cache-overshoot now SUSTAINED (3 consecutive over-cap samples for the same alias) instead of one instantaneous sample. Cache legitimately spikes then settles during warmup; a single transient over-cap reading is jitter. A settled overshoot (3 straight) still fails — real gate preserved.
- Fix 3 (#227 #1 scope): YT progression + overshoot loops scoped to CI-owned aliases ($ciOwnedAliases = "e2e rtmp","e2e hls"); stray endpoints (leftover soak / operator FB) attached to E2E-Test no longer poison the gate (2026-05-20 root cause).
- CI YAML only; no Rust change. ASCII-only PowerShell. PR #271 now Closes #258 + #227.

## 2026-07-10 — Batch #289 #288 #287 (rescue persistence + calm-banner E2E green + gate hardening, v0.29.1)
- **Why:** main's e2e-gate RED on the "Long outage" ASSERT 3 (calm `.banner--recovering` missing during a sustained outage) blocked `auto-release` → no release tag since v0.29.0. Live-confirmed the failing step is `FAILED: NO calm recovering banner` on dev HEAD d431ddaa (which ALREADY carries the #288 host-side fix), proving #289 is the remaining root cause. E2E Streaming's "364 chunks still pending" was a cascade (Long-outage threw at ASSERT 3 before unblocking S3).
- **#288 (bug, needs-decision) — ALREADY FIXED on dev:** commit d431ddaa (poll_delivery_metrics preserves last-known endpoints on a failed VPS poll). No new code. Removed the stale `needs-decision` label (no open design question; real blocker was #289). Filed follow-up #290 (VPS-side root cause: why /api/status stalls ~40s during rescue).
- **#289 (bug) — CORE:** rescue EXIT tracked flapping producer_active.
  - RED `fc17e620` — `rescue_behavioral_tests.rs::outage_rescue_persists_when_producer_active_flaps_without_fresh_chunks` (producer_active stuck true + highest_sent frozen → asserts loop does NOT exit; RED on old code which exits ~120s in). Also corrected `rescue_push_resumes_normal_when_producer_recovers` to model genuine recovery (background producer advancing highest_sent).
  - GREEN `de88e8eb` — `rust_rescue_push_with_pusher` now exits rescue only when producer_active continuous for RESCUE_REFILL_TARGET_SECS AND `highest_sent_chunk_id` advanced during the window (fresh chunks genuinely queued). `highest_sent_chunk_id` = fetch_max capped at the pre-outage live edge, so respawn churn / stale-tail re-fetch never advance it → rescue stays latched (banner-worthy) through a sustained/trickle outage; genuine recovery (chunks flow after S3 unblock) still exits after 120s → rescue_recovered (ASSERT 5). delivery_mode="recovering" only when fresh chunks flow, else "rescue". Rescue ENTRY (#280/#284) untouched.
  - Decision: discriminator is `highest_sent_chunk_id` advance, NOT continuous-advance-per-tick (channel backpressure plateaus it during rescue) — a >0 delta over the continuous-active window; trickle-with-gaps already handled by the existing reset-on-`producer_active==false`.
- **#287 (no label) — CI gate:** `fix(ci)` `aa73d4cc`. Phase 2 of the crash-exhaustion gate iterated `foreach ($ep in $st.endpoints)` with no non-empty guard → vacuous pass on empty endpoints. Now iterates pre-kill `$aliases` with a count guard on `$st.endpoints`. ASCII-only PowerShell. `[no-test]` (CI YAML gate = the test).
- **Version:** dev already 0.29.1 (all 4 files) > main 0.29.0 — no bump.
- **PR:** dev->main batch, body Closes #289 #288 #287. (SHAs above; log commit + PR/CI ids appended in the completion evidence.)
