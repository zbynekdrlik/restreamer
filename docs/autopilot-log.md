# Autopilot Log

Terse per-issue record of autonomous autopilot work (decisions, SHAs, RED→GREEN tests, PRs).

---

## 2026-07-25 — Batch #267 + #281 + #268 (one PR, v0.29.18, CI-infra only)

Bundled batch on `dev`, version bump `0c412de1` (0.29.17→0.29.18, 4 files). No Rust/JS/Python
source changed — all three are `.github/workflows/ci.yml` / `.cargo/audit.toml` / `CLAUDE.md` only.

- **#267 (CI gate) — e2e/deploy jobs ran on non-compiling code:** `e712da53`. `e2e-streaming-test`,
  `e2e-obs-youtube-test`, `e2e-fb-push-stream-lan` gated on `needs.deploy-stream-lan.result !=
  'failure'`; since `'skipped' != 'failure'` is true, a compile/lint/test failure that made
  deploy-stream-lan SKIP let the ~1.5h real-YouTube/FB e2e run anyway (proven in run 27884399527).
  Switched all three to `== 'success'`; `e2e-fb-push-stream-lan` additionally requires both upstream
  e2e jobs `== 'success'` (it previously only checked deploy). Updated the pre-existing
  `verify-ci-yaml-invariants` self-check (`Verify E2E tests use == success`) that had asserted the
  OLD `!= 'failure'` invariant — this self-check IS the regression guard (dogfoods the same pattern
  as the repo's existing `Verify deploy-stream-lan has always()` / `Verify auto-release has
  always()` checks). `deploy-stream-lan`'s own `always()`-gated condition untouched.
- **#281 (CI gate) — cargo-audit ignore list moved to committed `.cargo/audit.toml`:** `c68932ca`.
  8 inline `--ignore RUSTSEC-...` flags on the `cargo audit` command (grown one flag at a time
  across 5+ commits, no single reviewable place) replaced by `.cargo/audit.toml` (confirmed via
  cargo-audit 0.22.0's own `config.rs` + a local test run that project-local `.cargo/audit.toml` is
  auto-read, no `--ignore` flags needed). Every entry carries `# RUSTSEC-ID: <why> | dep: <path> |
  expires: <date>`. New "Validate audit.toml" CI step parses + fails the build once an entry
  expires. Investigated live: only 3 of the 8 ignored IDs (`RUSTSEC-2023-0071` rsa,
  `RUSTSEC-2025-0134` rustls-pemfile unmaintained, `RUSTSEC-2026-0194`/`0195` quick-xml) still match
  the current lockfile; `0037`/`0049`/`0098`/`0099` (quinn-proto/rustls-webpki) are now stale no-ops
  (deps already upgraded past the patched versions) — kept per the decided scope ("carry every
  currently-ignored ID") rather than pruned in this PR, documented inline in `audit.toml`.
- **#268 (CI gate, rescoped) — stale `refs/pull/N/merge` race:** `91e9ad3e`. Added "Verify merge ref
  is not stale" step to the existing `version-check` job (pull_request-only, already full-history
  checked out, already required by `rust-ci-gate` on PRs): fails fast when `HEAD^2` (the checked-out
  merge commit's second parent, i.e. the code under test) doesn't match
  `github.event.pull_request.head.sha` (the PR's real current head) — GitHub can serve a cached
  merge ref after a close/reopen, silently testing stale code for a full ~2h e2e cycle (2026-06-21
  incident, PR #266/#255). The push+pull_request double-fire half of the original #268 report is
  tracked separately in #272, out of scope here.
- **Filed #322** (out of scope, discovered incidentally): `Cargo.lock` local-workspace-member
  version fields were stale (0.29.5 vs Cargo.toml's 0.29.17) and missing an `rs-core -> reqwest`
  edge already declared in `crates/rs-core/Cargo.toml`. Self-heals on every CI run (no `--locked` on
  the main build), so left untouched (matches this log's own 2026-07-21 precedent of tolerating a
  stale Cargo.lock for path deps). Reverted a local `cargo audit`/`cargo tree` side-effect on
  Cargo.lock before committing, to keep the diff scoped to #267/#268/#281.
- **`[no-test: <reason>]`:** CI-YAML/TOML conditional-logic fixes, no Rust/JS/Python source touched.
  Each fix's regression guard is the meta self-check step (grep-based assertion on the workflow
  file's own condition strings) added/updated in its own commit — the SAME established pattern this
  repo already uses for `deploy-stream-lan`'s `always()` and `auto-release`'s `e2e-gate` dependency
  (precedent: 2026-07-19 `#287 (no label) — CI gate` entry above, `[no-test]` "CI YAML gate = the
  test"). `pre-push-test-check.sh`'s Gate 2 doesn't recognize this pattern as a test (its inline-test
  regex has no YAML-self-check case, and a `sys.exit(1)` in the #281 validator's Python
  false-positive-matched its `it\(` heuristic on later commits, masking the real gap) — flagging
  `e712da53` specifically. Root-cause verified end-to-end by this very push's own CI run (positive
  path: deploy succeeds, e2e runs; #267's negative "skip" path is exactly what the *previous* buggy
  behavior demonstrated, and cannot be re-proven without deliberately breaking the build).
- **PR:** dev→main, body Closes #267 #281 #268. (CI run-id + merge SHA appended by supervisor.)

---

## 2026-07-21 — Batch #68 + #169 + #244 (one PR, v0.29.7)

Bundled batch on `dev`, version bump `9001e33f` (0.29.6→0.29.7, 4 files).

- **#244 (bug) — orphaned Hetzner VPS never deleted:** RED `107d307a` (`delivery_orphan_tests::start_delivery_deletes_orphaned_stale_vps`, wiremock DELETE `.expect(1)`) → GREEN `7ef3a79a`. New `DeliveryOrchestrator::cleanup_orphan_delivery_vps(instance_id, event_id, trigger)` deletes the VPS + marks row `deleted` + audits `vps_deleted` (reason `start_failed`/`stale_row`/`delete_error`). Wired at BOTH leak sites: poll_and_init failure handler (delivery_handlers.rs) AND start_delivery #165 stale-row cleanup (delivery.rs). Decision: immediate-delete + audit over a TTL/failed-list (audit trail carries post-mortem, per #75). RED proven on dev2 (Verifications failed pre-fix).
- **#169 (feature) — activity-log burst grouping:** `575b61fb` (+ `bfb194f0` GroupedRow PartialEq for the leptos Memo bound). Decision: CLIENT-SIDE grouping in audit_panel.rs (covers LIVE WS rows; a server-only group misses them) — a documented wasm-side mirror of `rs_core::db::audit::group_audit_rows` (leptos can't dep on native rs-core); ALSO wired that pure fn as its first production caller via `?group=true` in audit_handlers.rs. "Group bursts" toggle default ON (window 0 = ungrouped). rs-api grouping tests + Playwright toggle E2E.
- **#68 (feature) — guided "Change Key" flow:** `ebdc635f`. "Key" button on each live endpoint node → `ChangeKeyModal` runs remove→update_endpoint→add(Live) in one click (re-add re-reads fresh DB config incl. new key). endpoints.rs edit-form live-delivery warning banner. New mock `_test/change-key-ops` recorder + Playwright E2E asserting the ordered sequence.
- **Cargo.lock:** left stale workspace-member versions (0.29.5) — cargo tolerates for path deps; matches the green 0.29.6 release (workspace build not `--locked`).
- **dev2 verify:** clippy `--workspace --all-targets -D warnings` clean; full `cargo test --workspace` green (3 new tests named-confirmed); leptos wasm compile clean; frontend E2E change-key + audit-panel-grouping + remove-last-endpoint all pass.
- **PR:** dev→main, body Closes #68 #169 #244. (CI run-id + merge SHA appended by supervisor.)

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

## 2026-07-20 — #294 + #295 (fast-delay CI round, v0.29.3)

- **Version:** bumped 0.29.2 → 0.29.3 (all 4 files) + Cargo.lock caught up from stale 0.29.0. `b7d70ae1`.
- **#294 (bug) — lag-ladder re-arm:** RED `b47b17f6` (`fast_endpoint_corrects_blind_band_drift_without_lowering_the_buffer`) → GREEN `f3231f5b`. The ladder's first rung was `2*delay`, so drift in [1x,2x) of the ratcheted target was invisible; the removed healthy-shrink used to re-arm it. Fix: fast endpoints start the ladder at a small constant rung (`LAG_PROBE_FAST_FIRST_RUNG=2`), run more rungs (`LAG_PROBE_FAST_LADDER_MAX=16`) to keep reach, and binary-refine (`pin_live_edge`) to pin the exact edge. Buffer never lowered (`target = max_id - delay`, max_id proven to exist ⇒ ≥ delay behind live). Delayed endpoints unchanged (skipping their content would be a jump-cut). Param cleanup `390b749f` (drop dead `delivery_delay_ms` to clear clippy too_many_arguments).
- **#294 respawn integration test:** `587068b2` — drives a FAST endpoint through a real #237 producer panic via `endpoint_loop`/`PanicOnceFetcher`, asserts `fast_delay_target_secs` survives. Mutation-verified on dev2: drop seed `.load()` → 5s FAIL; drop grow `.store()` → 0s FAIL.
- **#294 review finding 2 (bounded gentle decay): NOT implemented** — operator explicitly forbade any downward buffer movement (issue comment 2026-07-20). Decay = the `on_healthy` shrink that caused the original bug.
- **#295 (bug) — dashboard RED for a held ratchet:** RED `45d4685f` → GREEN `3434f0c7`. Fast bar was `secs>8 ⇒ critical` (assumed 2-5s near-live) while #294 ratchets 5-120s. New `fast_buffer_class(secs, target)` colours RELATIVE to the reported target (≤ target×1.25 healthy, >×2 critical), falls back to old absolute bands when no target. Threaded `fast_delay_target_secs` additively rs-delivery→rs-api→leptos (av_skew_ms path, NOT producer_active). Display cap raised 30→120s. Browser E2E `5517acd8` (2 specs, dev2 frontend suite 102/102).
- **Filed:** #297 (leptos-ui unused pub-use warnings on wasm build — pre-existing, out of diff).
- **Playbook:** new `.claude/skills/dev2-build-verify` (dev2 build/test/clippy/frontend-E2E procedure); wire-path gotcha added to `streaming-boxes`.
- **PR:** dev→main, body Closes #294 #295. (CI run-id + merge SHA appended by supervisor.)

## 2026-07-25 — Batch #322 #325 #278 (Cargo.lock drift + self-check grep fragility + S3-region guard, v0.29.19)

- **Version:** bumped 0.29.18 → 0.29.19 (all 4 files). `9fa754d1`.
- **#325 (no label) — ci.yml self-check grep anchoring:** `d0f90619`. All 7 job-name-substring greps in `verify-ci-yaml-invariants` (`deploy-stream-lan:`, `auto-release:` x2, `e2e-streaming-test:`, `e2e-obs-youtube-test:`, `e2e-fb-push-stream-lan:`, `e2e-gate:`) anchored to `^  <job-name>:` (2-space job indent) so each matches exactly the real job definition line, never its own source line / the `# See deploy-stream-lan: ...` comments / the e2e-gate summary echo block. Verified all 7 self-check pipelines still resolve to the correct string post-edit. `[no-test]`-class CI-YAML fix (regression guard IS the self-check).
- **#322 (no label) — Cargo.lock drift:** two commits, ORDER MATTERS. `e1a97ace` regenerates the lock (all 11 local members were stuck at 0.29.5 vs Cargo.toml's 0.29.19; `rs-core`'s `reqwest.workspace = true` edge was missing) via `cargo metadata` on dev2 — diff is minimal, only local-member versions + the one edge, no transitive dep churn. `a969fa19` (committed AFTER, per the ticket's sequencing note) adds `--locked` to the 5 real workspace commands (clippy, test x2, build-delivery, test-integrity's zero-ignored check) so CI stops silently self-healing lock drift.
- **#278 (no label) — S3 region guard, RESCOPED to signal-only (no auto-override, no validate() rejection):** `b8cd27bb`. `STANDARD_S3_REGION="fsn1"` const + `Config::s3_region_is_standard()` in rs-core::config; `Action::S3RegionNonStandard` (Severity::Critical, emitted ONCE at orchestrator startup); `s3_region_standard: bool` threaded through `/api/v1/status` (computed fresh from `config_live`, not the startup snapshot) AND the Tauri IPC `get_status` command (the tray app is the real prod deploy, not just the LAN browser path — easy to forget since `disk_pressure` needed the same dual-wiring back in #234); new `S3RegionBanner` leptos component mirroring `DiskPressureBanner`. Fixed the stale `streaming-boxes` skill doc (said streampp=nbg1, corrected to fsn1 per the 2026-06-24 migration).
- **Playbook:** `.claude/skills/dev2-build-verify` gained 3 entries — (1) the warm checkout can be missing entirely, bootstrap via `mkdir -p` + the same rsync; (2) the rsync's `--exclude '*.png'` breaks `trunk build` on a fresh checkout (index.html references icon-*.png) — one-time `scp` fix; (3) **`e2e/tauri-mock.js`'s `get_status` hand-composes its OWN response object and does NOT forward new `/api/v1/status` fields automatically** — a field added only to `mock-api.js` silently vanishes on the Tauri-IPC path the WHOLE frontend suite actually exercises (`window.__TAURI__` is always injected); (4) `playwright-frontend.config.ts` hardcodes `workers: 1` on purpose (shared mock-api.js state) — a `--workers=2` full-suite run produces real-looking cross-test-contamination failures that vanish at `workers:1`.
- **PR:** dev→main batch, body Closes #322 #325 #278. (CI run-id + merge SHA appended by the completion evidence.)
