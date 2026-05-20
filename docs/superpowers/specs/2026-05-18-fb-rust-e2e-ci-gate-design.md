# FB Rust E2E CI Gate + Real-FB Verified Soak — Design Spec

**Date:** 2026-05-18
**Closes:** #177, #217
**PR:** PR2 of two-PR FB-completion commitment (PR1 = #218, v0.18.0 shipped CONNECT AMF compliance + migration v29)

## 1. Goal

Lock Facebook rust RTMPS delivery in CI on every push to `dev` and `main`, with the same SOTA verification bar as YouTube. Real FB Live Producer must show the rust-pusher feed (preview visible, healthy stream indicators) for ≥30 minutes sustained, zero `endpoint_rtmp_push_died` events on the FB endpoint, on every push. If the gate fails, the PR is not mergeable.

## 2. Why

PR1 (#218) shipped the CONNECT AMF fix and a mock-server integration test. Real FB never observed receiving rust-pusher bytes since the regression report. User has been burned multiple times by FB being declared fixed while unverified. Live event upcoming in a few days requires hard proof, not promises.

Unit tests + mock servers are defense-in-depth regression guards. They are NOT proof FB accepts the bytes. Only a real FB Live Producer broadcast observed receiving the feed proves the system works.

## 3. Architecture (locked-in; mirror e2e-obs-youtube-test)

The CI job `e2e-fb-push-stream-lan` is structurally identical to `e2e-obs-youtube-test` (`.github/workflows/ci.yml` line ~1786). Reuses the stream.lan self-hosted runner, the Hetzner Cloud orchestrator, the rs-delivery binary published to S3, and the persistent-browser-profile Playwright pattern from `youtube-studio-check.spec.ts`.

### 3.1 CI job placement

- Job key: `e2e-fb-push-stream-lan`
- Runner: `[self-hosted, windows, stream-lan]`
- Depends on: `deploy-stream-lan`
- Trigger condition: `always() && needs.deploy-stream-lan.result != 'failure' && (github.ref == 'refs/heads/dev' || github.ref == 'refs/heads/main') && (github.event_name == 'push' || github.event_name == 'workflow_dispatch')`
- Timeout: 60 minutes
- Wired into `e2e-gate` aggregator alongside `e2e-streaming-test` and `e2e-obs-youtube-test`

The pre-existing `e2e-fb-push` job (ubuntu-hosted, schema/ingest sanity check shipped in PR #218) is REMOVED. PR2 replaces it entirely with the stream-lan job. The mock-server integration test (`crates/rs-rtmp-push/tests/fb_mock_server.rs`) stays — fast AMF regression guard, independent of this gate.

### 3.2 Flow per CI run

1. **Configure Hetzner credentials** on stream.lan via the existing PowerShell pattern from `e2e-obs-youtube-test` (write to `C:\ProgramData\Restreamer\config.json`, restart `RestreamerGUI` scheduled task, poll `/api/v1/status` until alive).
2. **Seed FB config** via new CI-only endpoint `POST /api/v1/facebook/config/seed`. Body: `{ alias: "e2e fb", stream_key: "<from FB_TEST_STREAM_KEY secret>" }`. Endpoint creates-or-updates one endpoint row: `alias='e2e fb'`, `pusher='rust'`, `service_type='FB'`, `stream_key=<key>`. Idempotent.
3. **Launch OBS** on stream.lan (reuse OBS startup PowerShell from `e2e-obs-youtube-test`). OBS pushes its `Stream_Obs` profile output to the local RTMP ingest at `rtmp://127.0.0.1:1935/live`.
4. **Start delivery**: `POST /api/v1/delivery/start` with `event_id` of the dedicated E2E event. DeliveryOrchestrator spawns Hetzner VPS, rs-delivery pulls binary from S3 (nbg1), opens RTMPS to FB endpoint.
5. **Parallel verification (both must pass for 30 min sustained):**
   - **Local watchdog (PowerShell, foreground step):** polls `/api/v1/delivery/status` every 10 s for 30 min. Hard-fails if `alive=false`, `chunks_pushed` does not advance between two consecutive polls, `endpoint_rtmp_push_died > 0` in the audit, OR `bytes_sent_since_connect` not strictly growing across the 30 min.
   - **FB-side check (Playwright `e2e/fb-live-producer-check.spec.ts`, foreground step):** opens FB Live Producer for the test broadcast via persistent Chrome profile at `C:\Users\newlevel\.playwright-fb-profile`. Polls every 60 s for 30 min. Asserts:
     - `<video>` element exists with `readyState >= 3` (HAVE_FUTURE_DATA) and `currentTime` strictly advancing across polls
     - FB health label is present and is NOT `No signal`, NOT `Connecting`, NOT empty
     - Bitrate readout matches `\d+\s*kbps` AND is non-zero
6. **Screenshot capture:** Playwright saves screenshots at minute 0, 5, 15, 30 into `C:\Users\newlevel\.playwright-fb-screenshots\<run-id>\`. Uploaded as artifact `fb-live-producer-screenshots`.
7. **Teardown:** `POST /api/v1/delivery/stop` deletes the Hetzner VPS. OBS stays running for the next test in the matrix.

### 3.3 Failure semantics

- If the local watchdog detects zero chunks advancing for 30 s, hard-fail the step. No retry.
- If Playwright spec fails any assertion at any poll, hard-fail the step. No retry.
- If either step fails, the entire `e2e-fb-push-stream-lan` job fails. `e2e-gate` fails. PR not mergeable.
- Screenshots upload on failure too — debug aid.

## 4. New components

| File | Purpose | Est LoC | TDD? |
|---|---|---|---|
| `.github/workflows/ci.yml` (`e2e-fb-push-stream-lan` job + `e2e-gate` `needs:` update + remove old `e2e-fb-push` job) | The CI job | ~280 | The CI job IS the test |
| `e2e/fb-live-producer-check.spec.ts` | Playwright FB DOM check (mirror `youtube-studio-check.spec.ts`) | ~220 | RED-then-GREEN against local manually-streamed broadcast |
| `e2e/playwright-facebook.config.ts` | Playwright config (persistent profile path, headless, 30-min timeout per spec) | ~30 | n/a — config |
| `e2e/package.json` | Add `test:facebook` script + ensure `@playwright/test` covers FB config | ~5 | n/a |
| `crates/rs-api/src/facebook_seed.rs` | `POST /api/v1/facebook/config/seed` handler | ~80 | RED-then-GREEN unit test in same file |
| `crates/rs-api/src/lib.rs` | Route registration for `/api/v1/facebook/config/seed` | ~3 | covered by handler test |
| `scripts/setup-fb-profile.ps1` | One-time operator setup: HEADED Playwright launch → manual FB login → session save | ~40 | n/a — operator script |
| `docs/superpowers/specs/2026-05-18-fb-rust-e2e-ci-gate-design.md` | this spec | ~280 | n/a |
| `docs/superpowers/plans/2026-05-18-fb-rust-e2e-ci-gate.md` | implementation plan | ~400 | n/a |

## 5. Operator one-time setup (before first green CI run)

These are NOT code tasks — operator runs them after PR2 merges to dev:

1. On stream.lan via MCP: `pwsh.exe -File scripts\setup-fb-profile.ps1`. Script launches headed Chromium, opens FB login. Operator signs in with the dedicated test-account that owns the test broadcast. Session is saved to `C:\Users\newlevel\.playwright-fb-profile`.
2. Operator creates the FB test broadcast in Live Producer once. Configure as `scheduled / unpublished` — never auto-go-live, but remains addressable for repeated CI runs. Persistent stream key is generated.
3. Operator sets `gh secret set FB_TEST_STREAM_KEY --body "<persistent-key>"`.
4. Operator triggers a manual CI run via `workflow_dispatch` to verify green.

## 6. Configuration changes

- **GitHub secrets:** add `FB_TEST_STREAM_KEY` (string, persistent FB stream key for the dedicated test broadcast).
- **No app review, no Graph API, no rs-facebook crate** for this PR. Dashboard-side FB health (#166) is a separate concern, out of scope.
- **No new dependencies.** Playwright + chromium are already installed for the YT spec.

## 7. Verification primitives (Playwright assertions, mirror YT)

The YT spec reads the YouTube Studio Live Control Room DOM. FB has Live Producer at `https://www.facebook.com/live/producer/<broadcast-id>`. The spec asserts:

```typescript
// Preview must be advancing
const video = page.locator('video').first();
const t0 = await video.evaluate((v: HTMLVideoElement) => v.currentTime);
await page.waitForTimeout(5000);
const t1 = await video.evaluate((v: HTMLVideoElement) => v.currentTime);
expect(t1).toBeGreaterThan(t0);
expect(await video.evaluate((v: HTMLVideoElement) => v.readyState)).toBeGreaterThanOrEqual(3);

// Health label present, not in error states
const healthText = await page.locator('[data-testid="stream-health"], [aria-label*="health" i]').first().textContent();
expect(healthText).toBeTruthy();
expect(healthText).not.toMatch(/no signal|connecting|disconnected/i);

// Bitrate readable
const bitrateText = await page.locator('text=/\\d+\\s*kbps/i').first().textContent();
expect(bitrateText).toMatch(/\d+\s*kbps/i);
const kbps = parseInt(bitrateText!.match(/(\d+)/)![1]);
expect(kbps).toBeGreaterThan(0);
```

Exact selectors are determined RED-phase against the live FB Live Producer DOM. The spec captures a baseline DOM snapshot on first run for documentation. If FB changes the DOM, the spec fails loud — same exposure as the YT spec.

## 8. Sustained-soak budget

- 30 min per push, inside the 60-min job timeout. Matches `e2e-obs-youtube-test`.
- Acceptable per `feedback_no_nightly_ci` — nightly cron banned, so per-push soak is the only valid pattern.
- 4-hour soak is tracked separately in #213 (manual operator run, not a CI gate).

## 9. Error handling & resilience

- **FB session expired:** Playwright will see the FB login screen instead of Live Producer. The spec asserts the URL is `live/producer/...` not `login/...` — clean fail with actionable message: "FB session expired; operator must rerun `setup-fb-profile.ps1`."
- **Hetzner VPS spawn fails:** existing DeliveryOrchestrator behavior — `POST /delivery/start` errors out, local watchdog fails immediately.
- **FB broadcast got deleted / unpublished:** Live Producer URL returns 404 / redirect. Spec fails with "FB broadcast not addressable; operator must re-create test broadcast."
- **Network glitch mid-soak:** the sustained-soak assertion design is strict — any single failure during the 30 min fails the run. No retry, no flake-tolerance. Per `test-strictness.md` — flakes are bugs.
- **Browser console errors:** the Playwright spec asserts zero console errors during the 30 min (per `browser-console-zero-errors.md`).

## 10. Testing

### 10.1 RED phase

- Playwright spec lands first, asserts the DOM selectors work. Run locally against a manually-streamed FB broadcast (operator sets up the test broadcast, streams OBS → real FB endpoint via the dashboard, runs `npx playwright test fb-live-producer-check` HEADED). Spec must pass against this manual stream. THIS IS THE RED PROOF — without it, the selectors are unverified.
- `facebook_seed.rs` handler ships with failing-RED unit test (returns 404 / wrong payload shape) before the handler is written.

### 10.2 GREEN phase

- Implement handler + register route → unit test passes.
- Implement CI job → first push to dev triggers the job. On first run, expect it to fail until operator completes section 5 (setup-fb-profile + FB_TEST_STREAM_KEY secret).
- After operator setup: workflow_dispatch should produce a green run end-to-end.

### 10.3 Regression locking

The whole point: every future push that breaks FB rust delivery turns this job red. A green merge to main means FB worked end-to-end at the merge commit. No regressions can slip past unless the gate itself is disabled (banned per `no-continue-on-error.md`).

## 11. Acceptance for PR2 merge

- `e2e-fb-push-stream-lan` job green on the PR's own CI run, including the full 30-min soak
- Screenshots in the `fb-live-producer-screenshots` artifact show FB preview visible at minutes 0, 5, 15, 30
- `e2e-gate` job green, including the FB job in its `needs`
- `cargo fmt`, `cargo clippy`, `cargo test --no-run` (Tier-0 pre-push gate) all clean
- Completion report includes operator confirmation: "logged into FB Live Producer manually post-merge on streamsnv, saw rust-pusher preview" — captured as the final verification beat, per `feedback_fb_not_done_until_verified.md`
- Closes #177 and #217 in the merge commit

## 12. Out of scope (file as separate issues if discovered)

- FB Graph API integration / rs-facebook crate (#166) — dashboard-side FB health surface, NOT this CI gate
- Removing PR #218's mock-server unit test — stays as fast AMF regression guard
- 4-hour soak workflow (#213) — manual operator test, separate concern
- Other FB endpoints beyond `e2e fb` — production endpoints get verified by the soak via shared rust-pusher code; per-endpoint CI gates are not needed
- Multi-page FB testing — single dedicated test broadcast is sufficient regression-locking

## 13. References

- `e2e/youtube-studio-check.spec.ts` — architectural template for the FB spec
- `.github/workflows/ci.yml` `e2e-obs-youtube-test` (line ~1786) — architectural template for the CI job
- `.github/workflows/ci.yml` `e2e-gate` (line ~4902) — aggregator
- `feedback_fb_not_done_until_verified.md` — acceptance bar
- `project_fb_two_pr_commitment.md` — context: this is the second-of-two committed FB PRs
- `feedback_fb_ci_mirrors_yt_decided.md` — architecture choice is locked
- `feedback_no_ffmpeg_fallback.md` — no fallback under any failure mode
- PR #218 — predecessor (CONNECT AMF compliance fix, mock test, migration v29)
- Issue #166 — separate FB Graph API dashboard integration
- Issue #213 — separate 4-hour operator soak
