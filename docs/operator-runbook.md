# Operator Runbook

Procedures for running live events with restreamer. Keep this file short — one-liner checks the operator follows from the top before going live, plus a small set of post-event spot checks.

## Before Going Live

- [ ] **Audio smoke-test (mandatory after every restreamer upgrade):**
  Push a 30-second test stream from OBS into restreamer (use the regular live event RTMP URL).
  Listen on the rescue / preview output. Confirm:
  - Audio plays without chipmunk / pitch shift.
  - No glitches, clicks, or dropouts.
  - Audio stays in sync with video.
  If any of these fail — **do not go live.** Escalate to engineering. The 2026-04-26 live event was lost because this check was skipped after a restreamer version upgrade and audio was unusable from the start.

- [ ] **Dashboard liveness:**
  Open the dashboard. Confirm the deployed version banner matches the version you intended to run. Confirm the inpoint shows `disconnected` (no leftover stream) and there are no error banners.

- [ ] **No active leftover events:**
  On the dashboard's Events tab, confirm no event other than the one you are about to use shows `receiving_activated` or `delivering_activated`. Deactivate any leftovers.

## During the Event

- Watch the dashboard delivery cache value. It should sit near the configured `delivery_delay_secs` (default 120s) and not drift toward 0 or balloon past 200s.
- If audio degrades mid-event, switch to the fallback path (non-restreamer) immediately. Don't try to fix restreamer mid-stream — file an issue afterwards.

## After the Event

- Stop the active event from the dashboard (don't just stop OBS — the receive/deliver flags should clear cleanly).
- If anything was off, file a GitHub issue with: timestamp, symptom, dashboard screenshot, and which restreamer version you were running (visible in the dashboard footer).
