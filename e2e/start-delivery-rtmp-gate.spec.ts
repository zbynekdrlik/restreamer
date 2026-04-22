import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

/**
 * RTMP-stable gate: the Start Delivering button must be disabled until
 * the ingest has been stable for RTMP_STABLE_REQUIRED_SECS (15s in prod).
 * This prevents the operator from triggering a Hetzner VPS provision
 * while OBS is still mid-handshake — which today aborted two provisions
 * at 07:00 and 07:02 (2026-04-19 post-mortem, issue G).
 *
 * The mock-api exposes POST /api/v1/_test/set-rtmp-stable-secs so this
 * spec can drive the gate deterministically without racing against the
 * mock's time-based tick scenario.
 */
test("Start Delivering is gated behind rtmp_stable_secs >= required threshold", async ({
  page,
  request,
}) => {
  const consoleMessages: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");

  // Force rtmp_stable_secs=0 so the gate blocks Start Delivering even
  // after the operator picks an event.
  await request.post(
    "http://127.0.0.1:8910/api/v1/_test/set-rtmp-stable-secs",
    { data: { secs: 0 } },
  );

  await page.goto("/");
  await expect(page.locator(".event-selector")).toBeVisible({
    timeout: 10000,
  });

  // Pick a real event so the other disable-condition (no event) goes away
  // and we're only testing the RTMP gate.
  await page.locator(".event-selector").selectOption({ index: 1 });

  const startBtn = page.locator(".start-btn");
  await expect(startBtn).toBeDisabled();

  // Hover tooltip must surface the reason so an operator knows WHY the
  // button is greyed out.
  const title = await startBtn.getAttribute("title");
  expect(title ?? "").toMatch(/waiting for obs stream to stabilize/i);

  // Simulate 15 seconds of stable ingest and poll: the button unlocks.
  await request.post(
    "http://127.0.0.1:8910/api/v1/_test/set-rtmp-stable-secs",
    { data: { secs: 15 } },
  );

  // The dashboard polls /status every 2s (operator_dashboard.rs:59).
  // Give it two ticks of headroom — this is deterministic because we
  // pinned the mock, not racing against real wall-clock.
  await expect(startBtn).toBeEnabled({ timeout: 5000 });

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
