import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// Inject the Tauri mock so the Leptos app runs in browser mode (fetch over
// HTTP, not Tauri IPC), matching every other frontend spec in this suite.
const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// Chromium-level warnings that are not application bugs.
const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

// #260 — dedicated "no rescue video" banner.
//
// Event 9316 (2026-06-19) went live with rescue_video_url=NULL, so the 4G
// outage fell back to the generic default clip with zero operator signal. This
// banner is the loud dashboard-level warning; its durable counterpart is the
// Action::NoRescueVideoConfigured audit row emitted at delivery start. The
// banner reads rescue_video_url straight off the active streaming_event on the
// /api/v1/status payload the dashboard polls — no dedicated status scalar.

test("active event with no rescue video shows the amber banner, clean console", async ({
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
  await request.post("http://127.0.0.1:8910/api/v1/_test/scenario", {
    data: { scenario: "no-rescue-video" },
  });

  await page.goto("/");

  const banner = page.locator('[data-testid="no-rescue-video-banner"]');
  await expect(banner).toBeVisible({ timeout: 10000 });
  await expect(banner).toHaveClass(/banner--warn/);
  await expect(banner).toContainText("záložné video");

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("no banner when the active event HAS a rescue video (keys on rescue_video_url)", async ({
  page,
  request,
}) => {
  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");
  await request.post("http://127.0.0.1:8910/api/v1/_test/scenario", {
    data: { scenario: "rescue-video-set" },
  });

  await page.goto("/");

  // Prove the dashboard actually loaded and polled /status (otherwise a
  // toHaveCount(0) would pass vacuously on a blank page). Only THEN assert the
  // banner is absent — proving it keys on rescue_video_url, not merely on an
  // active event.
  await page.waitForResponse(
    (r) => r.url().endsWith("/api/v1/status") && r.ok(),
    { timeout: 10000 },
  );
  await page.waitForTimeout(500);
  await expect(
    page.locator('[data-testid="no-rescue-video-banner"]'),
  ).toHaveCount(0);
});

test("no banner on the idle dashboard (no active event)", async ({
  page,
  request,
}) => {
  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");
  // Default scenario => idle dashboard, no active streaming event.
  await page.goto("/");

  // Wait for a real /status poll before asserting absence (no vacuous pass on
  // a blank page).
  await page.waitForResponse(
    (r) => r.url().endsWith("/api/v1/status") && r.ok(),
    { timeout: 10000 },
  );
  await page.waitForTimeout(500);
  await expect(
    page.locator('[data-testid="no-rescue-video-banner"]'),
  ).toHaveCount(0);
});
