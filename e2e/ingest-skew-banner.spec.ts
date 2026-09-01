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

// #354 — ingest A/V-desync (OBS source) must be diagnosed LOUDLY and gate
// Start Delivering. The dashboard polls /api/v1/status every 2s; the
// "ingest-skew" scenario feeds inpoint.details.ingest_skew_active=true with a
// ~25 s skew. Acceptance:
//   - a desynced source raises the red banner (DOM-read),
//   - Start Delivering is refused (button disabled) with the plain reason,
//   - a clean source never raises the banner (no false positives).

test("desynced OBS source raises the red ingest-skew banner, clean console", async ({
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
    data: { scenario: "ingest-skew" },
  });

  await page.goto("/");

  const banner = page.locator('[data-testid="ingest-skew-banner"]');
  await expect(banner).toBeVisible({ timeout: 10000 });
  await expect(banner).toHaveClass(/banner--critical/);
  // Names the cause + the remedy in plain Slovak.
  await expect(banner).toContainText("OBS");
  await expect(banner).toContainText("rozídené");
  await expect(banner).toContainText("reštartuj");

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("Start Delivering is gated (disabled with the plain reason) while ingest skew holds", async ({
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
    data: { scenario: "ingest-skew" },
  });

  await page.goto("/");
  await expect(page.locator(".event-selector")).toBeVisible({
    timeout: 10000,
  });

  // Pick a real event so the "no event" disable-condition goes away and we're
  // testing ONLY the ingest-skew gate (rtmp_stable_secs is 999 in this
  // scenario, so the RTMP gate is already satisfied).
  await page.locator(".event-selector").selectOption({ index: 1 });

  const startBtn = page.locator(".start-btn");
  await expect(startBtn).toBeDisabled();

  // The hover tooltip must surface the SOURCE fault + the remedy.
  const title = await startBtn.getAttribute("title");
  expect(title ?? "").toMatch(/rozídené.*OBS|OBS.*rozídené/i);

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("no ingest-skew banner when the source is clean", async ({
  page,
  request,
}) => {
  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");
  // Default scenario => ingest_skew_active=false.
  await page.goto("/");

  // Give the dashboard a moment to load + poll, then confirm no banner.
  await page.waitForTimeout(2000);
  await expect(
    page.locator('[data-testid="ingest-skew-banner"]'),
  ).toHaveCount(0);
});
