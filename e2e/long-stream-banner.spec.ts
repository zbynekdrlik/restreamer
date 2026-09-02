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

// #84 — dedicated long-stream banner.
//
// A single delivery running longer than delivery.long_stream_warn_secs
// (default 2.5 h) usually means the stream was left on after the event
// finished. The LongStreamWarning audit row + Discord ping are the one-shot
// signals; this banner is the persistent dashboard one. In the real backend
// the flag is computed from the delivery instance's created_at vs the config
// threshold; here it is seeded via the scenario-based mock-api harness
// (long_stream_warning on the /api/v1/status payload the dashboard polls
// every 2s), which is exactly "delivery running past a tiny threshold".

test("a long-running delivery shows the amber long-stream banner, clean console", async ({
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
    data: { scenario: "long-stream" },
  });

  await page.goto("/");

  const banner = page.locator('[data-testid="long-stream-banner"]');
  await expect(banner).toBeVisible({ timeout: 10000 });
  await expect(banner).toHaveClass(/banner--warn/);
  await expect(banner).toContainText("Stream beží už veľmi dlho");

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("no long-stream banner under the default scenario", async ({
  page,
  request,
}) => {
  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");
  // Default scenario => long_stream_warning=false.
  await page.goto("/");

  // Give the dashboard a moment to load + poll, then confirm no banner.
  await page.waitForTimeout(2000);
  await expect(
    page.locator('[data-testid="long-stream-banner"]'),
  ).toHaveCount(0);
});
