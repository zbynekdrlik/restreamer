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

// #278 — dedicated S3-region banner.
//
// A stale per-install config.json can silently carry a degraded/wrong
// s3.region across an upgrade (the 2026-06-24 incident: streampp ran a live
// event on nbg1). The audit Critical row is the log-level signal; this
// banner is the loud dashboard-level one. Data is seeded via the
// scenario-based mock-api harness (s3_region_standard on the
// /api/v1/status payload the dashboard polls every 2s).

test("non-standard S3 region shows the red S3-region banner, clean console", async ({
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
    data: { scenario: "s3-region-nonstandard" },
  });

  await page.goto("/");

  const banner = page.locator('[data-testid="s3-region-banner"]');
  await expect(banner).toBeVisible({ timeout: 10000 });
  await expect(banner).toHaveClass(/banner--critical/);
  await expect(banner).toContainText("NOT the project standard");

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("no S3-region banner when the region is standard", async ({
  page,
  request,
}) => {
  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");
  // Default scenario => s3_region_standard=true.
  await page.goto("/");

  // Give the dashboard a moment to load + poll, then confirm no banner.
  await page.waitForTimeout(2000);
  await expect(
    page.locator('[data-testid="s3-region-banner"]'),
  ).toHaveCount(0);
});
