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

// #352 — orphaned-delivery-VPS banner.
//
// Every teardown path is keyed on a delivery_instances DB row; when that row is
// lost the Hetzner VPS bills forever, invisible to the app. The runtime orphan
// reaper finds these and publishes the still-billing count on /api/v1/status;
// this banner is the loud dashboard-level signal. Data is seeded via the
// scenario-based mock-api harness (vps_orphan_count on the /status payload the
// dashboard polls every 2s).

test("orphaned VPS shows the amber orphan banner, clean console", async ({
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
    data: { scenario: "vps-orphan" },
  });

  await page.goto("/");

  const banner = page.locator('[data-testid="vps-orphan-banner"]');
  await expect(banner).toBeVisible({ timeout: 10000 });
  await expect(banner).toHaveClass(/banner--warn/);
  await expect(banner).toContainText("orphaned delivery VPS still billing");
  await expect(banner).toContainText("2");

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("no orphan banner when nothing is orphaned", async ({ page, request }) => {
  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");
  // Default scenario => vps_orphan_count=0.
  await page.goto("/");

  // Give the dashboard a moment to load + poll, then confirm no banner.
  await page.waitForTimeout(2000);
  await expect(
    page.locator('[data-testid="vps-orphan-banner"]'),
  ).toHaveCount(0);
});
