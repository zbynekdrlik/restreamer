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

// #231 — dedicated disk-pressure banner.
//
// The never-drop continuity guarantee buffers chunks on the laptop disk, so
// disk pressure is the safety valve. A dedicated banner is clearer than the
// per-endpoint red wall and surfaces the early WARN (80%) state. Data is
// seeded via the scenario-based mock-api harness (disk_pressure on the
// /api/v1/status payload the dashboard polls every 2s).

test("disk WARN shows the amber disk-pressure banner, clean console", async ({
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
    data: { scenario: "disk-warn" },
  });

  await page.goto("/");

  const banner = page.locator('[data-testid="disk-pressure-banner"]');
  await expect(banner).toBeVisible({ timeout: 10000 });
  await expect(banner).toHaveClass(/banner--warn/);
  await expect(banner).toContainText("filling up");

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("disk CRITICAL shows the red disk-pressure banner, clean console", async ({
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
    data: { scenario: "disk-critical" },
  });

  await page.goto("/");

  const banner = page.locator('[data-testid="disk-pressure-banner"]');
  await expect(banner).toBeVisible({ timeout: 10000 });
  await expect(banner).toHaveClass(/banner--critical/);
  await expect(banner).toContainText("CRITICALLY full");

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("no disk-pressure banner when disk is OK", async ({ page, request }) => {
  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");
  // Default scenario => disk_pressure="ok".
  await page.goto("/");

  // Give the dashboard a moment to load + poll, then confirm no banner.
  await page.waitForTimeout(2000);
  await expect(
    page.locator('[data-testid="disk-pressure-banner"]'),
  ).toHaveCount(0);
});
