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

// #73 — unified app-level "any issue" red edge glow.
//
// The glow is a single, always-in-view aggregate cue that pulses a red halo
// around the viewport border whenever ANY condition the dashboard already
// renders red is active. It invents no new backend signal, so it is driven
// through the same scenario mock harness as every banner spec. These tests
// prove: (a) a REAL attention state (endpoint lifecycle="attention", seeded
// via the API) makes the glow appear; (b) an unrelated red condition
// (disk-critical) also drives it, proving the aggregate; (c) a CALM
// survivable state (outage-rescue) does NOT (semaphore consistency); and
// (d) the idle dashboard has no glow.

test("real attention state drives the glow, clean console", async ({
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
  // One endpoint in the "attention" lifecycle (rejected stream key) — a real
  // issue the operator must act on. Seeds BOTH cached-status and the WS push.
  await request.post("http://127.0.0.1:8910/api/v1/_test/scenario", {
    data: { scenario: "outage-attention" },
  });

  await page.goto("/");

  // The red per-endpoint node confirms the attention state actually landed,
  // then the aggregate glow overlay must be present and visible.
  await expect(page.locator(".endpoint-node.attention")).toBeVisible({
    timeout: 10000,
  });
  const glow = page.locator('[data-testid="alert-glow"]');
  await expect(glow).toBeVisible();
  await expect(glow).toHaveClass(/alert-glow/);

  // Zero browser console errors / warnings (subresource integrity exempted).
  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("an unrelated red condition (disk critical) also drives the glow, clean console", async ({
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

  // The dedicated disk banner confirms the condition landed; the aggregate
  // glow must fire on it too (proving it is not attention-only).
  await expect(
    page.locator('[data-testid="disk-pressure-banner"]'),
  ).toBeVisible({ timeout: 10000 });
  await expect(page.locator('[data-testid="alert-glow"]')).toBeVisible();

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("a calm survivable outage (rescue) does NOT glow, clean console", async ({
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
    data: { scenario: "outage-rescue" },
  });

  await page.goto("/");

  // Calm blue "recovering" banner confirms the rescue state landed. Rescue is
  // a survivable auto-recovery state, so the red glow must stay OFF.
  await expect(page.locator(".banner--recovering")).toBeVisible({
    timeout: 10000,
  });
  await expect(page.locator('[data-testid="alert-glow"]')).toHaveCount(0);

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("no glow on the idle dashboard", async ({ page, request }) => {
  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");
  // Default scenario => idle, no issues.
  await page.goto("/");

  // Give the dashboard a moment to load + poll, then confirm no glow.
  await page.waitForTimeout(2000);
  await expect(page.locator('[data-testid="alert-glow"]')).toHaveCount(0);
});
