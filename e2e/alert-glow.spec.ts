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

const API = "http://127.0.0.1:8910/api/v1";

function watchConsole(page: import("@playwright/test").Page): string[] {
  const msgs: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      msgs.push(`[${msg.type()}] ${msg.text()}`);
    }
  });
  return msgs;
}

function assertCleanConsole(msgs: string[]) {
  const real = msgs.filter((m) => !ALLOWED_CONSOLE.some((r) => r.test(m)));
  expect(real).toEqual([]);
}

// #73 — unified app-level "any issue" red edge glow.
//
// The glow is a single, always-in-view aggregate cue that pulses a red halo
// around the viewport border whenever ANY condition the dashboard already
// renders red is active. It invents no new backend signal, so every case is
// driven through the same scenario mock harness as the banner specs, anchored
// on the per-condition element that proves the real state landed.

// One positive case per OR-branch of `any_issue`, so deleting any single
// branch from the aggregate is caught (not just the attention branch).
const POSITIVE: { scenario: string; anchor: string; desc: string }[] = [
  {
    scenario: "outage-attention",
    anchor: ".endpoint-node.attention",
    desc: "endpoint attention",
  },
  {
    scenario: "disk-critical",
    anchor: '[data-testid="disk-pressure-banner"]',
    desc: "disk critical",
  },
  {
    scenario: "zero-endpoints",
    anchor: ".banner--critical",
    desc: "delivery active with zero endpoints",
  },
  {
    scenario: "ingest-skew",
    anchor: '[data-testid="ingest-skew-banner"]',
    desc: "ingest A/V skew",
  },
  {
    scenario: "s3-region-nonstandard",
    anchor: '[data-testid="s3-region-banner"]',
    desc: "non-standard S3 region",
  },
];

for (const { scenario, anchor, desc } of POSITIVE) {
  test(`${desc} drives the glow, clean console`, async ({ page, request }) => {
    const consoleMessages = watchConsole(page);

    await page.addInitScript(tauriMockScript);
    await request.post(`${API}/__reset`);
    // Seed BEFORE navigating so cached-status + the WS connect payload both
    // describe the condition (endpoint lifecycle / pipeline / status flags).
    await request.post(`${API}/_test/scenario`, { data: { scenario } });

    await page.goto("/");

    // The per-condition element confirms the real state actually landed,
    // THEN the aggregate glow overlay must be present and visible.
    await expect(page.locator(anchor).first()).toBeVisible({ timeout: 10000 });
    const glow = page.locator('[data-testid="alert-glow"]');
    await expect(glow).toBeVisible();
    // Not a tautology on its own testid: assert the computed style, which
    // mechanically locks that the `.alert-glow` class is DEFINED in the
    // stylesheet and actually applied (position + the pulse animation).
    await expect(glow).toHaveCSS("position", "fixed");
    await expect(glow).toHaveCSS("animation-name", "alertGlowPulse");

    assertCleanConsole(consoleMessages);
  });
}

test("a calm survivable outage (rescue) does NOT glow, clean console", async ({
  page,
  request,
}) => {
  const consoleMessages = watchConsole(page);

  await page.addInitScript(tauriMockScript);
  await request.post(`${API}/__reset`);
  await request.post(`${API}/_test/scenario`, {
    data: { scenario: "outage-rescue" },
  });

  await page.goto("/");

  // Calm blue "recovering" banner confirms the rescue state landed. Rescue is
  // a survivable auto-recovery state, so the red glow must stay OFF.
  await expect(page.locator(".banner--recovering")).toBeVisible({
    timeout: 10000,
  });
  await expect(page.locator('[data-testid="alert-glow"]')).toHaveCount(0);

  assertCleanConsole(consoleMessages);
});

test("no glow on the idle dashboard, clean console", async ({
  page,
  request,
}) => {
  const consoleMessages = watchConsole(page);

  await page.addInitScript(tauriMockScript);
  await request.post(`${API}/__reset`);
  // Default scenario => idle, no issues. This also guards the #73 review 🔴
  // regression: an idle dashboard (empty pipeline state on first paint) must
  // never false-alarm the glow.
  await page.goto("/");

  // Give the dashboard a moment to load + poll, then confirm no glow.
  await page.waitForTimeout(2000);
  await expect(page.locator('[data-testid="alert-glow"]')).toHaveCount(0);

  assertCleanConsole(consoleMessages);
});
