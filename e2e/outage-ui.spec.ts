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

// Task T19 — outage-survival UX.
//
// The dashboard semaphore must read CALM, not alarming, for a survivable
// outage: a rescue/recovering endpoint shows a single BLUE banner with
// "No action needed" and a blue endpoint node — never a wall of red. Only a
// genuine auth-reject (lifecycle="attention") goes RED, and when anything is
// red the calm banner must be suppressed so the operator's eye lands on the
// thing that actually needs them.
//
// Data is seeded through the existing scenario-based mock-api harness rather
// than `page.route`: the mock backs BOTH the HTTP cached-status load and the
// WebSocket DeliveryStatus push that follows it, so the two stay coherent. A
// `page.route` that fulfilled only cached-status would be clobbered ~200ms
// later by the mock's WS broadcast resetting lifecycle to the default "live".

test("survivable outage shows calm blue banner, not red wall, clean console", async ({
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
  // Seed BEFORE navigating so cached-status + the WS connect payload both
  // describe one endpoint in the survivable "rescue" lifecycle.
  await request.post("http://127.0.0.1:8910/api/v1/_test/scenario", {
    data: { scenario: "outage-rescue" },
  });

  await page.goto("/");

  // Calm, single blue banner — survivable outage, recovering automatically.
  const banner = page.locator(".banner--recovering");
  await expect(banner).toBeVisible({ timeout: 10000 });
  await expect(banner).toContainText("No action needed");

  // The endpoint node is blue (recovering), and NOTHING is red (attention).
  await expect(page.locator(".endpoint-node.recovering")).toBeVisible();
  await expect(page.locator(".endpoint-node.attention")).toHaveCount(0);

  // Zero browser console errors / warnings (subresource integrity exempted).
  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("auth-reject endpoint is red and suppresses the calm banner", async ({
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
  // One endpoint in the "attention" lifecycle (rejected stream key) — this is
  // the case that genuinely needs the operator, so it MUST be red.
  await request.post("http://127.0.0.1:8910/api/v1/_test/scenario", {
    data: { scenario: "outage-attention" },
  });

  await page.goto("/");

  // The endpoint node is red (attention).
  await expect(page.locator(".endpoint-node.attention")).toBeVisible({
    timeout: 10000,
  });
  // The calm banner is suppressed whenever anything needs attention, so the
  // operator's eye is not lulled by a "no action needed" message.
  await expect(page.locator(".banner--recovering")).toHaveCount(0);

  // Zero browser console errors / warnings (subresource integrity exempted).
  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
