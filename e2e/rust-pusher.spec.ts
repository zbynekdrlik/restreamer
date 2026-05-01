import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// Inject Tauri mock so the Leptos app is in "Tauri mode" for invoke().
const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// Chromium-level warnings that are not application bugs.
const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

/**
 * Rust pusher smoke test.
 *
 * This test runs against the mock API (not a live Hetzner VPS) and verifies:
 *   1. The dashboard loads without console errors.
 *   2. GET /api/v1/audit returns 200 (backfill endpoint exists).
 *   3. The delivery status cached response exposes a `reconnect_count` field
 *      on endpoint entries -- proving the API contract for the Rust pusher
 *      metric was added (Task 11, issue #103).
 *
 * Full end-to-end verification of the Rust RTMP push path (chunks_processed
 * advancing, zero reconnects over 5 min) requires a live Hetzner VPS with a
 * `pusher: "rust"` endpoint and OBS streaming.  That is covered by the
 * `e2e-obs-youtube-test` CI job which wires the "e2e rtmp" endpoint with
 * `pusher = 'rust'` before starting the stream (see ci.yml).
 */
test("rust pusher: dashboard loads and reconnect_count field is present in delivery status", async ({
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
  // last-endpoint scenario: one endpoint entry that includes reconnect_count.
  await request.post("http://127.0.0.1:8910/api/v1/_test/scenario", {
    data: { scenario: "last-endpoint" },
  });

  await page.goto("/");
  // Dashboard must render the main layout without crashing.
  await expect(page.locator("body")).toBeVisible({ timeout: 10_000 });

  // Audit backfill endpoint must return 200 so AuditPanel mounts cleanly.
  const auditResp = await request.get("http://127.0.0.1:8910/api/v1/audit");
  expect(auditResp.ok()).toBeTruthy();

  // Delivery status cached endpoint must expose reconnect_count on each
  // endpoint entry.  This field is added by Task 11 (#103) and used by the
  // dashboard to display Rust-pusher reconnect telemetry.
  const cachedResp = await request.get(
    "http://127.0.0.1:8910/api/v1/delivery/status/cached",
  );
  expect(cachedResp.ok()).toBeTruthy();
  const cached = await cachedResp.json();
  expect(cached.endpoints.length).toBeGreaterThan(0);
  const ep = cached.endpoints[0];
  // reconnect_count must be present and must be a non-negative integer.
  expect(typeof ep.reconnect_count).toBe("number");
  expect(ep.reconnect_count).toBeGreaterThanOrEqual(0);

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
