import { test, expect, type Request } from "@playwright/test";
import { broadcast, waitForWsClient } from "./lib/ws";
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

// #106 — an RTMP port-bind failure (e.g. a legacy inpoint_service holding 1234)
// used to fail SILENTLY: the dashboard showed "everything fine" while OBS could
// not publish. The dashboard polls /api/v1/status every 2s; the
// "rtmp-bind-error" scenario feeds inpoint.details.rtmp_bind_error naming the
// port + holding process. Acceptance:
//   - the failure raises the red banner naming the port + holding process,
//   - a healthy listener never raises the banner (no false positives).

test("rtmp bind failure raises the red banner naming the port + holder, clean console", async ({
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
    data: { scenario: "rtmp-bind-error" },
  });

  await page.goto("/");

  const banner = page.locator('[data-testid="rtmp-bind-error-banner"]');
  await expect(banner).toBeVisible({ timeout: 10000 });
  await expect(banner).toHaveClass(/banner--critical/);
  // Names the failure, the port, and the holding process.
  await expect(banner).toContainText("RTMP server failed to start");
  await expect(banner).toContainText("1234");
  await expect(banner).toContainText("inpoint_service.exe");

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

test("RtmpBindFailed WebSocket event raises the banner instantly (no poll wait)", async ({
  page,
  request,
}) => {
  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");
  // Default scenario => the /status poll reports NO bind error, so the banner
  // can only appear from the WebSocket event — proving the ws.rs arm wires it.
  // #377: that same 2 s ControlBar poll WRITES rtmp_bind_error (null here), so
  // a poll answered just after the push would clear the banner again. Track
  // every /status request so we can hold the poll before pushing (below).
  const isStatus = (url: string) => new URL(url).pathname === "/api/v1/status";
  const openStatus = new Set<Request>();
  const heldStatus = new Set<Request>();
  page.on("request", (r) => {
    if (isStatus(r.url())) openStatus.add(r);
  });
  page.on("requestfinished", (r) => openStatus.delete(r));
  page.on("requestfailed", (r) => openStatus.delete(r));

  await page.goto("/");
  await expect(page.locator(".event-selector")).toBeVisible({ timeout: 10000 });

  const banner = page.locator('[data-testid="rtmp-bind-error-banner"]');
  await expect(banner).toHaveCount(0);

  await waitForWsClient(page, request);
  // Hold every later /status poll (never answered while the test runs), then
  // wait until no poll sent BEFORE the hold is still unanswered: from here no
  // /status response can reach the store, so the banner below can only come
  // from the WebSocket arm, and nothing can clear it.
  await page.route(
    (url) => isStatus(url.toString()),
    (route) => {
      heldStatus.add(route.request());
    },
  );
  await expect
    .poll(() => [...openStatus].filter((r) => !heldStatus.has(r)).length, {
      message: "a /status poll sent before the hold is still in flight",
    })
    .toBe(0);
  await broadcast(request, {
    type: "RtmpBindFailed",
    data: {
      port: 1234,
      error:
        "Port 1234 is already in use by another process. RTMP streaming will not work until the conflict is resolved.",
    },
  });

  await expect(banner).toBeVisible({ timeout: 5000 });
  await expect(banner).toContainText("1234");
});

test("no rtmp-bind-error banner when the listener is healthy", async ({
  page,
  request,
}) => {
  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");
  // Default scenario => rtmp_bind_error=null.
  await page.goto("/");

  // Wait for the dashboard to finish its FIRST /status poll (the event
  // selector only renders once that lands), rather than a fixed sleep — proves
  // the negative assertion is checked against real polled state.
  await expect(page.locator(".event-selector")).toBeVisible({
    timeout: 10000,
  });
  const banner = page.locator('[data-testid="rtmp-bind-error-banner"]');
  await expect(banner).toHaveCount(0);

  // The store polls /status every 2s — wait one more full cycle and re-check,
  // so a banner that only appeared on a LATER poll would still be caught.
  await page.waitForTimeout(2500);
  await expect(banner).toHaveCount(0);
});
