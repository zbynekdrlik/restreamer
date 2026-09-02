import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

const API = "http://127.0.0.1:8910";

/**
 * Standalone POST /api/v1/delivery/start (#136).
 *
 * The primary "Start Delivering" button drives /events/{id}/start-stream
 * (stream_handlers::start_stream). The SEPARATE standalone endpoint
 * POST /api/v1/delivery/start (delivery_handlers::delivery_start) — used by
 * CI scripts, tooling and future external integrations — has real
 * state-affecting behavior (flips streaming_events.delivering_activated,
 * emits an audit row, broadcasts a WS StreamingEvent so dashboards flip
 * IDLE<->STREAMING immediately; commits 06f10f3, 442d7aa, f95923b, #130) but
 * had NO Playwright coverage. A refactor could silently regress the
 * dashboard-state reflection of this path.
 *
 * These specs exercise the standalone path against the mock API (which gained
 * /delivery/start + /delivery/stop routes mirroring the real handlers) and
 * assert the dashboard reflects the delivering state end-to-end: the header
 * state badge, the S3->VPS (VPS link) node, the endpoint row, the event
 * dropdown auto-select, and zero console errors/warnings.
 */
test.describe("standalone POST /delivery/start", () => {
  test.beforeEach(async ({ page, request }) => {
    await page.addInitScript(tauriMockScript);
    await request.post(`${API}/api/v1/__reset`);
    // Pin the RTMP-stable gate open (>=15s) so the standalone start is allowed.
    await request.post(`${API}/api/v1/_test/set-rtmp-stable-secs`, {
      data: { secs: 15 },
    });
  });

  test("flips dashboard IDLE -> STREAMING and back, reflecting VPS + endpoint state", async ({
    page,
    request,
  }) => {
    const consoleMessages: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() === "error" || msg.type() === "warning") {
        consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
      }
    });

    await page.goto("/");

    // --- Baseline: dashboard header shows IDLE, event dropdown is selectable ---
    await expect(page.locator(".event-selector")).toBeVisible({
      timeout: 10000,
    });
    await expect(page.locator(".state-badge")).toContainText("Idle");
    await expect(page.locator(".event-selector")).toBeEnabled();
    // The warming-up badge only comes from the standalone start below — the
    // default WS-connect delivery payload uses delivery_mode "normal".
    await expect(page.locator(".endpoint-mode-warmup")).toHaveCount(0);

    // Operator picks the event to deliver (Sunday Service, id=1).
    await page.locator(".event-selector").selectOption("1");
    await expect(page.locator(".event-selector")).toHaveValue("1");

    // --- Activate the event (sets receiving_activated), then start delivery
    // DIRECTLY via the standalone endpoint (NOT via /start-stream) ---
    const activateResp = await request.post(
      `${API}/api/v1/events/1/activate`,
    );
    expect(activateResp.ok()).toBeTruthy();

    const startResp = await request.post(`${API}/api/v1/delivery/start`, {
      data: { event_id: 1 },
    });
    expect(startResp.status()).toBe(200);
    const startBody = await startResp.json();
    expect(startBody).toMatchObject({
      instance_id: expect.any(Number),
      hetzner_id: expect.any(Number),
      status: "running",
    });

    // --- Dashboard header flips to STREAMING (WS-driven, no manual reload) ---
    await expect(page.locator(".state-badge")).toContainText("Streaming", {
      timeout: 5000,
    });

    // The selected delivering event stays shown and the dropdown locks while
    // delivery is active.
    await expect(page.locator(".event-selector")).toHaveValue("1");
    await expect(page.locator(".event-selector")).toBeDisabled();

    // The VPS link (S3 -> VPS pipeline node) reflects a live delivering VPS.
    const s3Node = page
      .locator(".pipeline-node")
      .filter({ hasText: "S3" });
    await expect(s3Node).toHaveClass(/active/);
    await expect(s3Node.locator(".pipeline-node-metric")).toContainText(
      "delivered",
    );

    // The endpoint row renders with a WARMUP badge (endpoint warming up on VPS).
    await expect(page.locator(".endpoint-mode-warmup")).toContainText(
      "WARMUP",
      { timeout: 5000 },
    );

    // --- Stop delivery via the standalone endpoint: dashboard returns to IDLE ---
    const stopResp = await request.post(`${API}/api/v1/delivery/stop`, {
      data: { event_id: 1 },
    });
    expect(stopResp.status()).toBe(200);

    await expect(page.locator(".state-badge")).toContainText("Idle", {
      timeout: 5000,
    });
    // Endpoint tree collapses (no endpoints, VPS torn down) — WARMUP gone.
    await expect(page.locator(".endpoint-mode-warmup")).toHaveCount(0);
    // Selector re-enabled once delivery is no longer active.
    await expect(page.locator(".event-selector")).toBeEnabled();

    const real = consoleMessages.filter(
      (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
    );
    expect(real).toEqual([]);
  });

  test("standalone start is gated behind rtmp_stable_secs >= 15", async ({
    page,
    request,
  }) => {
    const consoleMessages: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() === "error" || msg.type() === "warning") {
        consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
      }
    });

    // Ingest not yet stable — the standalone start must be refused.
    await request.post(`${API}/api/v1/_test/set-rtmp-stable-secs`, {
      data: { secs: 0 },
    });

    await page.goto("/");
    await expect(page.locator(".state-badge")).toContainText("Idle", {
      timeout: 10000,
    });

    await request.post(`${API}/api/v1/events/1/activate`);
    const gated = await request.post(`${API}/api/v1/delivery/start`, {
      data: { event_id: 1 },
    });
    expect(gated.status()).toBe(400);
    const body = await gated.json();
    expect(body.error).toBe("rtmp_not_stable");
    expect(body.need_secs).toBe(15);

    // Dashboard stays IDLE — the refused start never flipped state.
    await expect(page.locator(".state-badge")).toContainText("Idle");

    const real = consoleMessages.filter(
      (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
    );
    expect(real).toEqual([]);
  });
});
