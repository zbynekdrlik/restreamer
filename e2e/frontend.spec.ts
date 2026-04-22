import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// Inject Tauri mock before each page navigation
const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// Chromium-level warnings that are not application bugs
const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i, // Chromium bug crbug.com/981419
];

// Collect console errors/warnings per-test and assert clean console in afterEach
let consoleMessages: string[] = [];

test.beforeEach(async ({ page, request }) => {
  consoleMessages = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });
  await page.addInitScript(tauriMockScript);
  // Reset mock API state so each test starts with clean initial data
  await request.post("http://127.0.0.1:8910/api/v1/__reset");
});

test.afterEach(async () => {
  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});

// --- Operator Dashboard (/) ---

test.describe("Operator Dashboard", () => {
  test("renders header with Restreamer title", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("h1.app-title")).toHaveText("Restreamer");
  });

  test("header shows WebSocket connection status", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".ws-indicator")).toBeVisible();
  });

  test("header shows settings link with gear icon", async ({ page }) => {
    await page.goto("/");
    const settingsLink = page.locator('.header-nav-btn:has-text("Settings")');
    await expect(settingsLink).toBeVisible();
  });

  test("dashboard loads with event selector", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".event-selector")).toBeVisible({
      timeout: 10000,
    });
  });

  test("event dropdown populates from API", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".event-selector")).toBeVisible({
      timeout: 10000,
    });
    // Wait for events to load
    await page.waitForTimeout(1000);
    const options = page.locator(".event-selector option");
    // Should have at least 3 options: placeholder + 2 events
    await expect(options).toHaveCount(3);
  });

  test("shows Start Delivering and Stop buttons", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".start-btn")).toBeVisible({ timeout: 10000 });
    await expect(page.locator(".stop-btn")).toBeVisible();
  });

  test("Start Delivering disabled without event selection", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.locator(".start-btn")).toBeVisible({ timeout: 10000 });
    await expect(page.locator(".start-btn")).toBeDisabled();
  });

  test("Start Delivering enabled after selecting event", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".event-selector")).toBeVisible({
      timeout: 10000,
    });
    await page.waitForTimeout(1000);
    // Select first event
    await page.locator(".event-selector").selectOption({ index: 1 });
    await expect(page.locator(".start-btn")).toBeEnabled();
  });

  test("Start Delivering calls POST /events/{id}/start-stream", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);
    // Select first event (id=1)
    await page.locator(".event-selector").selectOption({ index: 1 });

    // Intercept the API call
    const [request] = await Promise.all([
      page.waitForRequest(
        (req) => req.url().includes("/start-stream") && req.method() === "POST",
      ),
      page.locator(".start-btn").click(),
    ]);
    expect(request.url()).toContain("/events/1/start-stream");
  });

  test("pipeline nodes render in vertical flow", async ({ page }) => {
    await page.goto("/");
    const nodes = page.locator(".pipeline-node");
    await expect(nodes).toHaveCount(4);
    await expect(page.locator(".pipeline-node-label").nth(0)).toContainText(
      "OBS",
    );
    await expect(page.locator(".pipeline-node-label").nth(1)).toContainText(
      "RTMP",
    );
    await expect(page.locator(".pipeline-node-label").nth(2)).toContainText(
      "Local Buffer",
    );
    await expect(page.locator(".pipeline-node-label").nth(3)).toContainText(
      "S3",
    );
    const connectors = page.locator(".pipeline-connector");
    await expect(connectors).toHaveCount(3);
  });

  test("pipeline nodes show status dots", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline")).toBeVisible({
      timeout: 10000,
    });
    // Use .pipeline-node .status-dot to count only pipeline node dots, not endpoint dots
    const dots = page.locator(".pipeline-node .status-dot");
    await expect(dots).toHaveCount(4);
  });

  test("state badge shows idle by default", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".state-badge")).toBeVisible({ timeout: 10000 });
    await expect(page.locator(".state-badge")).toContainText("Idle");
  });

  test("Start Delivering updates state badge after API call", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);
    // Select first event
    await page.locator(".event-selector").selectOption({ index: 1 });
    // Click start — mock broadcasts PipelineState with "buffering"
    await page.locator(".start-btn").click();
    // Wait for WebSocket PipelineState to update the badge
    await expect(page.locator(".state-badge")).toContainText(
      /Buffering|Streaming/,
      { timeout: 5000 },
    );
  });

  test("Stop Delivering returns to idle state", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);
    await page.locator(".event-selector").selectOption({ index: 1 });
    await page.locator(".start-btn").click();
    await page.waitForTimeout(500);
    // Now stop — confirm modal appears
    await page.locator(".stop-btn").click();
    await expect(page.locator(".confirm-modal")).toBeVisible({ timeout: 2000 });
    await page.locator(".confirm-btn-danger").click();
    await expect(page.locator(".state-badge")).toContainText("Idle", {
      timeout: 5000,
    });
  });

  test("Stop Delivering shows confirmation modal", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);
    await page.locator(".event-selector").selectOption({ index: 1 });
    await page.locator(".start-btn").click();
    await expect(page.locator(".state-badge")).toContainText(
      /Buffering|Streaming/,
      { timeout: 5000 },
    );

    // Click stop — modal appears
    await page.locator(".stop-btn").click();
    await expect(page.locator(".modal-overlay")).toBeVisible();
    await expect(page.locator(".confirm-modal")).toBeVisible();
    await expect(page.locator(".confirm-modal-title")).toHaveText(
      "Stop Delivering?",
    );
    await expect(page.locator(".confirm-modal-message")).toContainText(
      "stop all delivery",
    );

    // Dismiss to clean up
    await page.locator(".modal-cancel-btn").click();
  });

  test("Stop Delivering cancel dismisses modal without stopping", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);
    await page.locator(".event-selector").selectOption({ index: 1 });
    await page.locator(".start-btn").click();
    await expect(page.locator(".state-badge")).toContainText(
      /Buffering|Streaming/,
      { timeout: 5000 },
    );

    await page.locator(".stop-btn").click();
    await expect(page.locator(".confirm-modal")).toBeVisible();

    // Click cancel — modal closes, state unchanged
    await page.locator(".modal-cancel-btn").click();
    await expect(page.locator(".modal-overlay")).not.toBeVisible();
    await expect(page.locator(".state-badge")).toContainText(
      /Buffering|Streaming/,
    );
  });

  test("ConfirmModal click does not produce closure-dropped console errors", async ({
    page,
  }) => {
    // Regression for the bug where ConfirmModal's dismiss ran inside the
    // running click handler, freeing the wasm-bindgen Closure mid-call and
    // panicking with 'closure invoked recursively or after being dropped'.
    // The fix defers show.set(false) via setTimeout(0) so the click handler
    // returns before the button is unmounted.
    const consoleErrors: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() === "error") {
        consoleErrors.push(msg.text());
      }
    });

    await page.goto("/");
    await page.waitForTimeout(1000);
    await page.locator(".event-selector").selectOption({ index: 1 });
    await page.locator(".start-btn").click();
    await expect(page.locator(".state-badge")).toContainText(
      /Buffering|Streaming/,
      { timeout: 5000 },
    );

    // Open the Stop Delivering confirm modal
    await page.locator(".stop-btn").click();
    await expect(page.locator(".confirm-modal")).toBeVisible();

    // Click confirm — this is where the closure-drop bug used to fire
    await page.locator(".confirm-btn-danger").click();

    // Wait for the modal to fully unmount before checking errors
    await expect(page.locator(".modal-overlay")).not.toBeVisible({
      timeout: 3000,
    });
    await page.waitForTimeout(500);

    const closureErrors = consoleErrors.filter((e) =>
      e.includes("closure invoked recursively or after being dropped"),
    );
    expect(
      closureErrors,
      `ConfirmModal should not log closure errors. All errors: ${JSON.stringify(consoleErrors)}`,
    ).toEqual([]);
  });

  test("Stop Delivering confirm calls stop-stream API", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);
    await page.locator(".event-selector").selectOption({ index: 1 });
    await page.locator(".start-btn").click();
    await expect(page.locator(".state-badge")).toContainText(
      /Buffering|Streaming/,
      { timeout: 5000 },
    );

    await page.locator(".stop-btn").click();
    await expect(page.locator(".confirm-modal")).toBeVisible();

    const [request] = await Promise.all([
      page.waitForRequest(
        (req) => req.url().includes("/stop-stream") && req.method() === "POST",
      ),
      page.locator(".confirm-btn-danger").click(),
    ]);
    expect(request.url()).toContain("/events/1/stop-stream");
    await expect(page.locator(".modal-overlay")).not.toBeVisible();
  });

  test("Remove endpoint shows confirmation modal", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);
    await page.locator(".event-selector").selectOption({ index: 1 });
    await page.locator(".start-btn").click();
    await expect(page.locator(".state-badge")).toContainText(
      /Buffering|Streaming/,
      { timeout: 5000 },
    );

    // Simulate delivery with TWO endpoints so removing the first one hits
    // the regular ConfirmModal (the last-endpoint type-to-confirm modal
    // only triggers when endpoints.len() <= 1 during active delivery).
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "test-vps",
          status: "running",
          server_ip: "1.2.3.4",
          endpoint_count: 2,
          endpoints: [
            {
              alias: "YouTube Main",
              alive: true,
              current_chunk_id: 10,
              bytes_processed_total: 1000,
              chunks_processed: 10,
              chunk_delay_secs: 5.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
              is_fast: false,
            },
            {
              alias: "Facebook Page",
              alive: true,
              current_chunk_id: 10,
              bytes_processed_total: 1000,
              chunks_processed: 10,
              chunk_delay_secs: 5.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
              is_fast: false,
            },
          ],
        },
      },
    });

    await expect(page.locator(".btn-remove-endpoint").first()).toBeVisible({
      timeout: 5000,
    });
    await page.locator(".btn-remove-endpoint").first().click();

    // Confirm modal appears with endpoint name
    await expect(page.locator(".confirm-modal")).toBeVisible();
    await expect(page.locator(".confirm-modal-title")).toHaveText(
      "Remove Endpoint?",
    );
    await expect(page.locator(".confirm-modal-message")).toContainText(
      "YouTube Main",
    );

    // Cancel — endpoint stays
    await page.locator(".modal-cancel-btn").click();
    await expect(page.locator(".modal-overlay")).not.toBeVisible();
  });

  test("Remove endpoint confirm calls delivery remove API", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);
    await page.locator(".event-selector").selectOption({ index: 1 });
    await page.locator(".start-btn").click();
    await expect(page.locator(".state-badge")).toContainText(
      /Buffering|Streaming/,
      { timeout: 5000 },
    );

    // Two endpoints so removing the first one uses the regular confirm
    // flow, not the last-endpoint type-to-confirm flow.
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "test-vps",
          status: "running",
          server_ip: "1.2.3.4",
          endpoint_count: 2,
          endpoints: [
            {
              alias: "YouTube Main",
              alive: true,
              current_chunk_id: 10,
              bytes_processed_total: 1000,
              chunks_processed: 10,
              chunk_delay_secs: 5.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
              is_fast: false,
            },
            {
              alias: "Facebook Page",
              alive: true,
              current_chunk_id: 10,
              bytes_processed_total: 1000,
              chunks_processed: 10,
              chunk_delay_secs: 5.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
              is_fast: false,
            },
          ],
        },
      },
    });

    await expect(page.locator(".btn-remove-endpoint").first()).toBeVisible({
      timeout: 5000,
    });
    await page.locator(".btn-remove-endpoint").first().click();
    await expect(page.locator(".confirm-modal")).toBeVisible();

    const [request] = await Promise.all([
      page.waitForRequest(
        (req) =>
          req.url().includes("/delivery/endpoints/remove") &&
          req.method() === "POST",
      ),
      page.locator(".confirm-btn-danger").click(),
    ]);
    expect(request.postDataJSON().alias).toBe("YouTube Main");
    await expect(page.locator(".modal-overlay")).not.toBeVisible();
  });

  test("pipeline shows OBS Disconnected and RTMP Idle by default", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline")).toBeVisible({
      timeout: 10000,
    });
    const metrics = page.locator(".pipeline-node-metric");
    await expect(metrics.nth(0)).toHaveText("Disconnected");
    await expect(metrics.nth(1)).toHaveText("Idle");
  });

  test("OBS and RTMP status dots are not active by default", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline")).toBeVisible({
      timeout: 10000,
    });
    // Use .pipeline-node .status-dot to avoid picking up endpoint dots
    const dots = page.locator(".pipeline-node .status-dot");
    await expect(dots.nth(0)).not.toHaveClass(/active/);
    await expect(dots.nth(1)).not.toHaveClass(/active/);
  });

  test("pipeline shows RTMP Only after InpointStatus WebSocket event", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline-node-metric").nth(0)).toHaveText(
      "Disconnected",
      { timeout: 10000 },
    );

    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "InpointStatus",
        data: {
          state: "receiving",
          rtmp_connected: true,
          received_bytes: 1024,
          chunk_count: 5,
        },
      },
    });

    // Without OBS WebSocket connected, OBS node shows "RTMP Only" with warning dot
    await expect(page.locator(".pipeline-node-metric").nth(0)).toHaveText(
      "RTMP Only",
      { timeout: 5000 },
    );
    await expect(page.locator(".pipeline-node-metric").nth(1)).toContainText(
      "Receiving",
    );
    await expect(page.locator(".pipeline-node .status-dot").nth(0)).toHaveClass(
      /warning/,
    );
  });

  test("pipeline reverts to Disconnected after rtmp_connected=false", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline")).toBeVisible({
      timeout: 10000,
    });

    // Connect RTMP (without OBS WebSocket, shows "RTMP Only")
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "InpointStatus",
        data: {
          state: "receiving",
          rtmp_connected: true,
          received_bytes: 1024,
          chunk_count: 5,
        },
      },
    });
    await expect(page.locator(".pipeline-node-metric").nth(0)).toHaveText(
      "RTMP Only",
      { timeout: 5000 },
    );

    // Disconnect
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "InpointStatus",
        data: {
          state: "idle",
          rtmp_connected: false,
          received_bytes: 1024,
          chunk_count: 5,
        },
      },
    });
    await expect(page.locator(".pipeline-node-metric").nth(0)).toHaveText(
      "Disconnected",
      { timeout: 5000 },
    );
    await expect(page.locator(".pipeline-node-metric").nth(1)).toHaveText(
      "Idle",
    );
  });

  test("RTMP node shows absolute received_bytes, not session delta", async ({
    page,
  }) => {
    // Regression for Issue 6: after 10 hours of streaming the dashboard
    // showed ~3 MB instead of ~57 GB because the session-bytes computation
    // reset to 0 on every page load. The fix shows the absolute
    // received_bytes from InpointStatus directly.
    await page.goto("/");
    await expect(page.locator(".pipeline")).toBeVisible({ timeout: 10000 });

    const FIVE_GB: number = 5_000_000_000;
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "InpointStatus",
        data: {
          state: "receiving",
          rtmp_connected: true,
          received_bytes: FIVE_GB,
          chunk_count: 5000,
        },
      },
    });

    // The RTMP node is at index 1 in pipeline-node-metric. With absolute
    // bytes display the metric must contain "GB" (5 GB or 4.65 GB depending
    // on formatter). The old broken code showed bytes since the page opened,
    // which is approximately zero on a freshly-loaded page.
    await expect(page.locator(".pipeline-node-metric").nth(1)).toContainText(
      "GB",
      { timeout: 5000 },
    );
  });

  // --- Add Endpoint Modal ---

  test("add endpoint button opens modal when delivering", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);
    // Select first event and start delivering
    await page.locator(".event-selector").selectOption({ index: 1 });
    await page.locator(".start-btn").click();
    await expect(page.locator(".state-badge")).toContainText(
      /Buffering|Streaming/,
      { timeout: 5000 },
    );

    // Simulate delivery status via WebSocket so endpoint tree appears
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "test-vps",
          status: "running",
          server_ip: "1.2.3.4",
          endpoint_count: 1,
          endpoints: [
            {
              alias: "YouTube Main",
              alive: true,
              current_chunk_id: 10,
              bytes_processed_total: 1000,
              chunks_processed: 10,
              chunk_delay_secs: 5.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
              is_fast: false,
            },
          ],
        },
      },
    });

    // Wait for endpoint tree to render
    await expect(page.locator(".endpoint-tree")).toBeVisible({ timeout: 5000 });

    // Click Add button
    await page.locator(".btn-add-endpoint").click();

    // Modal should be visible
    await expect(page.locator(".modal-overlay")).toBeVisible();
    await expect(page.locator(".add-endpoint-modal")).toBeVisible();
    // Should show available endpoints (those not already active)
    await expect(page.locator(".modal-endpoint-row")).toHaveCount(1); // Facebook Page (YouTube Main is already active)
  });

  test("add endpoint modal calls delivery add API", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);
    await page.locator(".event-selector").selectOption({ index: 1 });
    await page.locator(".start-btn").click();
    await expect(page.locator(".state-badge")).toContainText(
      /Buffering|Streaming/,
      { timeout: 5000 },
    );

    // Simulate delivery running
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "test-vps",
          status: "running",
          server_ip: "1.2.3.4",
          endpoint_count: 0,
          endpoints: [],
        },
      },
    });

    await expect(page.locator(".endpoint-tree")).toBeVisible({ timeout: 5000 });
    await page.locator(".btn-add-endpoint").click();
    await expect(page.locator(".add-endpoint-modal")).toBeVisible();

    // Click on first available endpoint row
    await page.locator(".modal-endpoint-row").first().click();

    // Intercept the API call and click Add
    const [request] = await Promise.all([
      page.waitForRequest(
        (req) =>
          req.url().includes("/delivery/endpoints/add") &&
          req.method() === "POST",
      ),
      page.locator(".modal-add-btn").click(),
    ]);

    const body = request.postDataJSON();
    expect(body.endpoint_id).toBeDefined();
    expect(body.start_position).toBeDefined();

    // Modal should close after adding
    await expect(page.locator(".modal-overlay")).not.toBeVisible();
  });

  test("add endpoint modal closes on cancel", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);
    await page.locator(".event-selector").selectOption({ index: 1 });
    await page.locator(".start-btn").click();
    await expect(page.locator(".state-badge")).toContainText(
      /Buffering|Streaming/,
      { timeout: 5000 },
    );

    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "test-vps",
          status: "running",
          server_ip: "1.2.3.4",
          endpoint_count: 0,
          endpoints: [],
        },
      },
    });

    await expect(page.locator(".endpoint-tree")).toBeVisible({ timeout: 5000 });
    await page.locator(".btn-add-endpoint").click();
    await expect(page.locator(".add-endpoint-modal")).toBeVisible();

    // Click cancel
    await page.locator(".modal-cancel-btn").click();
    await expect(page.locator(".modal-overlay")).not.toBeVisible();
  });

  // --- Event Selector Lock ---

  test("event selector is disabled during active delivery", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);
    await page.locator(".event-selector").selectOption({ index: 1 });
    await page.locator(".start-btn").click();
    await expect(page.locator(".state-badge")).toContainText(
      /Buffering|Streaming/,
      { timeout: 5000 },
    );

    // Event selector should be disabled while delivering
    await expect(page.locator(".event-selector")).toBeDisabled();

    // Stop delivering — confirm modal
    await page.locator(".stop-btn").click();
    await expect(page.locator(".confirm-modal")).toBeVisible({ timeout: 2000 });
    await page.locator(".confirm-btn-danger").click();
    await expect(page.locator(".state-badge")).toContainText("Idle", {
      timeout: 5000,
    });

    // Event selector should be enabled again
    await expect(page.locator(".event-selector")).toBeEnabled();
  });

  test("auto-selects actively delivering event on page load", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);
    // Select first event and start
    await page.locator(".event-selector").selectOption({ index: 1 });
    await page.locator(".start-btn").click();
    await expect(page.locator(".state-badge")).toContainText(
      /Buffering|Streaming/,
      { timeout: 5000 },
    );

    // Reload the page — events_list is fetched via HTTP on mount
    await page.reload();
    await page.waitForTimeout(2000);

    // Event selector should auto-select the delivering event
    // (Effect reads events_list and finds delivering_activated=true)
    const selectedValue = await page.locator(".event-selector").inputValue();
    expect(selectedValue).not.toBe("");
  });
});

// --- Settings Page (/settings) ---

test.describe("Settings page", () => {
  test("settings page loads with events section", async ({ page }) => {
    await page.goto("/settings");
    await expect(page.locator(".settings-page")).toBeVisible({
      timeout: 10000,
    });
    await expect(page.locator(".settings-page > h2").first()).toHaveText(
      "Settings",
    );
  });

  test("settings shows back arrow to dashboard", async ({ page }) => {
    await page.goto("/settings");
    const backLink = page.locator('.header-nav-btn:has-text("Dashboard")');
    await expect(backLink).toBeVisible({ timeout: 10000 });
  });

  test("endpoints section renders with create form", async ({ page }) => {
    await page.goto("/settings");
    await expect(page.locator(".endpoints-tab")).toBeVisible({
      timeout: 10000,
    });
    await expect(page.locator(".endpoints-tab .create-form")).toBeVisible();
  });

  test("endpoint list shows existing endpoints with aliases", async ({
    page,
  }) => {
    await page.goto("/settings");
    await page.waitForTimeout(1000);
    const section = page.locator(".endpoints-tab");
    const cards = section.locator(".endpoint-card");
    await expect(cards).toHaveCount(2);
    const cardTexts = await cards.allTextContents();
    const allText = cardTexts.join(" ");
    expect(allText).toContain("YouTube Main");
    expect(allText).toContain("Facebook Page");
  });

  test("navigating to dashboard from settings works", async ({ page }) => {
    await page.goto("/settings");
    await page.locator('.header-nav-btn:has-text("Dashboard")').click();
    await expect(page.locator(".operator-dashboard")).toBeVisible({
      timeout: 10000,
    });
  });
});

// --- Endpoint Editing ---

test.describe("Endpoint Editing", () => {
  test("endpoint edit shows stream key with show/hide toggle", async ({
    page,
  }) => {
    await page.goto("/settings");
    await page.waitForTimeout(1000);
    const section = page.locator(".endpoints-tab");
    // Click Edit on the first endpoint
    await section
      .locator(".endpoint-card")
      .first()
      .locator('button:has-text("Edit")')
      .click();
    // Should show edit form with stream key field
    await expect(section.locator(".endpoint-edit-form")).toBeVisible({
      timeout: 5000,
    });
    // Key input should be a password field by default
    const keyInput = section.locator(".key-input-wrapper input");
    await expect(keyInput).toBeVisible();
    await expect(keyInput).toHaveAttribute("type", "password");
    // Click Show button
    await section.locator(".toggle-key-btn").click();
    await expect(keyInput).toHaveAttribute("type", "text");
    // Click Hide button
    await section.locator(".toggle-key-btn").click();
    await expect(keyInput).toHaveAttribute("type", "password");
  });

  test("endpoint edit form shows correct service type for non-HLS endpoint", async ({
    page,
  }) => {
    await page.goto("/settings");
    await page.waitForTimeout(1000);
    const section = page.locator(".endpoints-tab");
    // Click Edit on the SECOND endpoint (Facebook Page, type=FB)
    await section
      .locator(".endpoint-card")
      .nth(1)
      .locator('button:has-text("Edit")')
      .click();
    await expect(section.locator(".endpoint-edit-form")).toBeVisible({
      timeout: 5000,
    });
    // The type dropdown MUST show "FB", not "YT_HLS"
    const typeSelect = section.locator(
      '.edit-row:has(label:text("Type")) select',
    );
    await expect(typeSelect).toHaveValue("FB");
  });

  test("saving endpoint preserves original service type when unchanged", async ({
    page,
  }) => {
    await page.goto("/settings");
    await page.waitForTimeout(1000);
    const section = page.locator(".endpoints-tab");
    // Edit second endpoint (FB type) — only change alias, don't touch type
    await section
      .locator(".endpoint-card")
      .nth(1)
      .locator('button:has-text("Edit")')
      .click();
    await expect(section.locator(".endpoint-edit-form")).toBeVisible({
      timeout: 5000,
    });
    const aliasInput = section.locator(
      '.edit-row:has(label:text("Alias")) input',
    );
    await aliasInput.clear();
    await aliasInput.fill("Facebook Updated");
    // Intercept the PUT — service_type MUST be "FB", not "YT_HLS"
    const [request] = await Promise.all([
      page.waitForRequest(
        (req) => req.url().includes("/endpoints/") && req.method() === "PUT",
      ),
      section.locator('button:has-text("Save")').click(),
    ]);
    const body = request.postDataJSON();
    expect(body.service_type).toBe("FB");
  });

  test("endpoint edit saves changes", async ({ page }) => {
    await page.goto("/settings");
    await page.waitForTimeout(1000);
    const section = page.locator(".endpoints-tab");
    // Click Edit on the first endpoint
    await section
      .locator(".endpoint-card")
      .first()
      .locator('button:has-text("Edit")')
      .click();
    await expect(section.locator(".endpoint-edit-form")).toBeVisible({
      timeout: 5000,
    });
    // Clear and change alias
    const aliasInput = section.locator(
      '.edit-row:has(label:text("Alias")) input',
    );
    await aliasInput.clear();
    await aliasInput.fill("YouTube Updated");
    // Intercept the PUT call
    const [request] = await Promise.all([
      page.waitForRequest(
        (req) => req.url().includes("/endpoints/") && req.method() === "PUT",
      ),
      section.locator('button:has-text("Save")').click(),
    ]);
    const body = request.postDataJSON();
    expect(body.alias).toBe("YouTube Updated");
  });
});

// --- Per-Endpoint Cache Bar ---

test.describe("Per-Endpoint Cache Bar", () => {
  test("endpoint card shows cache bar when delivering", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast DeliveryStatus with an endpoint that has chunk_delay_secs
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "e2e-vps",
          status: "delivering",
          server_ip: "1.2.3.4",
          endpoint_count: 1,
          endpoints: [
            {
              alias: "e2e rtmp",
              alive: true,
              current_chunk_id: 20,
              bytes_processed_total: 1000000,
              chunks_processed: 20,
              chunk_delay_secs: 90.0,
              is_fast: false,
            },
          ],
        },
      },
    });
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "streaming",
          event_id: 1,
          event_name: "Test Event",
          target_delay_secs: 120,
          session_start: null,
          local_buffer_chunks: 0,
          s3_queue_chunks: 20,
          cache_duration_secs: 90.0,
        },
      },
    });

    // Cache bar fill should be visible with healthy class (90/120 = 75%)
    const healthyBar = page.locator(".endpoint-node .buffer-bar-fill.healthy");
    await expect(healthyBar).toBeVisible({ timeout: 5000 });
    const cacheLabel = page.locator(".endpoint-cache-label");
    await expect(cacheLabel).toContainText("90s / 120s cache");
  });

  // Regression test for the per-endpoint cache label bug. The dashboard
  // previously displayed a single global `ps.cache_duration_secs` on every
  // endpoint's cache bar, so two endpoints with very different per-endpoint
  // delays would both show the same label. This hid drift on individual
  // endpoints and made it look like "all endpoints are going down" when in
  // fact only a subset were affected. This test asserts each endpoint's
  // cache label shows ITS OWN chunk_delay_secs.
  test("per-endpoint cache label shows individual chunk_delay_secs", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast PipelineState with a global cache_duration_secs that is
    // DIFFERENT from both endpoint delays below. If the dashboard
    // accidentally uses this global value for per-endpoint cache bars,
    // both endpoints would show "75s" instead of their individual delays.
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "streaming",
          event_id: 1,
          event_name: "Test Event",
          target_delay_secs: 120,
          session_start: null,
          local_buffer_chunks: 0,
          s3_queue_chunks: 38,
          cache_duration_secs: 75.0,
        },
      },
    });

    // Two endpoints with very different per-endpoint delays:
    //   - YT stable:  chunk_delay_secs = 118s (healthy)
    //   - FB drifted: chunk_delay_secs = 35s (critical, e.g., stale key)
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "e2e-vps",
          status: "delivering",
          server_ip: "1.2.3.4",
          endpoint_count: 2,
          endpoints: [
            {
              alias: "YT Stable",
              alive: true,
              current_chunk_id: 500,
              bytes_processed_total: 2000000,
              chunks_processed: 500,
              chunk_delay_secs: 118.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
              is_fast: false,
            },
            {
              alias: "FB Drifted",
              alive: true,
              current_chunk_id: 535,
              bytes_processed_total: 1500000,
              chunks_processed: 500,
              chunk_delay_secs: 35.0,
              stall_reason: null,
              ffmpeg_restart_count: 12,
              last_error: "ffmpeg stdin closed",
              is_fast: false,
            },
          ],
        },
      },
    });

    // Wait for both endpoints to render
    await expect(page.locator(".endpoint-node")).toHaveCount(2, {
      timeout: 5000,
    });

    // Each endpoint must display ITS OWN chunk_delay_secs, not the shared
    // global (75s). Scope the cache label lookup to each endpoint node.
    const ytNode = page.locator(".endpoint-node", { hasText: "YT Stable" });
    const fbNode = page.locator(".endpoint-node", { hasText: "FB Drifted" });

    await expect(ytNode.locator(".endpoint-cache-label")).toContainText(
      "118s / 120s cache",
    );
    await expect(fbNode.locator(".endpoint-cache-label")).toContainText(
      "35s / 120s cache",
    );

    // Bar color must also reflect per-endpoint delay: YT is healthy (>75%),
    // FB is critical (<40%).
    await expect(
      ytNode.locator(".buffer-bar-fill.healthy"),
    ).toBeVisible();
    await expect(
      fbNode.locator(".buffer-bar-fill.critical"),
    ).toBeVisible();
  });

  test("cache bar color changes with delay level", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Warning level: cache_duration_secs = 60 (50% of 120, between 40-75%)
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "streaming",
          event_id: 1,
          event_name: "Test Event",
          target_delay_secs: 120,
          session_start: null,
          local_buffer_chunks: 0,
          s3_queue_chunks: 20,
          cache_duration_secs: 60.0,
        },
      },
    });
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "e2e-vps",
          status: "delivering",
          server_ip: "1.2.3.4",
          endpoint_count: 1,
          endpoints: [
            {
              alias: "e2e rtmp",
              alive: true,
              current_chunk_id: 20,
              bytes_processed_total: 1000000,
              chunks_processed: 20,
              chunk_delay_secs: 60.0,
              is_fast: false,
            },
          ],
        },
      },
    });
    const warningBar = page.locator(".endpoint-node .buffer-bar-fill.warning");
    await expect(warningBar).toBeVisible({ timeout: 5000 });

    // Critical level: chunk_delay_secs = 10 (8% of 120). Update BOTH the
    // per-endpoint delay and the pipeline cache_duration_secs. The cache bar
    // now reads per-endpoint chunk_delay_secs (see commit 018af89), so a
    // pipeline-only update is insufficient.
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "streaming",
          event_id: 1,
          event_name: "Test Event",
          target_delay_secs: 120,
          session_start: null,
          local_buffer_chunks: 0,
          s3_queue_chunks: 22,
          cache_duration_secs: 10.0,
        },
      },
    });
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "e2e-vps",
          status: "delivering",
          server_ip: "1.2.3.4",
          endpoint_count: 1,
          endpoints: [
            {
              alias: "e2e rtmp",
              alive: true,
              current_chunk_id: 22,
              bytes_processed_total: 1100000,
              chunks_processed: 22,
              chunk_delay_secs: 10.0,
              is_fast: false,
            },
          ],
        },
      },
    });
    const criticalBar = page.locator(
      ".endpoint-node .buffer-bar-fill.critical",
    );
    await expect(criticalBar).toBeVisible({ timeout: 5000 });
  });

  test("pending endpoint shows S3 cache from first second", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast PipelineState with S3 chunks buffered and real cache duration
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "buffering",
          event_id: 1,
          event_name: "Test Event",
          target_delay_secs: 120,
          session_start: null,
          local_buffer_chunks: 0,
          s3_queue_chunks: 20,
          cache_duration_secs: 100.0,
        },
      },
    });

    // Broadcast endpoint with alive=false, chunks_processed=0
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "e2e-vps",
          status: "creating",
          server_ip: null,
          endpoint_count: 1,
          endpoints: [
            {
              alias: "e2e rtmp",
              alive: false,
              current_chunk_id: 0,
              bytes_processed_total: 0,
              chunks_processed: 0,
              chunk_delay_secs: 0.0,
              is_fast: false,
            },
          ],
        },
      },
    });

    // Wait for endpoint node to appear
    await expect(page.locator(".endpoint-node")).toHaveCount(1, {
      timeout: 5000,
    });
    // Pending endpoint should show cache bar using backend-computed cache duration
    const cacheLabel = page.locator(".endpoint-cache-label");
    await expect(cacheLabel).toContainText("100s / 120s cache", {
      timeout: 5000,
    });
    // 100/120 = 83% -> healthy
    await expect(page.locator(".buffer-bar-fill.healthy")).toBeVisible();
  });

  test("buffering indicator shows S3 chunk count", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast PipelineState in buffering state with S3 chunks and real cache duration
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "buffering",
          event_id: 1,
          event_name: "Test Event",
          target_delay_secs: 120,
          session_start: null,
          local_buffer_chunks: 0,
          s3_queue_chunks: 16,
          cache_duration_secs: 16.5,
        },
      },
    });

    // Broadcast DeliveryStatus with no alive endpoints
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "e2e-vps",
          status: "creating",
          server_ip: null,
          endpoint_count: 0,
          endpoints: [],
        },
      },
    });

    // Buffering indicator should show S3 chunk count and cache duration
    const indicator = page.locator(".buffering-indicator");
    await expect(indicator).toBeVisible({ timeout: 5000 });
    await expect(indicator).toContainText("16 chunks");
    await expect(indicator).toContainText("16s");
  });
});

// --- Pending Endpoint State ---

test.describe("Pending Endpoint State", () => {
  test("pending endpoints show with pending CSS class", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast DeliveryStatus with placeholder endpoints (alive=false, chunks=0)
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "rs-delivery-1",
          status: "creating",
          server_ip: null,
          endpoint_count: 2,
          endpoints: [
            {
              alias: "YouTube Main",
              alive: false,
              current_chunk_id: 0,
              bytes_processed_total: 0,
              chunks_processed: 0,
              chunk_delay_secs: 0.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
            },
            {
              alias: "Facebook Page",
              alive: false,
              current_chunk_id: 0,
              bytes_processed_total: 0,
              chunks_processed: 0,
              chunk_delay_secs: 0.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
            },
          ],
        },
      },
    });

    // Endpoint nodes should appear with pending class
    const pendingNodes = page.locator(".endpoint-node.pending");
    await expect(pendingNodes).toHaveCount(2, { timeout: 5000 });
  });

  test("pending endpoints transition to alive state", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Start with pending endpoints
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "rs-delivery-1",
          status: "creating",
          server_ip: null,
          endpoint_count: 1,
          endpoints: [
            {
              alias: "YouTube Main",
              alive: false,
              current_chunk_id: 0,
              bytes_processed_total: 0,
              chunks_processed: 0,
              chunk_delay_secs: 0.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
            },
          ],
        },
      },
    });
    await expect(page.locator(".endpoint-node.pending")).toHaveCount(1, {
      timeout: 5000,
    });

    // Transition to alive
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "rs-delivery-1",
          status: "running",
          server_ip: "1.2.3.4",
          endpoint_count: 1,
          endpoints: [
            {
              alias: "YouTube Main",
              alive: true,
              current_chunk_id: 42,
              bytes_processed_total: 1048576,
              chunks_processed: 100,
              chunk_delay_secs: 45.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
            },
          ],
        },
      },
    });

    // Pending class should be gone, healthy class should appear
    await expect(page.locator(".endpoint-node.pending")).toHaveCount(0, {
      timeout: 5000,
    });
    await expect(page.locator(".endpoint-node.healthy")).toBeVisible();
  });
});

// --- Delivery Endpoint Add/Remove Controls ---

test.describe("Delivery Endpoint Add/Remove Controls", () => {
  test("add endpoint button appears when delivery is running", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast running delivery status
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "rs-delivery-1",
          status: "running",
          server_ip: "1.2.3.4",
          endpoint_count: 1,
          endpoints: [
            {
              alias: "YouTube Main",
              alive: true,
              current_chunk_id: 42,
              bytes_processed_total: 1000000,
              chunks_processed: 40,
              chunk_delay_secs: 15.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
              is_fast: false,
            },
          ],
        },
      },
    });

    // Add endpoint button should be visible (opens modal on click)
    await expect(page.locator(".btn-add-endpoint")).toBeVisible({
      timeout: 5000,
    });
  });

  test("remove button appears on endpoint nodes when delivering", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast running delivery status
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "rs-delivery-1",
          status: "running",
          server_ip: "1.2.3.4",
          endpoint_count: 1,
          endpoints: [
            {
              alias: "YouTube Main",
              alive: true,
              current_chunk_id: 42,
              bytes_processed_total: 1000000,
              chunks_processed: 40,
              chunk_delay_secs: 15.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
              is_fast: false,
            },
          ],
        },
      },
    });

    // Remove button (×) should appear on the endpoint node
    const removeBtn = page.locator(".btn-remove-endpoint");
    await expect(removeBtn).toBeVisible({ timeout: 5000 });
  });

  test("add/remove controls hidden when delivery is idle", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast idle delivery status to clear any leftover state from previous tests
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "",
          status: "none",
          server_ip: null,
          endpoint_count: 0,
          endpoints: [],
        },
      },
    });
    await page.waitForTimeout(500);

    // Idle state — no add/remove controls
    await expect(page.locator(".btn-add-endpoint")).not.toBeVisible();
    await expect(page.locator(".btn-remove-endpoint")).not.toBeVisible();
  });
});

// --- Cache Bar Health Colors ---
// (Removed: global cache bar replaced by per-endpoint cache bar — see "Per-Endpoint Cache Bar" suite)

// --- Pipeline Node Data ---

test.describe("Pipeline Node Data", () => {
  test("pipeline shows buffer metric and VPS metric when delivering", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast PipelineState with buffer data
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "streaming",
          event_id: 1,
          event_name: "Sunday Service",
          target_delay_secs: 120,
          session_start: null,
          local_buffer_chunks: 5,
          s3_queue_chunks: 42,
        },
      },
    });

    // Also broadcast delivery status
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "rs-delivery-1",
          status: "running",
          server_ip: "1.2.3.4",
          endpoint_count: 1,
          endpoints: [
            {
              alias: "YouTube Main",
              alive: true,
              current_chunk_id: 1200,
              bytes_processed_total: 5000000,
              chunks_processed: 1203,
              chunk_delay_secs: 96.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
            },
          ],
        },
      },
    });

    // Local Buffer node (index 2) shows chunk count when delivering
    const metrics = page.locator(".pipeline-node-metric");
    await expect(metrics.nth(2)).toContainText("5 chunks", { timeout: 5000 });
    // VPS node (index 3) shows queued + endpoints count
    await expect(metrics.nth(3)).toContainText("42 queued");
  });

  test("pipeline shows pending chunks when idle (not delivering)", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast chunk stats via EndpointStatus (idle, no delivery)
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "EndpointStatus",
        data: {
          state: "uploading",
          pending_chunks: 8,
          active_uploads: 1,
          buffer_duration: "00:00:30",
        },
      },
    });

    // Buffer node should show pending_chunks when idle
    const metrics = page.locator(".pipeline-node-metric");
    await expect(metrics.nth(2)).toContainText("8 chunks", { timeout: 5000 });
  });

  test("buffer dot is gray when not delivering", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline")).toBeVisible({
      timeout: 10000,
    });
    const bufferDot = page.locator(".pipeline-node .status-dot").nth(2);
    await expect(bufferDot).not.toHaveClass(/active/);
    await expect(bufferDot).not.toHaveClass(/warning/);
    await expect(bufferDot).not.toHaveClass(/error/);
  });

  test("buffer dot shows healthy when delivering with good progress", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Streaming state — S3 dot shows active when delivery is running
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "streaming",
          event_id: 1,
          event_name: "Test",
          target_delay_secs: 120,
          session_start: null,
          local_buffer_chunks: 0,
          s3_queue_chunks: 53,
        },
      },
    });

    // S3 dot is at index 3 (OBS=0, RTMP=1, Local Buffer=2, S3/VPS=3)
    const s3Dot = page.locator(".pipeline-node .status-dot").nth(3);
    await expect(s3Dot).toHaveClass(/active/, { timeout: 5000 });
  });

  test("VPS dot is active when delivery running", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "rs-delivery-1",
          status: "running",
          server_ip: "1.2.3.4",
          endpoint_count: 1,
          endpoints: [
            {
              alias: "YouTube Main",
              alive: true,
              current_chunk_id: 42,
              bytes_processed_total: 1000000,
              chunks_processed: 40,
              chunk_delay_secs: 15.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
            },
          ],
        },
      },
    });

    const vpsDot = page.locator(".pipeline-node .status-dot").nth(3);
    await expect(vpsDot).toHaveClass(/active/, { timeout: 5000 });
  });
});

// --- Navigation ---

test.describe("Navigation", () => {
  test("navigating to settings from dashboard works", async ({ page }) => {
    await page.goto("/");
    await page.locator('.header-nav-btn:has-text("Settings")').click();
    await expect(page.locator(".settings-page")).toBeVisible({
      timeout: 10000,
    });
  });

  test("unknown route shows fallback", async ({ page }) => {
    await page.goto("/nonexistent");
    await expect(page.locator(".empty")).toContainText("Page not found");
  });
});

test.describe("YouTube Health Badge", () => {
  test("YouTube endpoint node shows health badge after polling", async ({
    page,
  }) => {
    await page.goto("/");
    // Wait for WebSocket delivery status (includes "YouTube Main" endpoint)
    // and for the initial YouTube health poll to fire (5s interval detects endpoints, then fetches)
    const ytNode = page.locator(
      '.endpoint-node:has(.endpoint-alias:has-text("YouTube Main"))',
    );
    await expect(ytNode).toBeVisible({ timeout: 10000 });
    const badge = ytNode.locator(".yt-health-badge");
    // Badge renders immediately as "unknown", then updates after poll fetches YouTube status
    await expect(badge).toHaveClass(/good/, { timeout: 15000 });
    await expect(badge).toHaveText("good");
  });

  test("non-YouTube endpoint does not show health badge", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(2000);

    const fbNode = page.locator(
      '.endpoint-node:has(.endpoint-alias:has-text("Facebook Page"))',
    );
    await expect(fbNode).toBeVisible({ timeout: 5000 });
    const badge = fbNode.locator(".yt-health-badge");
    await expect(badge).toHaveCount(0);
  });
});

// --- Endpoint Tree ---

test.describe("Endpoint Tree", () => {
  test("endpoint tree shows branches when delivering", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "test-vps",
          status: "delivering",
          server_ip: "1.2.3.4",
          endpoint_count: 2,
          endpoints: [
            {
              alias: "YT-Main",
              alive: true,
              current_chunk_id: 100,
              bytes_processed_total: 1000000,
              chunks_processed: 100,
              chunk_delay_secs: 12.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
              is_fast: false,
            },
            {
              alias: "FB-Stream",
              alive: true,
              current_chunk_id: 95,
              bytes_processed_total: 800000,
              chunks_processed: 95,
              chunk_delay_secs: 45.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
              is_fast: false,
            },
          ],
        },
      },
    });

    // 2 endpoint branches + 1 AddEndpointControl branch (visible when delivering)
    const branches = page.locator(".endpoint-branch");
    await expect(branches).toHaveCount(3, { timeout: 5000 });
    await expect(page.locator(".endpoint-alias").nth(0)).toContainText(
      "YT-Main",
    );
    await expect(page.locator(".endpoint-alias").nth(1)).toContainText(
      "FB-Stream",
    );
  });

  test("endpoint shows anomaly only when unhealthy", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "DeliveryStatus",
        data: {
          instance_name: "test-vps",
          status: "delivering",
          server_ip: "1.2.3.4",
          endpoint_count: 2,
          endpoints: [
            {
              alias: "YT-Main",
              alive: true,
              current_chunk_id: 100,
              bytes_processed_total: 1000000,
              chunks_processed: 100,
              chunk_delay_secs: 12.0,
              stall_reason: null,
              ffmpeg_restart_count: 0,
              last_error: null,
              is_fast: false,
            },
            {
              alias: "YT-Monitor",
              alive: false,
              current_chunk_id: 80,
              bytes_processed_total: 500000,
              chunks_processed: 80,
              chunk_delay_secs: 0.0,
              stall_reason: "chunk_miss",
              ffmpeg_restart_count: 3,
              last_error: null,
              is_fast: true,
            },
          ],
        },
      },
    });

    // Healthy endpoint: no anomaly text visible
    const healthyNode = page.locator(".endpoint-node").nth(0);
    await expect(healthyNode).toBeVisible({ timeout: 5000 });
    await expect(healthyNode.locator(".endpoint-anomaly")).toHaveCount(0);
    // Unhealthy endpoint: shows anomaly (stall + ffmpeg restarts = 2 spans)
    const unhealthyNode = page.locator(".endpoint-node").nth(1);
    const anomalies = unhealthyNode.locator(".endpoint-anomaly");
    await expect(anomalies).toHaveCount(2);
    await expect(anomalies.nth(0)).toContainText("stall");
    await expect(anomalies.nth(1)).toContainText("ffmpeg");
  });
});

// --- Mobile Viewport ---

test.describe("Mobile Viewport", () => {
  test("mobile viewport renders without horizontal scroll", async ({
    page,
  }) => {
    await page.setViewportSize({ width: 375, height: 812 });
    await page.goto("/");
    const scrollWidth = await page.evaluate(
      () => document.documentElement.scrollWidth,
    );
    const clientWidth = await page.evaluate(
      () => document.documentElement.clientWidth,
    );
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth);
    await expect(page.locator(".pipeline-node").first()).toBeVisible();
  });
});

// --- PWA Manifest ---

test.describe("PWA Manifest", () => {
  test("PWA manifest is served", async ({ page }) => {
    const response = await page.goto("/manifest.json");
    expect(response?.status()).toBe(200);
    const manifest = await response?.json();
    expect(manifest.name).toBe("Restreamer");
    expect(manifest.display).toBe("standalone");
    expect(manifest.theme_color).toBe("#0f172a");
  });
});

// --- Templates Management ---

test.describe("Templates Management", () => {
  test("templates tab shows template list", async ({ page }) => {
    await page.goto("/settings");
    await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

    // Click the Templates tab
    await page.locator(".settings-tabs button:has-text('Templates')").click();
    await page.waitForTimeout(1000);

    // Should show the templates-tab container
    await expect(page.locator(".templates-tab")).toBeVisible({ timeout: 5000 });

    // Should show 2 mock template cards
    await expect(
      page.locator(".templates-tab .items-list .settings-card"),
    ).toHaveCount(2, { timeout: 5000 });
  });

  test("create template calls POST /api/v1/templates", async ({ page }) => {
    await page.goto("/settings");
    await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

    await page.locator(".settings-tabs button:has-text('Templates')").click();
    await expect(page.locator(".templates-tab")).toBeVisible({ timeout: 5000 });

    // Intercept the POST request
    const requestPromise = page.waitForRequest(
      (req) =>
        req.url().includes("/api/v1/templates") && req.method() === "POST",
    );

    // Fill the template name input and click Create
    await page
      .locator('.templates-tab .create-form input[placeholder="Template name"]')
      .fill("special-event");
    await page
      .locator(".templates-tab .create-form button:has-text('Create Template')")
      .click();

    const request = await requestPromise;
    const body = request.postDataJSON();
    expect(body.name).toBe("special-event");
  });

  test("delete template calls DELETE /api/v1/templates/:id", async ({
    page,
  }) => {
    await page.goto("/settings");
    await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

    await page.locator(".settings-tabs button:has-text('Templates')").click();
    await expect(
      page.locator(".templates-tab .items-list .settings-card"),
    ).toHaveCount(2, { timeout: 5000 });

    const requestPromise = page.waitForRequest(
      (req) =>
        req.url().match(/\/api\/v1\/templates\/\d+$/) !== null &&
        req.method() === "DELETE",
    );

    // Click delete on first template card
    await page
      .locator(".templates-tab .items-list .settings-card")
      .first()
      .locator("button.btn-danger")
      .click();

    const request = await requestPromise;
    expect(request.url()).toMatch(/\/api\/v1\/templates\/\d+$/);
  });
});

// --- Events Management Tab ---

test.describe("Events Management Tab", () => {
  test("events tab shows event list", async ({ page }) => {
    await page.goto("/settings");
    await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

    // Click the Events tab
    await page.locator(".settings-tabs button:has-text('Events')").click();
    await page.waitForTimeout(1000);

    // Should show the events-management-tab container
    await expect(page.locator(".events-management-tab")).toBeVisible({
      timeout: 5000,
    });

    // Should show 2 mock event cards
    await expect(
      page.locator(".events-management-tab .items-list .settings-card"),
    ).toHaveCount(2, { timeout: 5000 });
  });

  test("create event from template opens picker modal", async ({ page }) => {
    await page.goto("/settings");
    await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

    await page.locator(".settings-tabs button:has-text('Events')").click();
    await expect(page.locator(".events-management-tab")).toBeVisible({
      timeout: 5000,
    });

    // Click "New from Template" button
    await page
      .locator(".events-management-tab button.btn-primary:has-text('New from Template')")
      .click();

    // Modal should appear
    await expect(page.locator(".modal-overlay")).toBeVisible({ timeout: 3000 });
    await expect(
      page.locator(".confirm-modal-title:has-text('New Event from Template')"),
    ).toBeVisible();

    // Should list the 2 mock templates as picker buttons
    await expect(page.locator(".template-pick-btn")).toHaveCount(2, {
      timeout: 5000,
    });

    // Dismiss modal
    await page.locator(".modal-cancel-btn").click();
    await expect(page.locator(".modal-overlay")).not.toBeVisible();
  });

  test("create event from template calls POST /api/v1/events with template_id", async ({
    page,
  }) => {
    await page.goto("/settings");
    await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

    await page.locator(".settings-tabs button:has-text('Events')").click();
    await expect(page.locator(".events-management-tab")).toBeVisible({
      timeout: 5000,
    });

    // Open the template picker
    await page
      .locator(".events-management-tab button.btn-primary:has-text('New from Template')")
      .click();
    await expect(page.locator(".template-pick-btn")).toHaveCount(2, {
      timeout: 5000,
    });

    // Intercept the POST /events request
    const requestPromise = page.waitForRequest(
      (req) =>
        req.url().includes("/api/v1/events") && req.method() === "POST",
    );

    // Click on the first template (sunday-service, id=1)
    await page
      .locator(".template-pick-btn:has-text('sunday-service')")
      .click();

    const request = await requestPromise;
    const body = request.postDataJSON();
    expect(body.template_id).toBe(1);
  });

  test("delete event shows confirmation modal", async ({ page }) => {
    await page.goto("/settings");
    await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

    await page.locator(".settings-tabs button:has-text('Events')").click();
    await expect(
      page.locator(".events-management-tab .items-list .settings-card"),
    ).toHaveCount(2, { timeout: 5000 });

    // Click delete on the first non-streaming event
    await page
      .locator(".events-management-tab .items-list .settings-card")
      .first()
      .locator("button.btn-danger:has-text('Delete + Cleanup')")
      .click();

    // Confirmation modal should appear
    await expect(page.locator(".confirm-modal")).toBeVisible({ timeout: 3000 });
    await expect(
      page.locator(".confirm-modal-title:has-text('Delete Event')"),
    ).toBeVisible();
    await expect(page.locator(".confirm-modal-message")).toContainText(
      "clean up S3 chunks",
    );

    // Cancel to clean up
    await page.locator(".modal-cancel-btn").click();
    await expect(page.locator(".modal-overlay")).not.toBeVisible();
  });

  test("delete event confirm calls DELETE /api/v1/events/:id", async ({
    page,
  }) => {
    await page.goto("/settings");
    await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

    await page.locator(".settings-tabs button:has-text('Events')").click();
    await expect(
      page.locator(".events-management-tab .items-list .settings-card"),
    ).toHaveCount(2, { timeout: 5000 });

    // Click delete on the first event
    await page
      .locator(".events-management-tab .items-list .settings-card")
      .first()
      .locator("button.btn-danger:has-text('Delete + Cleanup')")
      .click();

    await expect(page.locator(".confirm-modal")).toBeVisible({ timeout: 3000 });

    const requestPromise = page.waitForRequest(
      (req) =>
        req.url().match(/\/api\/v1\/events\/\d+$/) !== null &&
        req.method() === "DELETE",
    );

    // Confirm the deletion
    await page.locator(".confirm-btn-danger").click();

    const request = await requestPromise;
    expect(request.url()).toMatch(/\/api\/v1\/events\/\d+$/);
  });

  test("event card shows assigned endpoint badges", async ({ page }) => {
    await page.goto("/settings");
    await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

    await page.locator(".settings-tabs button:has-text('Events')").click();
    await page.waitForTimeout(1000);

    // Find the first event card and verify endpoint tag(s) visible inside it
    const firstCard = page
      .locator(".events-management-tab .settings-card")
      .first();
    await expect(firstCard.locator(".endpoint-tag")).toHaveCount(1, {
      timeout: 5000,
    });
  });

  test("event card shows editable cache delay input", async ({ page }) => {
    await page.goto("/settings");
    await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

    await page.locator(".settings-tabs button:has-text('Events')").click();
    await page.waitForTimeout(1000);

    const firstCard = page
      .locator(".events-management-tab .settings-card")
      .first();
    await expect(firstCard.locator(".cache-delay-input")).toBeVisible({
      timeout: 5000,
    });
  });

  test("event card shows rescue video URL input and saves via PATCH", async ({
    page,
  }) => {
    await page.goto("/settings");
    await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

    await page.locator(".settings-tabs button:has-text('Events')").click();
    await page.waitForTimeout(1000);

    const firstCard = page
      .locator(".events-management-tab .settings-card")
      .first();

    // The rescue video input reuses the .cache-delay-input class inside a
    // .cache-edit block whose label reads "Rescue video URL:"
    const rescueInput = firstCard
      .locator(".cache-edit")
      .filter({ hasText: "Rescue video URL:" })
      .locator("input.rescue-video-input");
    await expect(rescueInput).toBeVisible({ timeout: 5000 });

    // Track PATCH requests to verify the value is sent
    const patchBodies: string[] = [];
    await page.route("**/api/v1/events/*", async (route) => {
      if (route.request().method() === "PATCH") {
        const body = route.request().postData() ?? "";
        patchBodies.push(body);
      }
      await route.continue();
    });

    const testUrl = "https://s3.example.com/rescue-test.mp4";
    await rescueInput.fill(testUrl);

    // Click the Save button in the same .cache-edit block
    const saveBtn = firstCard
      .locator(".cache-edit")
      .filter({ hasText: "Rescue video URL:" })
      .locator("button.btn-small");
    await saveBtn.click();

    // Wait for the PATCH to fire
    await page.waitForTimeout(500);

    // At least one PATCH was sent containing the rescue_video_url field
    const sentRescue = patchBodies.some((b) => b.includes("rescue_video_url"));
    expect(sentRescue).toBe(true);

    // The sent body should include our test URL
    const sentUrl = patchBodies.some((b) => b.includes("rescue-test.mp4"));
    expect(sentUrl).toBe(true);
  });

  test("Config tab no longer shows Events section", async ({ page }) => {
    await page.goto("/settings");
    await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

    // Click Config tab
    await page.locator(".settings-tabs button:has-text('Config')").click();
    await page.waitForTimeout(500);

    // Verify no h3 with text "Events" appears in the Config tab content
    // (the OLD EventsSection had <h3>"Events"</h3>)
    const eventsHeader = page.locator(".settings-section h3:text-is('Events')");
    await expect(eventsHeader).toHaveCount(0);
  });
});

test.describe("Upload telemetry UI", () => {
  test("dashboard shows upload strip with live values", async ({ page }) => {
    await page.goto("/");
    const strip = page.locator(".upload-strip");
    await expect(strip).toBeVisible({ timeout: 10_000 });
    await expect(strip.locator(".upload-strip__rate")).toContainText("c/s");
    await expect(strip.locator(".upload-strip__median")).toContainText("ms");
    await expect(strip.locator(".upload-strip__inflight")).toContainText("in-flight");
    await expect(strip.locator(".upload-strip__errors")).toContainText("errors");
  });

  test("clicking strip navigates to /uploads page", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".upload-strip")).toBeVisible({ timeout: 10_000 });
    await page.locator(".upload-strip").click();
    await expect(page).toHaveURL(/\/uploads$/);
    await expect(page.locator(".uploads-table")).toBeVisible();
  });

  test("/uploads page lists chunks with status classes", async ({ page }) => {
    await page.goto("/uploads");
    await expect(page.locator(".uploads-table")).toBeVisible({ timeout: 10_000 });
    // At least one sent row and one retrying row per mock fixture
    await expect(page.locator(".uploads-row--sent").first()).toBeVisible();
    await expect(page.locator(".uploads-row--retrying").first()).toBeVisible();
  });

  test("errors-only filter hides sent rows", async ({ page }) => {
    await page.goto("/uploads");
    await expect(page.locator(".uploads-table")).toBeVisible({ timeout: 10_000 });
    const checkbox = page.locator('.uploads-filter input[type="checkbox"]');
    await checkbox.check();
    // Sent rows should no longer be visible
    await expect(page.locator(".uploads-row--sent")).toHaveCount(0);
    // Retrying row still visible (it has last_error)
    await expect(page.locator(".uploads-row--retrying").first()).toBeVisible();
    await checkbox.uncheck();
    // Sent rows come back
    await expect(page.locator(".uploads-row--sent").first()).toBeVisible();
  });
});
