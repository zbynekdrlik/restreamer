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

test.beforeEach(async ({ page }) => {
  consoleMessages = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });
  await page.addInitScript(tauriMockScript);
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
    const dots = page.locator(".pipeline .status-dot");
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
    // Now stop
    await page.locator(".stop-btn").click();
    await expect(page.locator(".state-badge")).toContainText("Idle", {
      timeout: 5000,
    });
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
    const dots = page.locator(".pipeline .status-dot");
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
    await expect(page.locator(".pipeline-node-metric").nth(1)).toHaveText(
      "Receiving",
    );
    await expect(page.locator(".pipeline .status-dot").nth(0)).toHaveClass(
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

  test("events section shows event list", async ({ page }) => {
    await page.goto("/settings");
    await page.waitForTimeout(1000);
    // Events section is the second .settings-section (OBS section is first)
    const cards = page
      .locator(".settings-section")
      .nth(1)
      .locator(".settings-card");
    await expect(cards.first()).toBeVisible({ timeout: 10000 });
  });

  test("endpoints section renders with create form", async ({ page }) => {
    await page.goto("/settings");
    await expect(page.locator(".endpoints-tab")).toBeVisible({
      timeout: 10000,
    });
    await expect(page.locator(".endpoints-tab .create-form")).toBeVisible();
  });

  test("event create form exists", async ({ page }) => {
    await page.goto("/settings");
    await expect(
      page.locator('.settings-section:has(h3:text("Events")) .create-form'),
    ).toBeVisible({ timeout: 10000 });
    await expect(
      page.locator(
        '.settings-section:has(h3:text("Events")) input[placeholder="Event name"]',
      ),
    ).toBeVisible();
  });

  test("can create a new event and it appears with correct name", async ({
    page,
  }) => {
    await page.goto("/settings");
    await page.waitForTimeout(1000);
    const section = page.locator('.settings-section:has(h3:text("Events"))');
    await section.locator('input[placeholder="Event name"]').fill("Test Event");
    await section.locator('button:has-text("Create Event")').click();
    await page.waitForTimeout(500);
    // Should now show the new event in the list
    await expect(section.locator(".settings-card")).toHaveCount(3);
    // Verify the new event name appears
    const cardTexts = await section.locator(".settings-card").allTextContents();
    expect(cardTexts.join(" ")).toContain("Test Event");
  });

  test("event card shows cache delay editor", async ({ page }) => {
    await page.goto("/settings");
    await page.waitForTimeout(1000);
    const section = page.locator('.settings-section:has(h3:text("Events"))');
    // Should have cache delay input
    const cacheInput = section.locator(".cache-delay-input").first();
    await expect(cacheInput).toBeVisible({ timeout: 5000 });
  });

  test("cache delay save calls PATCH API", async ({ page }) => {
    await page.goto("/settings");
    await page.waitForTimeout(1000);
    const section = page.locator('.settings-section:has(h3:text("Events"))');
    const cacheInput = section.locator(".cache-delay-input").first();
    await cacheInput.fill("300");

    // Intercept the PATCH call
    const [request] = await Promise.all([
      page.waitForRequest(
        (req) => req.url().includes("/events/") && req.method() === "PATCH",
      ),
      section.locator(".btn-small").first().click(),
    ]);
    const body = request.postDataJSON();
    expect(body.cache_delay_secs).toBe(300);
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

// --- Predictive Buffer State ---

test.describe("Predictive Buffer State", () => {
  test("cache bar shows predicted state with warning style", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast predicted PipelineState
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "buffering",
          event_id: 1,
          event_name: "Sunday Service",
          buffer_progress: 0.5,
          target_delay_secs: 120,
          current_delay_secs: 60.0,
          session_start: null,
          predicted: true,
        },
      },
    });

    // Cache bar fill should have predicted class
    const fill = page.locator(".cache-bar-fill");
    await expect(fill).toHaveClass(/predicted/, { timeout: 5000 });
    // Control bar cache display should show predicted prefix (~)
    await expect(page.locator(".cache-display")).toContainText("~60s / 120s");
  });

  test("cache bar shows buffer exhausted state", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast buffer_exhausted PipelineState
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "buffer_exhausted",
          event_id: 1,
          event_name: "Sunday Service",
          buffer_progress: 0.0,
          target_delay_secs: 120,
          current_delay_secs: 0.0,
          session_start: null,
          predicted: true,
        },
      },
    });

    // Cache bar fill should have exhausted class
    const fill = page.locator(".cache-bar-fill");
    await expect(fill).toHaveClass(/exhausted/, { timeout: 5000 });
    // State badge should show "Exhausted"
    await expect(page.locator(".state-badge")).toContainText("Exhausted");
  });

  test("transitions from predicted back to live data", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Start with predicted state
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "buffering",
          event_id: 1,
          event_name: "Sunday Service",
          buffer_progress: 0.3,
          target_delay_secs: 120,
          current_delay_secs: 36.0,
          session_start: null,
          predicted: true,
        },
      },
    });
    await expect(page.locator(".cache-bar-fill")).toHaveClass(/predicted/, {
      timeout: 5000,
    });

    // Transition back to live
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "streaming",
          event_id: 1,
          event_name: "Sunday Service",
          buffer_progress: 0.8,
          target_delay_secs: 120,
          current_delay_secs: 96.0,
          session_start: null,
          predicted: false,
        },
      },
    });

    // Should no longer have predicted class
    await expect(page.locator(".cache-bar-fill")).not.toHaveClass(/predicted/, {
      timeout: 5000,
    });
    await expect(page.locator(".cache-bar-fill")).not.toHaveClass(/exhausted/);
    // Cache display should show normal (no ~ prefix)
    await expect(page.locator(".cache-display")).toContainText("96s / 120s");
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
  test("add endpoint dropdown appears when delivery is running", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast running delivery status
    await page.request.post(
      "http://127.0.0.1:8910/api/v1/_test/ws-broadcast",
      {
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
      },
    );

    // Add endpoint dropdown should be visible
    await expect(page.locator(".add-endpoint-select")).toBeVisible({
      timeout: 5000,
    });
    await expect(page.locator(".start-position-select")).toBeVisible();
  });

  test("remove button appears on endpoint nodes when delivering", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast running delivery status
    await page.request.post(
      "http://127.0.0.1:8910/api/v1/_test/ws-broadcast",
      {
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
      },
    );

    // Remove button (×) should appear on the endpoint node
    const removeBtn = page.locator(".btn-remove-endpoint");
    await expect(removeBtn).toBeVisible({ timeout: 5000 });
  });

  test("add/remove controls hidden when delivery is idle", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast idle delivery status to clear any leftover state from previous tests
    await page.request.post(
      "http://127.0.0.1:8910/api/v1/_test/ws-broadcast",
      {
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
      },
    );
    await page.waitForTimeout(500);

    // Idle state — no add/remove controls
    await expect(page.locator(".add-endpoint-select")).not.toBeVisible();
    await expect(page.locator(".btn-remove-endpoint")).not.toBeVisible();
  });
});

// --- Cache Bar Health Colors ---

test.describe("Cache Bar Health Colors", () => {
  test("cache bar shows healthy class when progress >= 75%", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "streaming",
          event_id: 1,
          event_name: "Sunday Service",
          buffer_progress: 0.85,
          target_delay_secs: 120,
          current_delay_secs: 102.0,
          session_start: null,
          predicted: false,
          local_buffer_chunks: 2,
          s3_queue_chunks: 10,
        },
      },
    });

    const fill = page.locator(".cache-bar-fill");
    await expect(fill).toHaveClass(/healthy/, { timeout: 5000 });
  });

  test("cache bar shows warning class when progress 40-75%", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "buffering",
          event_id: 1,
          event_name: "Sunday Service",
          buffer_progress: 0.5,
          target_delay_secs: 120,
          current_delay_secs: 60.0,
          session_start: null,
          predicted: false,
          local_buffer_chunks: 5,
          s3_queue_chunks: 20,
        },
      },
    });

    const fill = page.locator(".cache-bar-fill");
    await expect(fill).toHaveClass(/warning/, { timeout: 5000 });
  });

  test("cache bar shows critical class when progress < 40%", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "buffering",
          event_id: 1,
          event_name: "Sunday Service",
          buffer_progress: 0.2,
          target_delay_secs: 120,
          current_delay_secs: 24.0,
          session_start: null,
          predicted: false,
          local_buffer_chunks: 8,
          s3_queue_chunks: 5,
        },
      },
    });

    const fill = page.locator(".cache-bar-fill");
    await expect(fill).toHaveClass(/critical/, { timeout: 5000 });
  });

  test("cache bar transitions from critical to healthy as buffer fills", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Start critical
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "buffering",
          event_id: 1,
          event_name: "Sunday Service",
          buffer_progress: 0.1,
          target_delay_secs: 120,
          current_delay_secs: 12.0,
          session_start: null,
          predicted: false,
          local_buffer_chunks: 10,
          s3_queue_chunks: 2,
        },
      },
    });
    await expect(page.locator(".cache-bar-fill")).toHaveClass(/critical/, {
      timeout: 5000,
    });

    // Transition to healthy
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "streaming",
          event_id: 1,
          event_name: "Sunday Service",
          buffer_progress: 0.9,
          target_delay_secs: 120,
          current_delay_secs: 108.0,
          session_start: null,
          predicted: false,
          local_buffer_chunks: 1,
          s3_queue_chunks: 15,
        },
      },
    });
    await expect(page.locator(".cache-bar-fill")).toHaveClass(/healthy/, {
      timeout: 5000,
    });
    await expect(page.locator(".cache-bar-fill")).not.toHaveClass(/critical/);
  });
});

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
          buffer_progress: 0.8,
          target_delay_secs: 120,
          current_delay_secs: 96.0,
          session_start: null,
          predicted: false,
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

    // Buffer node (index 2) shows delay metric
    const metrics = page.locator(".pipeline-node-metric");
    await expect(metrics.nth(2)).toContainText("96s / 120s", { timeout: 5000 });
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
    const bufferDot = page.locator(".pipeline .status-dot").nth(2);
    await expect(bufferDot).not.toHaveClass(/active/);
    await expect(bufferDot).not.toHaveClass(/warning/);
    await expect(bufferDot).not.toHaveClass(/error/);
  });

  test("buffer dot shows healthy when delivering with good progress", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Streaming state with good buffer_progress
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "streaming",
          event_id: 1,
          event_name: "Test",
          buffer_progress: 0.8,
          target_delay_secs: 120,
          current_delay_secs: 96.0,
          session_start: null,
          predicted: false,
          local_buffer_chunks: 0,
          s3_queue_chunks: 53,
        },
      },
    });

    // S3 dot is at index 3 (OBS=0, RTMP=1, Local Buffer=2, S3/VPS=3)
    const s3Dot = page.locator(".pipeline .status-dot").nth(3);
    await expect(s3Dot).toHaveClass(/active/, { timeout: 5000 });
  });

  test("S3 dot shows warning when progress 40-75%", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "buffering",
          event_id: 1,
          event_name: "Test",
          buffer_progress: 0.5,
          target_delay_secs: 120,
          current_delay_secs: 60.0,
          session_start: null,
          predicted: false,
          local_buffer_chunks: 3,
          s3_queue_chunks: 10,
        },
      },
    });

    const s3Dot = page.locator(".pipeline .status-dot").nth(3);
    await expect(s3Dot).toHaveClass(/warning/, { timeout: 5000 });
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

    const vpsDot = page.locator(".pipeline .status-dot").nth(3);
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
