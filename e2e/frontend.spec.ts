import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// Inject Tauri mock before each page navigation
const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

test.beforeEach(async ({ page }) => {
  await page.addInitScript(tauriMockScript);
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

  test("pipeline flow nodes render", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline-flow")).toBeVisible({
      timeout: 10000,
    });
    // Check pipeline labels
    await expect(page.locator(".pipeline-label").first()).toBeVisible();
    const labels = await page.locator(".pipeline-label").allTextContents();
    expect(labels).toContain("OBS");
    expect(labels).toContain("RTMP");
    expect(labels).toContain("Local Buffer");
    expect(labels).toContain("S3 Queue");
    expect(labels).toContain("Delivery");
  });

  test("pipeline nodes show status dots", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline-flow")).toBeVisible({
      timeout: 10000,
    });
    const dots = page.locator(".pipeline-flow .status-dot");
    await expect(dots).toHaveCount(5);
  });

  test("pipeline arrows render between nodes", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline-flow")).toBeVisible({
      timeout: 10000,
    });
    const arrows = page.locator(".pipeline-arrow");
    await expect(arrows).toHaveCount(4);
  });

  test("activity feed section renders", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".activity-feed")).toBeVisible({
      timeout: 10000,
    });
    await expect(page.locator(".section-title")).toHaveText("Activity Feed");
  });

  test("activity feed shows empty state initially", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".activity-feed")).toBeVisible({
      timeout: 10000,
    });
    await expect(
      page.locator('.activity-feed .empty-state:has-text("No activity yet")'),
    ).toBeVisible();
  });

  test("endpoint cards appear with alias text after delivery WebSocket", async ({
    page,
  }) => {
    await page.goto("/");
    // Wait for WebSocket delivery event to arrive
    await page.waitForTimeout(2000);
    // Should show endpoint cards with actual alias text
    const endpointCards = page.locator(".endpoint-card");
    await expect(endpointCards.first()).toBeVisible({ timeout: 10000 });
    const cardTexts = await endpointCards.allTextContents();
    const allText = cardTexts.join(" ");
    expect(allText).toContain("YouTube Main");
    expect(allText).toContain("Facebook Page");
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

  test("activity feed populates after start stream", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);
    await page.locator(".event-selector").selectOption({ index: 1 });
    await page.locator(".start-btn").click();
    // Wait for ActivityFeed WebSocket event
    await page.waitForTimeout(1000);
    // Activity feed should no longer show empty state
    const feedItems = page.locator(".activity-feed .activity-entry");
    await expect(feedItems.first()).toBeVisible({ timeout: 5000 });
    const feedText = await feedItems.first().textContent();
    expect(feedText).toContain("Stream started");
  });

  test("pipeline shows OBS Disconnected and RTMP Idle by default", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline-flow")).toBeVisible({
      timeout: 10000,
    });
    const metrics = page.locator(".pipeline-metric");
    await expect(metrics.nth(0)).toHaveText("Disconnected");
    await expect(metrics.nth(1)).toHaveText("Idle");
  });

  test("OBS and RTMP status dots are not active by default", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline-flow")).toBeVisible({
      timeout: 10000,
    });
    const dots = page.locator(".pipeline-flow .status-dot");
    await expect(dots.nth(0)).not.toHaveClass(/active/);
    await expect(dots.nth(1)).not.toHaveClass(/active/);
  });

  test("pipeline shows RTMP Only after InpointStatus WebSocket event", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline-metric").nth(0)).toHaveText(
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
    await expect(page.locator(".pipeline-metric").nth(0)).toHaveText(
      "RTMP Only",
      { timeout: 5000 },
    );
    await expect(page.locator(".pipeline-metric").nth(1)).toHaveText(
      "Receiving",
    );
    await expect(page.locator(".pipeline-flow .status-dot").nth(0)).toHaveClass(
      /warning/,
    );
  });

  test("pipeline reverts to Disconnected after rtmp_connected=false", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline-flow")).toBeVisible({
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
    await expect(page.locator(".pipeline-metric").nth(0)).toHaveText(
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
    await expect(page.locator(".pipeline-metric").nth(0)).toHaveText(
      "Disconnected",
      { timeout: 5000 },
    );
    await expect(page.locator(".pipeline-metric").nth(1)).toHaveText("Idle");
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

    // Cache bar should be visible with predicted class
    const fill = page.locator(".cache-bar-fill");
    await expect(fill).toHaveClass(/predicted/, { timeout: 5000 });
    // Label should contain "predicted"
    await expect(page.locator(".cache-bar-label")).toContainText("predicted");
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

    // Cache bar should be visible with exhausted class
    const fill = page.locator(".cache-bar-fill");
    await expect(fill).toHaveClass(/exhausted/, { timeout: 5000 });
    // Label should mention "Buffer Exhausted"
    await expect(page.locator(".cache-bar-label")).toContainText(
      "Buffer Exhausted",
    );
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
    // Label should show normal format
    await expect(page.locator(".cache-bar-label")).toContainText(
      "Cache: 96s / 120s target (healthy)",
    );
  });
});

// --- Pending Endpoint State ---

test.describe("Pending Endpoint State", () => {
  test("pending endpoints show Initializing... with pending CSS class", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast DeliveryStatus with placeholder endpoints (alive=false, chunks=0, delay=0)
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

    // Endpoint cards should appear with pending class
    const pendingCards = page.locator(".endpoint-card.pending");
    await expect(pendingCards).toHaveCount(2, { timeout: 5000 });
    // Should show "Initializing..." text
    const cardTexts = await pendingCards.allTextContents();
    const allText = cardTexts.join(" ");
    expect(allText).toContain("Initializing...");
    // Status indicator should have pending class
    await expect(
      page.locator(".status-indicator.pending").first(),
    ).toBeVisible();
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
    await expect(page.locator(".endpoint-card.pending")).toHaveCount(1, {
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

    // Pending class should be gone
    await expect(page.locator(".endpoint-card.pending")).toHaveCount(0, {
      timeout: 5000,
    });
    // Should show Alive status
    await expect(page.locator(".status-indicator.alive")).toBeVisible();
    // Should show real metrics
    await expect(page.locator(".endpoint-card.delivery")).toContainText(
      "45s delay",
    );
  });

  test("pending endpoints show placeholder metrics", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

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

    const card = page.locator(".endpoint-card.pending").first();
    await expect(card).toBeVisible({ timeout: 5000 });
    // Pending card should show dash placeholders instead of "0s delay" / "0 chunks"
    const text = await card.textContent();
    expect(text).toContain("—");
    expect(text).not.toContain("0s delay");
    expect(text).not.toContain("0 chunks");
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
    await expect(page.locator(".cache-bar-label")).toContainText("healthy");
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
    await expect(page.locator(".cache-bar-label")).toContainText("building");
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
    await expect(page.locator(".cache-bar-label")).toContainText("low");
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
  test("pipeline shows local buffer and S3 queue counts when delivering", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Broadcast PipelineState with chunk pipeline data
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

    // Also broadcast delivery status so delivered count is available
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

    // Pipeline nodes should show the chunk pipeline data
    const metrics = page.locator(".pipeline-metric");
    await expect(metrics.nth(2)).toHaveText("5 chunks", { timeout: 5000 });
    await expect(metrics.nth(3)).toHaveText("42 queued");
    await expect(metrics.nth(4)).toContainText("1203 delivered");
  });

  test("pipeline shows chunk stats when idle (not delivering)", async ({
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

    // Local Buffer should show pending_chunks from chunk_stats when idle
    const metrics = page.locator(".pipeline-metric");
    await expect(metrics.nth(2)).toHaveText("8 chunks", { timeout: 5000 });
  });

  test("local buffer dot is gray when not streaming", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline-flow")).toBeVisible({
      timeout: 10000,
    });
    const bufferDot = page.locator(".pipeline-flow .status-dot").nth(2);
    await expect(bufferDot).not.toHaveClass(/active/);
    await expect(bufferDot).not.toHaveClass(/warning/);
    await expect(bufferDot).not.toHaveClass(/error/);
  });

  test("local buffer dot uses chunk count: green at 0 chunks even with low buffer_progress", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // RTMP connected so local buffer dot is not gray
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "InpointStatus",
        data: {
          state: "receiving",
          rtmp_connected: true,
          received_bytes: 1024,
          chunk_count: 0,
        },
      },
    });

    // Streaming state with LOW buffer_progress but 0 local_buffer_chunks
    // Local Buffer dot should be GREEN (chunk count 0) even though bar is critical
    await page.request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
      data: {
        type: "PipelineState",
        data: {
          state: "streaming",
          event_id: 1,
          event_name: "Test",
          buffer_progress: 0.2,
          target_delay_secs: 120,
          current_delay_secs: 24.0,
          session_start: null,
          predicted: false,
          local_buffer_chunks: 0,
          s3_queue_chunks: 53,
        },
      },
    });

    const bufferDot = page.locator(".pipeline-flow .status-dot").nth(2);
    await expect(bufferDot).toHaveClass(/active/, { timeout: 5000 });
    await expect(bufferDot).not.toHaveClass(/warning/);
    await expect(bufferDot).not.toHaveClass(/error/);
  });

  test("local buffer dot uses chunk count: yellow at 3 chunks even with healthy bar", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

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

    // HIGH buffer_progress (bar is green) but 3 local_buffer_chunks
    // Local Buffer dot should be YELLOW (chunk count 3) even though bar is healthy
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
          local_buffer_chunks: 3,
          s3_queue_chunks: 10,
        },
      },
    });

    const bufferDot = page.locator(".pipeline-flow .status-dot").nth(2);
    await expect(bufferDot).toHaveClass(/warning/, { timeout: 5000 });
    await expect(bufferDot).not.toHaveClass(/active/);
  });

  test("s3 queue dot matches cache bar: green when bar healthy, regardless of chunk count", async ({
    page,
  }) => {
    await page.goto("/");
    await page.waitForTimeout(1000);

    // Healthy buffer_progress with 53 s3_queue_chunks
    // S3 Queue dot should be GREEN (matches bar) even though chunk count is high
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

    const s3Dot = page.locator(".pipeline-flow .status-dot").nth(3);
    await expect(s3Dot).toHaveClass(/active/, { timeout: 5000 });
    await expect(s3Dot).not.toHaveClass(/warning/);
    await expect(s3Dot).not.toHaveClass(/error/);
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

test.describe("Activity Feed Timezone", () => {
  test("activity feed shows local timezone time, not UTC", async ({ page }) => {
    // Browser timezone is set to America/New_York (UTC-4 or UTC-5)
    await page.goto("/");
    await page.waitForTimeout(1000);
    await page.locator(".event-selector").selectOption({ index: 1 });

    // Send an ActivityFeed event with a known UTC timestamp
    const utcTimestamp = "2026-06-15T18:30:45.000Z";
    // In America/New_York (EDT, UTC-4), this should display as 14:30:45
    await page.evaluate(async (ts) => {
      await fetch("/api/v1/_test/ws-broadcast", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          type: "ActivityFeed",
          data: {
            timestamp: ts,
            severity: "info",
            message: "Timezone test event",
            source: "test",
          },
        }),
      });
    }, utcTimestamp);

    await page.waitForTimeout(1000);
    const feedEntry = page.locator(
      '.activity-entry:has-text("Timezone test event")',
    );
    await expect(feedEntry).toBeVisible({ timeout: 5000 });

    const timeText = await feedEntry.locator(".activity-time").textContent();
    // Should show local time (14:30:45 EDT), NOT UTC (18:30:45)
    expect(timeText).toBe("14:30:45");
    expect(timeText).not.toBe("18:30:45");
  });
});

test.describe("YouTube Health Badge", () => {
  test("YouTube endpoint card shows health badge after polling", async ({
    page,
  }) => {
    await page.goto("/");
    // Wait for WebSocket delivery status (includes "YouTube Main" endpoint)
    // and for the initial YouTube health poll to fire (5s interval detects endpoints, then fetches)
    const ytCard = page.locator(
      '.endpoint-card:has(.endpoint-alias:has-text("YouTube Main"))',
    );
    await expect(ytCard).toBeVisible({ timeout: 10000 });
    const badge = ytCard.locator(".yt-health-badge");
    // Badge renders immediately as "unknown", then updates after poll fetches YouTube status
    // The 5s interval fires, detects endpoints, fetches /youtube/status, updates store
    await expect(badge).toHaveClass(/good/, { timeout: 15000 });
    await expect(badge).toHaveText("good");
  });

  test("non-YouTube endpoint does not show health badge", async ({ page }) => {
    await page.goto("/");
    await page.waitForTimeout(2000);

    const fbCard = page.locator(
      '.endpoint-card:has(.endpoint-alias:has-text("Facebook Page"))',
    );
    await expect(fbCard).toBeVisible({ timeout: 5000 });
    const badge = fbCard.locator(".yt-health-badge");
    await expect(badge).toHaveCount(0);
  });
});
