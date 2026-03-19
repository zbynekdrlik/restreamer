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
    expect(labels).toContain("Chunker");
    expect(labels).toContain("S3 Upload");
    expect(labels).toContain("VPS");
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

  test("pipeline shows Connected after InpointStatus WebSocket event", async ({
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

    await expect(page.locator(".pipeline-metric").nth(0)).toHaveText(
      "Connected",
      { timeout: 5000 },
    );
    await expect(page.locator(".pipeline-metric").nth(1)).toHaveText(
      "Receiving",
    );
    await expect(page.locator(".pipeline-flow .status-dot").nth(0)).toHaveClass(
      /active/,
    );
  });

  test("pipeline reverts to Disconnected after rtmp_connected=false", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.locator(".pipeline-flow")).toBeVisible({
      timeout: 10000,
    });

    // Connect
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
      "Connected",
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
    await expect(page.locator("h2")).toHaveText("Settings");
  });

  test("settings shows back arrow to dashboard", async ({ page }) => {
    await page.goto("/settings");
    const backLink = page.locator('.header-nav-btn:has-text("Dashboard")');
    await expect(backLink).toBeVisible({ timeout: 10000 });
  });

  test("events section shows event list", async ({ page }) => {
    await page.goto("/settings");
    await page.waitForTimeout(1000);
    const cards = page
      .locator(".settings-section")
      .first()
      .locator(".settings-card");
    await expect(cards.first()).toBeVisible({ timeout: 10000 });
  });

  test("endpoints section renders with create form", async ({ page }) => {
    await page.goto("/settings");
    await expect(
      page.locator('.settings-section:has(h3:text("Endpoints"))'),
    ).toBeVisible({ timeout: 10000 });
    await expect(
      page.locator('.settings-section:has(h3:text("Endpoints")) .create-form'),
    ).toBeVisible();
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
    const section = page.locator('.settings-section:has(h3:text("Endpoints"))');
    const cards = section.locator(".settings-card");
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
