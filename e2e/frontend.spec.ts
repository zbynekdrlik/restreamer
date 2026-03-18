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

  test("endpoint cards appear after delivery status WebSocket", async ({
    page,
  }) => {
    await page.goto("/");
    // Wait for WebSocket delivery event to arrive
    await page.waitForTimeout(2000);
    // Should show endpoint cards
    const endpointCards = page.locator(".endpoint-card");
    await expect(endpointCards.first()).toBeVisible({ timeout: 10000 });
  });

  test("state badge shows idle by default", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".state-badge")).toBeVisible({ timeout: 10000 });
    await expect(page.locator(".state-badge")).toContainText("Idle");
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

  test("can create a new event", async ({ page }) => {
    await page.goto("/settings");
    await page.waitForTimeout(1000);
    const section = page.locator('.settings-section:has(h3:text("Events"))');
    await section.locator('input[placeholder="Event name"]').fill("Test Event");
    await section.locator('button:has-text("Create Event")').click();
    await page.waitForTimeout(500);
    // Should now show the new event in the list
    await expect(section.locator(".settings-card")).toHaveCount(3);
  });

  test("endpoint list shows existing endpoints", async ({ page }) => {
    await page.goto("/settings");
    await page.waitForTimeout(1000);
    const section = page.locator('.settings-section:has(h3:text("Endpoints"))');
    const cards = section.locator(".settings-card");
    await expect(cards).toHaveCount(2);
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
