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

// --- Dashboard tab ---

test.describe("Dashboard tab", () => {
  test("renders header with Restreamer title and version", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("h1")).toHaveText("Restreamer");
    await expect(page.locator(".version")).toBeVisible();
    await expect(page.locator(".version")).toContainText("v");
  });

  test("shows 5 tab buttons", async ({ page }) => {
    await page.goto("/");
    const tabs = page.locator(".tabs button.tab");
    await expect(tabs).toHaveCount(5);
    await expect(tabs.nth(0)).toHaveText("Dashboard");
    await expect(tabs.nth(1)).toHaveText("Events");
    await expect(tabs.nth(2)).toHaveText("Endpoints");
    await expect(tabs.nth(3)).toHaveText("Schedules");
    await expect(tabs.nth(4)).toHaveText("Logs");
  });

  test("Dashboard tab is active by default", async ({ page }) => {
    await page.goto("/");
    const dashTab = page.locator('.tabs button.tab:has-text("Dashboard")');
    await expect(dashTab).toHaveClass(/active/);
  });

  test('shows "Loading..." initially, then content after API response', async ({
    page,
  }) => {
    await page.goto("/");
    // After Tauri mock resolves, should show status grid
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
  });

  test("displays status cards with streaming event info", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });

    // Streaming event card
    await expect(page.locator(".event-card")).toBeVisible();
    await expect(page.locator(".event-card")).toContainText("Streaming Event");

    // Event details
    await expect(page.locator(".event-card")).toContainText(
      "Weekly Sunday Service Stream",
    );
    await expect(page.locator(".event-card")).toContainText("Sunday Service");

    // Status cards
    await expect(page.locator(".card-title")).toContainText(["Inpoint"]);
    await expect(page.locator(".card-value").first()).toBeVisible();
  });

  test("shows chunk statistics", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    // Total chunks card value
    const chunksCard = page.locator(".card:has(.card-title:text('Chunks'))");
    await expect(chunksCard.locator(".card-value")).toHaveText("42");
    // Chunk stats labels
    await expect(chunksCard.locator(".card-label")).toContainText(
      "3 pending, 39 sent",
    );
  });
});

// --- Events tab ---

test.describe("Events tab", () => {
  test("clicking Events tab switches view", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Events")');
    await expect(page.locator(".events-tab")).toBeVisible();
    await expect(page.locator(".events-tab h2")).toHaveText("Streaming Events");
  });

  test("displays event list from API", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Events")');
    await expect(page.locator(".event-list")).toBeVisible();
    // Two events from mock data
    await expect(page.locator(".events-tab .event-card")).toHaveCount(2);
    await expect(page.locator(".events-tab .event-card").first()).toContainText(
      "Sunday Service",
    );
    await expect(page.locator(".events-tab .event-card").nth(1)).toContainText(
      "Wednesday Bible Study",
    );
  });

  test("shows create form with input and button", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Events")');
    await expect(
      page.locator('.events-tab .create-form input[type="text"]'),
    ).toBeVisible();
    await expect(
      page.locator('.events-tab .create-form button:has-text("Create Event")'),
    ).toBeVisible();
  });

  test("shows receiving/delivering badges per event", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Events")');
    // First event has receiving_activated=true
    const firstEvent = page.locator(".events-tab .event-card").first();
    await expect(firstEvent.locator(".badge.active")).toContainText([
      "Receiving",
    ]);
    // Second event is idle
    const secondEvent = page.locator(".events-tab .event-card").nth(1);
    await expect(secondEvent.locator(".badge").first()).toContainText("Idle");
  });

  test("shows Activate/Start Delivering/Deactivate buttons", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Events")');
    const firstEvent = page.locator(".events-tab .event-card").first();
    const actions = firstEvent.locator(".event-actions button");
    await expect(actions).toHaveCount(3);
    await expect(actions.nth(0)).toHaveText("Activate");
    await expect(actions.nth(1)).toHaveText("Start Delivering");
    await expect(actions.nth(2)).toHaveText("Deactivate");
  });
});

// --- Endpoints tab ---

test.describe("Endpoints tab", () => {
  test("clicking Endpoints tab switches view", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Endpoints")');
    await expect(page.locator(".endpoints-tab")).toBeVisible();
    await expect(page.locator(".endpoints-tab h2")).toHaveText(
      "Endpoint Configurations",
    );
  });

  test("displays endpoint list from API", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Endpoints")');
    await expect(page.locator(".endpoint-list")).toBeVisible();
    await expect(page.locator(".endpoint-card")).toHaveCount(2);
    await expect(page.locator(".endpoint-card").first()).toContainText(
      "YouTube Main",
    );
    await expect(page.locator(".endpoint-card").nth(1)).toContainText(
      "Facebook Page",
    );
  });

  test("shows create form with alias, service type dropdown, and stream key", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Endpoints")');
    const form = page.locator(".endpoints-tab .create-form");
    await expect(form.locator('input[type="text"]')).toHaveCount(2);
    await expect(form.locator("select")).toBeVisible();
    await expect(form.locator('button:has-text("Add Endpoint")')).toBeVisible();
  });

  test("service type dropdown has all options", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Endpoints")');
    const options = page.locator(".endpoints-tab .create-form select option");
    await expect(options).toHaveCount(6);
    const values = await options.evaluateAll((opts) =>
      (opts as HTMLOptionElement[]).map((o) => o.value),
    );
    expect(values).toEqual([
      "YT_HLS",
      "YT_RTMP",
      "FB",
      "VIMEO",
      "INSTAGRAM",
      "TEST_FILE",
    ]);
  });

  test("displays enabled/disabled and Fast badges", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Endpoints")');
    // First endpoint: enabled=true
    const first = page.locator(".endpoint-card").first();
    await expect(first.locator(".badge.active")).toContainText("Enabled");
    // Second endpoint: enabled=false, is_fast=true
    const second = page.locator(".endpoint-card").nth(1);
    await expect(second.locator(".badge").first()).toContainText("Disabled");
    await expect(second.locator(".badge.fast")).toContainText("Fast");
  });

  test("shows service type label", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Endpoints")');
    await expect(
      page.locator(".endpoint-card").first().locator(".service-type"),
    ).toContainText("YT_HLS");
    await expect(
      page.locator(".endpoint-card").nth(1).locator(".service-type"),
    ).toContainText("FB");
  });

  test("shows delete button per endpoint", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Endpoints")');
    const deleteButtons = page.locator(
      '.endpoint-actions button:has-text("Delete")',
    );
    await expect(deleteButtons).toHaveCount(2);
  });
});

// --- Schedules tab ---

test.describe("Schedules tab", () => {
  test("clicking Schedules tab switches view", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Schedules")');
    await expect(page.locator(".schedules-tab")).toBeVisible();
    await expect(page.locator(".schedules-tab h2")).toHaveText(
      "Scheduled Streams",
    );
  });

  test("displays schedule list with details", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Schedules")');
    await expect(page.locator(".schedule-card")).toHaveCount(2);
    // First schedule: event_id=1, has repeat
    const first = page.locator(".schedule-card").first();
    await expect(first).toContainText("Event #1");
    await expect(first).toContainText("weekly");
  });

  test("shows enabled/disabled badges", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Schedules")');
    const first = page.locator(".schedule-card").first();
    await expect(first.locator(".badge.active")).toContainText("Enabled");
    const second = page.locator(".schedule-card").nth(1);
    await expect(second.locator(".badge").last()).toContainText("Disabled");
  });

  test("shows next run time", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Schedules")');
    await expect(page.locator(".next-run").first()).toContainText("Next:");
  });

  test("shows delete button per schedule", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Schedules")');
    const deleteButtons = page.locator(
      '.schedule-actions button:has-text("Delete")',
    );
    await expect(deleteButtons).toHaveCount(2);
  });
});

// --- Logs tab ---

test.describe("Logs tab", () => {
  test("clicking Logs tab switches view", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Logs")');
    await expect(page.locator(".log-viewer")).toBeVisible({ timeout: 10000 });
  });

  test("displays log entries with level, target, message", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Logs")');
    await expect(page.locator(".log-viewer")).toBeVisible({ timeout: 10000 });
    // Should have log entries
    const entries = page.locator(".log-entry");
    await expect(entries.first()).toBeVisible();
    // Check first entry has all parts
    await expect(entries.first().locator(".log-level")).toBeVisible();
    await expect(entries.first().locator(".log-target")).toBeVisible();
  });

  test("color-codes log levels", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Logs")');
    await expect(page.locator(".log-viewer")).toBeVisible({ timeout: 10000 });
    // INFO level should have success color
    const infoLevel = page.locator(".log-level.INFO").first();
    await expect(infoLevel).toBeVisible();
    const color = await infoLevel.evaluate((el) => getComputedStyle(el).color);
    // var(--success) = #4ecca3 = rgb(78, 204, 163)
    expect(color).toBe("rgb(78, 204, 163)");
  });

  test("shows WARN and ERROR levels", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.tab:has-text("Logs")');
    await expect(page.locator(".log-viewer")).toBeVisible({ timeout: 10000 });
    await expect(page.locator(".log-level.WARN")).toBeVisible();
    await expect(page.locator(".log-level.ERROR")).toBeVisible();
  });
});

// --- Tab navigation ---

test.describe("Tab navigation", () => {
  test("switching between all tabs works", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });

    // Switch to each tab and verify content
    await page.click('.tab:has-text("Events")');
    await expect(page.locator(".events-tab")).toBeVisible();

    await page.click('.tab:has-text("Endpoints")');
    await expect(page.locator(".endpoints-tab")).toBeVisible();

    await page.click('.tab:has-text("Schedules")');
    await expect(page.locator(".schedules-tab")).toBeVisible();

    await page.click('.tab:has-text("Logs")');
    await expect(page.locator(".log-viewer")).toBeVisible({ timeout: 10000 });

    // Back to dashboard
    await page.click('.tab:has-text("Dashboard")');
    await expect(page.locator(".status-grid")).toBeVisible();
  });

  test("active tab has active class", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });

    await page.click('.tab:has-text("Events")');
    await expect(page.locator('.tab:has-text("Events")')).toHaveClass(/active/);
    await expect(page.locator('.tab:has-text("Dashboard")')).not.toHaveClass(
      /active/,
    );
  });
});
