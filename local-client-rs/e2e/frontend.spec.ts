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

// --- Dashboard route (/) ---

test.describe("Dashboard route", () => {
  test("renders header with Restreamer title and version", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("h1")).toHaveText("Restreamer");
    await expect(page.locator(".version")).toBeVisible();
    await expect(page.locator(".version")).toContainText("v");
  });

  test("shows 4 navigation links", async ({ page }) => {
    await page.goto("/");
    const links = page.locator(".nav-bar .nav-link");
    await expect(links).toHaveCount(4);
    await expect(links.nth(0)).toHaveText("Dashboard");
    await expect(links.nth(1)).toHaveText("Events");
    await expect(links.nth(2)).toHaveText("Endpoints");
    await expect(links.nth(3)).toHaveText("Logs");
  });

  test("Dashboard link is active by default", async ({ page }) => {
    await page.goto("/");
    const dashLink = page.locator('.nav-bar .nav-link:has-text("Dashboard")');
    await expect(dashLink).toHaveAttribute("aria-current", "page");
  });

  test("shows WebSocket connection status", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".ws-status")).toBeVisible();
    await expect(page.locator(".ws-status .status-indicator")).toBeVisible();
  });

  test("shows status grid after WebSocket/API data loads", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
  });

  test("displays status cards with streaming event info", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });

    // Streaming event card
    await expect(page.locator(".event-card")).toBeVisible();
    await expect(page.locator(".event-card")).toContainText("Streaming Event");

    // Event name shown
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

// --- Events route (/events) ---

test.describe("Events route", () => {
  test("navigating to Events shows events view", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.nav-link:has-text("Events")');
    await expect(page.locator(".events-tab")).toBeVisible();
    await expect(page.locator(".events-tab h2")).toHaveText("Streaming Events");
  });

  test("direct navigation to /events works", async ({ page }) => {
    await page.goto("/events");
    await expect(page.locator(".events-tab")).toBeVisible({ timeout: 10000 });
    await expect(page.locator(".events-tab h2")).toHaveText("Streaming Events");
  });

  test("displays event list from API", async ({ page }) => {
    await page.goto("/events");
    await expect(page.locator(".event-list")).toBeVisible({ timeout: 10000 });
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
    await page.goto("/events");
    await expect(page.locator(".events-tab")).toBeVisible({ timeout: 10000 });
    await expect(
      page.locator('.events-tab .create-form input[type="text"]'),
    ).toBeVisible();
    await expect(
      page.locator('.events-tab .create-form button:has-text("Create Event")'),
    ).toBeVisible();
  });

  test("shows receiving/delivering badges per event", async ({ page }) => {
    await page.goto("/events");
    await expect(page.locator(".event-list")).toBeVisible({ timeout: 10000 });
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
    await page.goto("/events");
    await expect(page.locator(".event-list")).toBeVisible({ timeout: 10000 });
    const firstEvent = page.locator(".events-tab .event-card").first();
    const actions = firstEvent.locator(".event-actions button");
    await expect(actions).toHaveCount(3);
    await expect(actions.nth(0)).toHaveText("Activate");
    await expect(actions.nth(1)).toHaveText("Start Delivering");
    await expect(actions.nth(2)).toHaveText("Deactivate");
  });

  test("shows assigned endpoints section", async ({ page }) => {
    await page.goto("/events");
    await expect(page.locator(".event-list")).toBeVisible({ timeout: 10000 });
    const firstEvent = page.locator(".events-tab .event-card").first();
    await expect(firstEvent.locator(".assigned-endpoints")).toBeVisible();
    await expect(firstEvent.locator(".assigned-label")).toHaveText(
      "Assigned Endpoints:",
    );
  });

  test("shows assigned endpoint with service badge", async ({ page }) => {
    await page.goto("/events");
    await expect(page.locator(".event-list")).toBeVisible({ timeout: 10000 });
    const firstEvent = page.locator(".events-tab .event-card").first();
    // First event has YouTube Main assigned in mock data
    await expect(firstEvent.locator(".assigned-ep")).toBeVisible({
      timeout: 5000,
    });
    await expect(firstEvent.locator(".assigned-ep")).toContainText(
      "YouTube Main",
    );
    await expect(firstEvent.locator(".service-badge")).toContainText("YT_HLS");
  });

  test("shows assign endpoint dropdown", async ({ page }) => {
    await page.goto("/events");
    await expect(page.locator(".event-list")).toBeVisible({ timeout: 10000 });
    const firstEvent = page.locator(".events-tab .event-card").first();
    await expect(firstEvent.locator(".assign-form select")).toBeVisible();
    await expect(
      firstEvent.locator('.assign-form button:has-text("Assign")'),
    ).toBeVisible();
  });

  test("second event shows None for assigned endpoints", async ({ page }) => {
    await page.goto("/events");
    await expect(page.locator(".event-list")).toBeVisible({ timeout: 10000 });
    const secondEvent = page.locator(".events-tab .event-card").nth(1);
    await expect(secondEvent.locator(".empty-inline")).toHaveText("None");
  });
});

// --- Endpoints route (/endpoints) ---

test.describe("Endpoints route", () => {
  test("navigating to Endpoints shows endpoints view", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.nav-link:has-text("Endpoints")');
    await expect(page.locator(".endpoints-tab")).toBeVisible();
    await expect(page.locator(".endpoints-tab h2")).toHaveText(
      "Endpoint Configurations",
    );
  });

  test("direct navigation to /endpoints works", async ({ page }) => {
    await page.goto("/endpoints");
    await expect(page.locator(".endpoints-tab")).toBeVisible({
      timeout: 10000,
    });
  });

  test("displays endpoint list from API", async ({ page }) => {
    await page.goto("/endpoints");
    await expect(page.locator(".endpoint-list")).toBeVisible({
      timeout: 10000,
    });
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
    await page.goto("/endpoints");
    await expect(page.locator(".endpoints-tab")).toBeVisible({
      timeout: 10000,
    });
    const form = page.locator(".endpoints-tab .create-form");
    await expect(form.locator('input[type="text"]')).toHaveCount(2);
    await expect(form.locator("select")).toBeVisible();
    await expect(form.locator('button:has-text("Add Endpoint")')).toBeVisible();
  });

  test("service type dropdown has all options", async ({ page }) => {
    await page.goto("/endpoints");
    await expect(page.locator(".endpoints-tab")).toBeVisible({
      timeout: 10000,
    });
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
    await page.goto("/endpoints");
    await expect(page.locator(".endpoint-list")).toBeVisible({
      timeout: 10000,
    });
    // First endpoint: enabled=true
    const first = page.locator(".endpoint-card").first();
    await expect(first.locator(".badge.active")).toContainText("Enabled");
    // Second endpoint: enabled=false, is_fast=true
    const second = page.locator(".endpoint-card").nth(1);
    await expect(second.locator(".badge").first()).toContainText("Disabled");
    await expect(second.locator(".badge.fast")).toContainText("Fast");
  });

  test("shows service type label", async ({ page }) => {
    await page.goto("/endpoints");
    await expect(page.locator(".endpoint-list")).toBeVisible({
      timeout: 10000,
    });
    await expect(
      page.locator(".endpoint-card").first().locator(".service-type"),
    ).toContainText("YT_HLS");
    await expect(
      page.locator(".endpoint-card").nth(1).locator(".service-type"),
    ).toContainText("FB");
  });

  test("shows delete button per endpoint", async ({ page }) => {
    await page.goto("/endpoints");
    await expect(page.locator(".endpoint-list")).toBeVisible({
      timeout: 10000,
    });
    const deleteButtons = page.locator(
      '.endpoint-actions button:has-text("Delete")',
    );
    await expect(deleteButtons).toHaveCount(2);
  });
});

// --- Logs route (/logs) ---

test.describe("Logs route", () => {
  test("navigating to Logs shows log viewer", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
    await page.click('.nav-link:has-text("Logs")');
    await expect(page.locator(".log-viewer")).toBeVisible({ timeout: 10000 });
  });

  test("direct navigation to /logs works", async ({ page }) => {
    await page.goto("/logs");
    await expect(page.locator(".log-viewer")).toBeVisible({ timeout: 10000 });
  });

  test("displays log entries with level, target, message", async ({ page }) => {
    await page.goto("/logs");
    await expect(page.locator(".log-viewer")).toBeVisible({ timeout: 10000 });
    // Should have log entries
    const entries = page.locator(".log-entry");
    await expect(entries.first()).toBeVisible();
    // Check first entry has all parts
    await expect(entries.first().locator(".log-level")).toBeVisible();
    await expect(entries.first().locator(".log-target")).toBeVisible();
  });

  test("color-codes log levels", async ({ page }) => {
    await page.goto("/logs");
    await expect(page.locator(".log-viewer")).toBeVisible({ timeout: 10000 });
    // INFO level should have success color
    const infoLevel = page.locator(".log-level.INFO").first();
    await expect(infoLevel).toBeVisible();
    const color = await infoLevel.evaluate((el) => getComputedStyle(el).color);
    // var(--success) = #4ecca3 = rgb(78, 204, 163)
    expect(color).toBe("rgb(78, 204, 163)");
  });

  test("shows WARN and ERROR levels", async ({ page }) => {
    await page.goto("/logs");
    await expect(page.locator(".log-viewer")).toBeVisible({ timeout: 10000 });
    await expect(page.locator(".log-level.WARN")).toBeVisible();
    await expect(page.locator(".log-level.ERROR")).toBeVisible();
  });
});

// --- Route navigation ---

test.describe("Route navigation", () => {
  test("switching between all routes works", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });

    // Switch to each route and verify content
    await page.click('.nav-link:has-text("Events")');
    await expect(page.locator(".events-tab")).toBeVisible();

    await page.click('.nav-link:has-text("Endpoints")');
    await expect(page.locator(".endpoints-tab")).toBeVisible();

    await page.click('.nav-link:has-text("Logs")');
    await expect(page.locator(".log-viewer")).toBeVisible({ timeout: 10000 });

    // Back to dashboard
    await page.click('.nav-link:has-text("Dashboard")');
    await expect(page.locator(".status-grid")).toBeVisible();
  });

  test("active route link has aria-current attribute", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });

    await page.click('.nav-link:has-text("Events")');
    await expect(page.locator('.nav-link:has-text("Events")')).toHaveAttribute(
      "aria-current",
      "page",
    );
    await expect(
      page.locator('.nav-link:has-text("Dashboard")'),
    ).not.toHaveAttribute("aria-current", "page");
  });

  test("browser back button works after route change", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });

    await page.click('.nav-link:has-text("Events")');
    await expect(page.locator(".events-tab")).toBeVisible();

    await page.goBack();
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });
  });

  test("URL changes when navigating between routes", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator(".status-grid")).toBeVisible({ timeout: 10000 });

    await page.click('.nav-link:has-text("Events")');
    await expect(page).toHaveURL(/\/events/);

    await page.click('.nav-link:has-text("Endpoints")');
    await expect(page).toHaveURL(/\/endpoints/);

    await page.click('.nav-link:has-text("Logs")');
    await expect(page).toHaveURL(/\/logs/);

    await page.click('.nav-link:has-text("Dashboard")');
    await expect(page).toHaveURL(/\/$/);
  });
});
