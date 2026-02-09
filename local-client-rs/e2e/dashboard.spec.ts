import { test, expect } from "@playwright/test";
import { mockAllApiRoutes, navigateAndWait } from "./helpers";
import { mockStatus, mockStatusDisconnected } from "./fixtures";

test.describe("Dashboard", () => {
  test("renders page header with title", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    await expect(page.locator("h1")).toHaveText("Restreamer Dashboard");
  });

  test("shows Connected status when API responds", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // Status poll should succeed — header shows "Connected" (exact match to avoid "(connected)")
    await expect(page.getByText("Connected", { exact: true })).toBeVisible();
  });

  test("shows Disconnected status when API fails", async ({ page }) => {
    await mockAllApiRoutes(page, { statusCode: 500 });
    await navigateAndWait(page);

    // API returns 500, so status poll throws — header shows "Disconnected"
    await expect(page.getByText("Disconnected", { exact: true })).toBeVisible();
  });

  test("renders service status cards", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // Wait for status to load (first poll)
    await expect(page.getByText("Service Status")).toBeVisible();

    // Check status cards using strong elements (card titles)
    await expect(page.locator("strong", { hasText: "Inpoint" })).toBeVisible();
    await expect(page.locator("strong", { hasText: "Endpoint" })).toBeVisible();
    await expect(page.locator("strong", { hasText: "Poller" })).toBeVisible();

    // Each should show "running" state
    const runningTexts = page.getByText("running");
    await expect(runningTexts.first()).toBeVisible();
  });

  test("renders streaming event details when active", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // Streaming event card should be visible
    await expect(page.getByText("Streaming Event")).toBeVisible();
    await expect(page.getByText("evt-test-001")).toBeVisible();

    // Check received bytes (50 MB formatted)
    await expect(page.getByText("50.0 MB")).toBeVisible();

    // Check receiving/delivering toggles
    await expect(page.getByText("Receiving: Yes")).toBeVisible();
    await expect(page.getByText("Delivering: Yes")).toBeVisible();
  });

  test("does not show streaming event when null", async ({ page }) => {
    await mockAllApiRoutes(page, { status: mockStatusDisconnected });
    await navigateAndWait(page);

    // Wait for status to load
    await expect(page.getByText("Service Status")).toBeVisible();

    // No streaming event card
    await expect(page.getByText("Streaming Event")).not.toBeVisible();
  });

  test("shows correct status colors", async ({ page }) => {
    await mockAllApiRoutes(page, { status: mockStatusDisconnected });
    await navigateAndWait(page);

    // Wait for status to render
    await expect(page.getByText("Service Status")).toBeVisible();

    // "stopped" should be visible for inpoint and poller
    const stoppedTexts = page.getByText("stopped");
    await expect(stoppedTexts.first()).toBeVisible();

    // "error" should be visible for endpoint
    await expect(page.getByText("error")).toBeVisible();
  });

  test("shows loading state before API response", async ({ page }) => {
    // Don't mock — let routes hang
    await page.route("http://127.0.0.1:8910/api/v1/status", (route) =>
      setTimeout(() => {
        route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify(mockStatus),
        });
      }, 2000),
    );
    // Mock WS to prevent console errors
    await page.route("http://127.0.0.1:8910/api/v1/ws", (route) =>
      route.abort(),
    );

    await page.goto("/");
    await page.waitForSelector("h1");

    // Should show loading initially
    await expect(page.getByText("Loading service status...")).toBeVisible();

    // After response arrives, loading disappears
    await expect(page.getByText("Service Status")).toBeVisible({
      timeout: 5000,
    });
  });

  test("formats bytes correctly", async ({ page }) => {
    const customStatus = {
      ...mockStatus,
      streaming_event: {
        ...mockStatus.streaming_event,
        received_bytes: 500,
      },
    };
    await mockAllApiRoutes(page, { status: customStatus });
    await navigateAndWait(page);

    // 500 bytes should show as "500 B"
    await expect(page.getByText("500 B")).toBeVisible();
  });

  test("formats kilobytes correctly", async ({ page }) => {
    const customStatus = {
      ...mockStatus,
      streaming_event: {
        ...mockStatus.streaming_event,
        received_bytes: 5120,
      },
    };
    await mockAllApiRoutes(page, { status: customStatus });
    await navigateAndWait(page);

    // 5120 bytes = 5.0 KB
    await expect(page.getByText("5.0 KB")).toBeVisible();
  });
});
