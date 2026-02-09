import { test, expect } from "@playwright/test";
import { mockAllApiRoutes, navigateAndWait } from "./helpers";

test.describe("LogViewer", () => {
  test("renders log viewer section", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    await expect(page.getByRole("heading", { name: /Live Log/ })).toBeVisible();
  });

  test("shows disconnected when WebSocket cannot connect", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // Since we mock HTTP routes but not WebSocket, WS will fail to connect
    // The LogViewer should show "(disconnected)"
    await expect(page.getByText("(disconnected)")).toBeVisible();
  });

  test("shows waiting message when no events", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // With no WS connection, there are no events
    await expect(page.getByText("Waiting for events...")).toBeVisible();
  });

  test("log viewer has dark background style", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // The log container has dark background
    const logContainer = page.locator(
      "div[style*='background: rgb(30, 30, 30)']",
    );
    await expect(logContainer).toBeVisible();
  });
});
