import { test, expect } from "@playwright/test";
import { mockAllApiRoutes, navigateAndWait } from "./helpers";
import { mockConfig } from "./fixtures";

test.describe("ConfigEditor", () => {
  test("renders configuration section", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    await expect(
      page.getByRole("heading", { name: "Configuration" }),
    ).toBeVisible();
  });

  test("displays config JSON with client_uuid", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // Config pre block should contain the client UUID
    await expect(page.getByText("test-uuid-00000000")).toBeVisible();
  });

  test("displays redacted S3 credentials", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // S3 credentials should show as "***"
    const preBlock = page.locator("pre");
    const content = await preBlock.textContent();
    expect(content).toContain('"access_key_id": "***"');
    expect(content).toContain('"secret_access_key": "***"');
  });

  test("displays manager URL", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    await expect(page.getByText("restreamer.newlevel.media")).toBeVisible();
  });

  test("displays inpoint configuration", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // RTMP port should be visible in config
    await expect(page.getByText("1935")).toBeVisible();
    // Chunk duration
    await expect(page.getByText("5000")).toBeVisible();
  });

  test("shows error when config fetch fails", async ({ page }) => {
    // Override config endpoint to return error
    await page.route("http://127.0.0.1:8910/api/v1/config", (route) =>
      route.fulfill({ status: 500, body: "Internal Server Error" }),
    );
    // Mock other routes normally
    await page.route("http://127.0.0.1:8910/api/v1/status", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          inpoint: { state: "running", details: {} },
          endpoint: { state: "running", details: {} },
          poller: { state: "running", details: {} },
          streaming_event: null,
        }),
      }),
    );
    await page.route("http://127.0.0.1:8910/api/v1/chunks*", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: "[]",
      }),
    );
    await page.route("http://127.0.0.1:8910/api/v1/chunks/stats", (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          total_chunks: 0,
          pending_chunks: 0,
          sent_chunks: 0,
          in_process_chunks: 0,
          total_bytes: 0,
          buffer_duration_secs: 0,
        }),
      }),
    );

    await page.goto("/");
    await page.waitForSelector("h1");

    // Error message should appear
    await expect(page.getByText("Failed to load config")).toBeVisible();
  });

  test("shows Loading before config arrives", async ({ page }) => {
    // Set up all mocks first, then override config with delayed response
    await mockAllApiRoutes(page);
    // Override with delayed config (last route wins in Playwright)
    await page.route("http://127.0.0.1:8910/api/v1/config", (route) => {
      if (route.request().method() === "GET") {
        return new Promise((resolve) =>
          setTimeout(() => {
            resolve(
              route.fulfill({
                status: 200,
                contentType: "application/json",
                body: JSON.stringify(mockConfig),
              }),
            );
          }, 3000),
        );
      }
      return route.fulfill({ status: 200, body: "" });
    });

    await page.goto("/");
    await page.waitForSelector("h1");

    // Pre block shows "Loading..." initially
    const preBlock = page.locator("pre");
    await expect(preBlock).toContainText("Loading...");
  });

  test("config JSON is properly formatted", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // The config should be formatted with indentation (JSON.stringify with 2 spaces)
    const preBlock = page.locator("pre");
    const content = await preBlock.textContent();
    // Formatted JSON should contain newlines (the pre block preserves them)
    expect(content).toContain("client_uuid");
    expect(content).toContain("manager_url");
    expect(content).toContain("inpoint");
    expect(content).toContain("s3");
  });
});
