import type { Page } from "@playwright/test";
import {
  API_BASE,
  mockStatus,
  mockChunks,
  mockChunkStats,
  mockConfig,
  mockLogsInpoint,
  mockLogsEndpoint,
} from "./fixtures";

/**
 * Set up route interception to mock all backend API responses.
 * This allows the frontend to render fully without a running Rust service.
 */
export async function mockAllApiRoutes(
  page: Page,
  overrides?: {
    status?: unknown;
    chunks?: unknown;
    chunkStats?: unknown;
    config?: unknown;
    logsInpoint?: unknown;
    logsEndpoint?: unknown;
    statusCode?: number;
  },
) {
  // Mock /health
  await page.route(`${API_BASE}/health`, (route) =>
    route.fulfill({ status: 200, body: "" }),
  );

  // Mock /status
  await page.route(`${API_BASE}/status`, (route) =>
    route.fulfill({
      status: overrides?.statusCode ?? 200,
      contentType: "application/json",
      body: JSON.stringify(overrides?.status ?? mockStatus),
    }),
  );

  // Mock /streaming-event (GET)
  await page.route(`${API_BASE}/streaming-event`, (route) => {
    if (route.request().method() === "DELETE") {
      return route.fulfill({ status: 204, body: "" });
    }
    const status = (overrides?.status as Record<string, unknown>) ?? mockStatus;
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(
        (status as { streaming_event?: unknown }).streaming_event ?? null,
      ),
    });
  });

  // Mock /chunks (GET with query params and DELETE)
  await page.route(`${API_BASE}/chunks?**`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(overrides?.chunks ?? mockChunks),
    }),
  );
  await page.route(`${API_BASE}/chunks`, (route) => {
    if (route.request().method() === "DELETE") {
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: "3",
      });
    }
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(overrides?.chunks ?? mockChunks),
    });
  });

  // Mock /chunks/stats
  await page.route(`${API_BASE}/chunks/stats`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(overrides?.chunkStats ?? mockChunkStats),
    }),
  );

  // Mock /config (GET and PATCH)
  await page.route(`${API_BASE}/config`, (route) => {
    if (route.request().method() === "PATCH") {
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(overrides?.config ?? mockConfig),
      });
    }
    return route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(overrides?.config ?? mockConfig),
    });
  });

  // Mock /logs/inpoint
  await page.route(`${API_BASE}/logs/inpoint*`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(overrides?.logsInpoint ?? mockLogsInpoint),
    }),
  );

  // Mock /logs/endpoint
  await page.route(`${API_BASE}/logs/endpoint*`, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(overrides?.logsEndpoint ?? mockLogsEndpoint),
    }),
  );

  // Mock action endpoints
  await page.route(`${API_BASE}/actions/**`, (route) =>
    route.fulfill({ status: 200, body: "" }),
  );

  // Mock WebSocket — Playwright can't intercept WS natively, so
  // the frontend will fail to connect and show "Disconnected" for
  // the WS-based features. We test WS via the ws-specific test
  // that launches a real WS server.
}

/**
 * Navigate to the app and wait for initial render.
 */
export async function navigateAndWait(page: Page) {
  await page.goto("/");
  // Wait for the h1 header to render
  await page.waitForSelector("h1");
}
