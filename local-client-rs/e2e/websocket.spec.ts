import { test, expect } from "@playwright/test";
import { WebSocketServer } from "ws";
import { mockAllApiRoutes, navigateAndWait } from "./helpers";
import { mockStatus, mockWsEvents } from "./fixtures";

let wss: WebSocketServer | null = null;

test.describe("WebSocket Live Events", () => {
  test.afterEach(async () => {
    if (wss) {
      wss.close();
      wss = null;
    }
  });

  test("shows connected when WebSocket server is available", async ({
    page,
  }) => {
    // Start a real WebSocket server on port 8910 to mimic the Rust backend
    wss = new WebSocketServer({ port: 8910, path: "/api/v1/ws" });

    // Also mock HTTP routes — but need to serve them from same port
    // Since we can't easily do both HTTP + WS on same port without a
    // full server, we instead point the status poll at the mocked route
    // and let the WS connect to our real WS server.
    await mockAllApiRoutes(page);

    // The frontend tries to connect to ws://127.0.0.1:8910/api/v1/ws
    // Our WebSocketServer will accept it
    await navigateAndWait(page);

    // Wait for WebSocket connection to establish
    await expect(page.getByText("(connected)")).toBeVisible({
      timeout: 5000,
    });
  });

  test("displays events sent via WebSocket", async ({ page }) => {
    wss = new WebSocketServer({ port: 8910, path: "/api/v1/ws" });

    // When a client connects, send events after a short delay
    wss.on("connection", (ws) => {
      setTimeout(() => {
        ws.send(JSON.stringify(mockWsEvents.inpointStatus));
      }, 200);
      setTimeout(() => {
        ws.send(JSON.stringify(mockWsEvents.chunkReceived));
      }, 400);
    });

    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // Wait for the events to appear in the log viewer
    await expect(page.getByText("InpointStatus")).toBeVisible({
      timeout: 5000,
    });
    await expect(page.getByText("ChunkReceived")).toBeVisible({
      timeout: 5000,
    });
  });

  test("displays error events from WebSocket", async ({ page }) => {
    wss = new WebSocketServer({ port: 8910, path: "/api/v1/ws" });

    wss.on("connection", (ws) => {
      setTimeout(() => {
        ws.send(JSON.stringify(mockWsEvents.error));
      }, 200);
    });

    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // Error event should appear with its message
    await expect(page.getByText("S3 connection timeout")).toBeVisible({
      timeout: 5000,
    });
  });

  test("shows disconnected after WebSocket server closes", async ({ page }) => {
    wss = new WebSocketServer({ port: 8910, path: "/api/v1/ws" });

    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // First confirm connection
    await expect(page.getByText("(connected)")).toBeVisible({
      timeout: 5000,
    });

    // Close the server
    wss.close();
    wss = null;

    // Should switch to disconnected
    await expect(page.getByText("(disconnected)")).toBeVisible({
      timeout: 10_000,
    });
  });
});
