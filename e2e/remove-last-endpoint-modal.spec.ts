import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

test("removing the last endpoint during active delivery shows confirm modal", async ({
  page,
  request,
}) => {
  const consoleMessages: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  await page.addInitScript(tauriMockScript);
  await request.post("http://127.0.0.1:8910/api/v1/__reset");
  // last-endpoint scenario: delivering active with exactly one endpoint "yt1".
  // /status polls and the cached-delivery endpoint will return the scenario's
  // streaming_event and cached DeliveryStatus.
  await request.post("http://127.0.0.1:8910/api/v1/_test/scenario", {
    data: { scenario: "last-endpoint" },
  });

  await page.goto("/");

  // Wait for the dashboard shell so we know the WebSocket client is
  // connected (the `.audit-panel` renders unconditionally on the operator
  // dashboard and does not depend on scenario state).
  await expect(page.locator(".audit-panel")).toBeVisible({ timeout: 10000 });

  // Explicitly broadcast DeliveryStatus + PipelineState to this page's WS
  // client. The mock-api's `scenario` variable is process-global, and
  // parallel Playwright workers running other spec files can reset it
  // between our `/_test/scenario` call and the time the WASM client opens
  // its WebSocket — which would make the WS connect-handler send default
  // data (2 endpoints, no "yt1") and the card below would never render.
  // Broadcasting here forces the state this test needs regardless of what
  // `scenario` happened to be at WS-open time.
  await request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
    data: {
      type: "DeliveryStatus",
      data: {
        instance_name: "rs-delivery-evt1",
        status: "running",
        server_ip: "1.2.3.4",
        endpoint_count: 1,
        endpoints: [
          {
            alias: "yt1",
            alive: true,
            current_chunk_id: 142,
            bytes_processed_total: 1073741824,
            chunks_processed: 1847,
            chunk_delay_secs: 3.2,
            stall_reason: null,
            ffmpeg_restart_count: 0,
            last_error: null,
            is_fast: false,
            delivery_mode: "normal",
            rescue_eta_secs: null,
          },
        ],
      },
    },
  });
  await request.post("http://127.0.0.1:8910/api/v1/_test/ws-broadcast", {
    data: {
      type: "PipelineState",
      data: {
        state: "streaming",
        event_id: 1,
        event_name: "test-event",
        target_delay_secs: 120,
        session_start: new Date().toISOString(),
        local_buffer_chunks: 10,
        s3_queue_chunks: 5,
        cache_duration_secs: 118.0,
      },
    },
  });

  // Wait for the endpoint card for "yt1" to render.
  await expect(page.locator(".endpoint-alias", { hasText: "yt1" })).toBeVisible(
    { timeout: 10000 },
  );

  // Click the × remove button on that endpoint. There's only one card
  // in this scenario so `.btn-remove-endpoint` is unambiguous.
  await page.locator(".btn-remove-endpoint").first().click();

  // The last-endpoint confirm modal (EndpointRemoveConfirmModal) renders
  // `.endpoint-remove-modal` with the "Remove last endpoint" heading.
  const modal = page.locator(".endpoint-remove-modal");
  await expect(modal).toBeVisible();
  await expect(modal).toContainText("Remove last endpoint");

  // Confirm button is disabled until the event name is typed verbatim.
  const confirm = modal.locator("button.confirm-btn-danger");
  await expect(confirm).toBeDisabled();

  await modal.locator("input.endpoint-remove-modal__input").fill("test-event");
  await expect(confirm).toBeEnabled();

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
