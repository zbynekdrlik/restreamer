import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// Inject Tauri mock so the Leptos app is in "Tauri mode" for invoke().
const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

// #68: the guided "Change Key" action on a live endpoint must run the whole
// remove -> update_endpoint -> re-add(Live) sequence from a single click, so an
// operator can rotate a broken YouTube/FB stream key mid-event without the
// undocumented 3-step dance across two screens (an in-place key edit while the
// endpoint is attached is silently inert).
test("Change Key on a live endpoint runs remove -> update -> re-add(Live)", async ({
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

  await page.goto("/");
  await expect(page.locator(".audit-panel")).toBeVisible({ timeout: 10000 });

  // Drive a live delivery with one endpoint whose alias matches a CONFIGURED
  // endpoint ("YouTube Main", id 1 in the mock's endpoints list) so the modal
  // can resolve the endpoint id for update_endpoint + re-add.
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
            alias: "YouTube Main",
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

  // The endpoint card renders with the guided "Key" button.
  await expect(
    page.locator(".endpoint-alias", { hasText: "YouTube Main" }),
  ).toBeVisible({ timeout: 10000 });
  const keyBtn = page.locator('[data-testid="btn-change-key"]').first();
  await expect(keyBtn).toBeVisible();
  await keyBtn.click();

  // The guided modal opens; enter a new key and confirm.
  const modal = page.locator('[data-testid="change-key-modal"]');
  await expect(modal).toBeVisible();
  await expect(modal).toContainText("YouTube Main");
  await modal
    .locator('[data-testid="change-key-input"]')
    .fill("new-secret-key-123");
  await modal.locator('[data-testid="change-key-confirm"]').click();

  // Modal closes once the sequence completes.
  await expect(modal).toBeHidden({ timeout: 5000 });

  // The mock recorded the full guided sequence in order: detach the live
  // endpoint, persist the new key, then re-add it at the LIVE edge.
  await expect
    .poll(
      async () => {
        const res = await request.get(
          "http://127.0.0.1:8910/api/v1/_test/change-key-ops",
        );
        return res.ok() ? await res.json() : [];
      },
      { timeout: 5000 },
    )
    .toHaveLength(3);

  const opsRes = await request.get(
    "http://127.0.0.1:8910/api/v1/_test/change-key-ops",
  );
  const ops = await opsRes.json();
  expect(ops[0]).toMatchObject({ op: "remove", alias: "YouTube Main" });
  expect(ops[1]).toMatchObject({
    op: "update",
    id: 1,
    stream_key: "new-secret-key-123",
  });
  expect(ops[2]).toMatchObject({ op: "add", endpoint_id: 1 });
  expect(ops[2].start_position).toMatchObject({ strategy: "Live" });

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
