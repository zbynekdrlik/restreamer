import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// Inject the Tauri mock so the Leptos app runs in browser mode (fetch over
// HTTP, not Tauri IPC), matching every other frontend spec in this suite.
const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// Chromium-level warnings that are not application bugs.
const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

// #408: the operator stopped the stream in OBS on purpose. The endpoint
// played its buffer out and stopped sending. The dashboard must show it as
// ended (grey, "UKONČENÉ (OBS zastavené)"), never as an outage: no calm
// outage banner, no red, and not the stale stall reason of its last push.
test("an endpoint ended by a deliberate OBS stop reads ended, not an outage", async ({
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
  await request.post("http://127.0.0.1:8910/api/v1/_test/scenario", {
    data: { scenario: "source-ended" },
  });

  await page.goto("/");

  const node = page.locator(".endpoint-node.ended");
  await expect(node).toBeVisible({ timeout: 10000 });
  await expect(node.locator(".status-dot.ended")).toBeVisible();
  await expect(node.locator(".endpoint-mode-ended")).toHaveText(
    "UKONČENÉ (OBS zastavené)",
  );
  // Not an outage: no stall text, no calm outage banner, nothing red.
  await expect(node.locator(".endpoint-anomaly")).toHaveCount(0);
  await expect(page.locator(".banner--recovering")).toHaveCount(0);
  await expect(page.locator(".endpoint-node.attention")).toHaveCount(0);
  await expect(page.locator(".endpoint-node.recovering")).toHaveCount(0);

  // Zero browser console errors / warnings (subresource integrity exempted).
  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
