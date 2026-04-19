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

test("start delivery button disabled until RTMP stable 15s", async ({
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
  // Use the explicit `set-rtmp-stable-secs` override instead of the
  // time-based `rtmp-gate-tick` scenario. The scenario's shared ticker
  // is racy under parallel workers: when another worker posts __reset
  // or changes the scenario it resets the ticker to 0 and the 8s
  // assertion window times out. The explicit override is set per-test
  // between assertions and does not depend on shared global state.
  await request.post("http://127.0.0.1:8910/api/v1/_test/scenario", {
    data: { scenario: "default" },
  });
  await request.post(
    "http://127.0.0.1:8910/api/v1/_test/set-rtmp-stable-secs",
    { data: { secs: 5 } },
  );

  await page.goto("/");

  await expect(page.locator(".event-selector")).toBeVisible({ timeout: 10000 });
  // Select the first event so the "needs event" gate is cleared — only
  // the RTMP-stable gate should keep the button disabled.
  await page.waitForTimeout(500);
  await page.locator(".event-selector").selectOption({ index: 1 });

  const btn = page.locator(".start-btn");
  await expect(btn).toBeVisible();

  // Initially disabled with a "Waiting for OBS" tooltip that shows
  // progress toward 15s (stable=5 < required=15).
  await expect(btn).toBeDisabled();
  await expect(btn).toHaveAttribute("title", /Waiting for OBS.*\d+\/15s/);

  // Now pin rtmp_stable_secs above the 15s threshold. The frontend
  // polls /status every 2s, so allow up to 6s for the new value to
  // propagate and re-enable the button.
  await request.post(
    "http://127.0.0.1:8910/api/v1/_test/set-rtmp-stable-secs",
    { data: { secs: 20 } },
  );
  await expect(btn).toBeEnabled({ timeout: 6000 });

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
