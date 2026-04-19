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
  // rtmp-gate-tick scenario: rtmp_stable_secs ramps from 0 at 15 simulated
  // seconds per real second, so the button enables within ~1s.
  await request.post("http://127.0.0.1:8910/api/v1/_test/scenario", {
    data: { scenario: "rtmp-gate-tick" },
  });

  await page.goto("/");

  await expect(page.locator(".event-selector")).toBeVisible({ timeout: 10000 });
  // Select the first event so the "needs event" gate is cleared — only
  // the RTMP-stable gate should keep the button disabled.
  await page.waitForTimeout(500);
  await page.locator(".event-selector").selectOption({ index: 1 });

  const btn = page.locator(".start-btn");
  await expect(btn).toBeVisible();

  // Initially disabled with a "Waiting for OBS" tooltip that shows
  // progress toward 15s.
  await expect(btn).toBeDisabled();
  await expect(btn).toHaveAttribute("title", /Waiting for OBS.*\d+\/15s/);

  // After the ticker ramps past 15s the button should enable. The
  // frontend polls /status every 2s, so allow up to 8s for the signal
  // to propagate.
  await expect(btn).toBeEnabled({ timeout: 8000 });

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
