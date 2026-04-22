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

test("endpoint history sparkline renders after metrics samples arrive", async ({
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
  // last-endpoint scenario gives us exactly one delivery card ("yt1") so
  // the first "History" button is unambiguous.
  await request.post("http://127.0.0.1:8910/api/v1/_test/scenario", {
    data: { scenario: "last-endpoint" },
  });

  await page.goto("/");

  await expect(page.locator(".endpoint-alias", { hasText: "yt1" })).toBeVisible(
    { timeout: 10000 },
  );

  // Emit several MetricsSample events for alias "yt1" so the sparkline
  // has ≥2 points to draw a path.
  await request.post("http://127.0.0.1:8910/api/v1/_test/emit-metrics-sample", {
    data: { alias: "yt1", count: 5 },
  });

  // Toggle the per-card History panel open.
  await page.locator(".btn-endpoint-history").first().click();

  // The sparkline SVG path should be visible once samples have been
  // received.
  await expect(page.locator(".endpoint-history svg path")).toBeVisible({
    timeout: 10000,
  });

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
