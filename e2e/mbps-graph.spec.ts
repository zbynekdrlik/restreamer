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

// #77: the dashboard shows a historical time-graph of outgoing (S3-upload)
// Mbps. This loads the dashboard, waits for the graph to render its SVG path
// from the mocked /uploads/throughput series, and asserts ZERO console
// errors/warnings (last).
test("outgoing-Mbps history graph renders from the throughput series", async ({
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
    data: { scenario: "last-endpoint" },
  });

  await page.goto("/");

  // The graph container mounts under the S3->VPS node.
  await expect(page.locator(".mbps-graph")).toBeVisible({ timeout: 10000 });
  await expect(page.locator(".mbps-graph__title")).toHaveText(
    /Outgoing to internet/i,
  );

  // With >=2 samples the area + line paths draw.
  await expect(page.locator(".mbps-graph__svg path.mbps-graph__line")).toBeVisible(
    { timeout: 10000 },
  );
  await expect(page.locator(".mbps-graph__svg path.mbps-graph__area")).toHaveCount(
    1,
  );

  // The header readout shows the peak from the series (6.4 Mbps).
  await expect(page.locator(".mbps-graph__peak")).toContainText("peak 6.4");

  // Zero console errors/warnings — assert LAST.
  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
