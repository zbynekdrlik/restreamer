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

test("banner visible when delivering active with zero endpoints", async ({
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
  // Select the zero-endpoints scenario BEFORE navigating so the mock
  // emits the right PipelineState + empty DeliveryStatus on connect.
  await request.post("http://127.0.0.1:8910/api/v1/_test/scenario", {
    data: { scenario: "zero-endpoints" },
  });

  await page.goto("/");

  // Banner must be visible when the pipeline is active but no endpoints
  // are attached.
  await expect(page.locator(".banner--critical")).toBeVisible({
    timeout: 10000,
  });
  await expect(page.locator(".banner--critical")).toContainText(
    "0 endpoints are running",
  );

  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
