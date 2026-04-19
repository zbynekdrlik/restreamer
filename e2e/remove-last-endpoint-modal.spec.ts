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
  await request.post("http://127.0.0.1:8910/api/v1/_test/scenario", {
    data: { scenario: "last-endpoint" },
  });

  await page.goto("/");

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
