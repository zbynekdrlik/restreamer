import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// #166 — Facebook ingestion-health badge on the endpoint card.
//
// The dashboard must surface FB-side ingest health next to the YT badge. The
// operator-critical case: we push bytes but FB has ZERO receiving live_video
// (silent discard) — the badge must read RED so the operator sees the failure
// without opening FB Live Producer. Seeded through the scenario-based mock-api
// harness (backs BOTH the cached-status HTTP load and the follow-up WS push),
// the same pattern outage-ui.spec.ts uses.

const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// Chromium-level warnings that are not application bugs.
const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i,
];

test("FB endpoint shows a red ingestion-health badge, clean console", async ({
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
  // One FB endpoint whose Graph ingest health is NO_LIVE_VIDEO/bad — the
  // silent-discard failure the whole feature exists to surface.
  await request.post("http://127.0.0.1:8910/api/v1/_test/scenario", {
    data: { scenario: "fb-health" },
  });

  await page.goto("/");

  const badge = page.locator('[data-testid="fb-health-badge"]');
  await expect(badge).toBeVisible({ timeout: 10000 });
  // RED — bad health (the silent-discard case).
  await expect(badge).toHaveAttribute("data-health", "bad");
  // Assert on the visible text span, not the container (which also holds the
  // hidden tooltip text).
  await expect(badge.locator(".fb-health-text")).toHaveText("bad");

  // The tooltip carries the FB status for the operator on hover.
  await badge.hover();
  await expect(
    page.locator('[data-testid="fb-health-tooltip"]'),
  ).toContainText("NO_LIVE_VIDEO");

  // Zero browser console errors / warnings (subresource integrity exempted) —
  // asserted LAST.
  const real = consoleMessages.filter(
    (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
  );
  expect(real).toEqual([]);
});
