import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// Regression E2E for #343: "Tried to access a reactive value that has already
// been disposed" WASM panic on SPA navigation.
//
// Root cause (see the issue's design comment): the dashboard's ControlBar sets
// up 1 s / 2 s `gloo_timers::Interval`s and detaches them with
// `std::mem::forget`, plus several components leak on-mount async tasks. When a
// CLIENT-SIDE route change disposes the component, the leaked timer/task keeps
// firing and touches a now-disposed signal -> `reactive_graph` panic ->
// `RuntimeError: unreachable`, surfaced via `console_error_panic_hook` as a
// console error (and a pageerror).
//
// The trigger is the SPA transition specifically: a FULL browser load of
// `/settings` never mounts `/` first, so nothing is disposed and no panic
// occurs. This spec therefore navigates by CLICKING the router `<A>` link
// (client-side), never `page.goto("/settings")`, and idles long enough for the
// leaked 1 s / 2 s dashboard intervals to fire against the disposed ControlBar
// signals.

const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// Chromium-level warnings that are not application bugs (mirrors frontend.spec).
const ALLOWED_CONSOLE = [
  /integrity.*attribute.*currently ignored.*subresource integrity/i, // crbug.com/981419
];

test.describe("SPA navigation reactive-disposal (#343)", () => {
  let consoleMessages: string[] = [];
  let pageErrors: string[] = [];

  test.beforeEach(async ({ page, request }) => {
    consoleMessages = [];
    pageErrors = [];
    page.on("console", (msg) => {
      if (msg.type() === "error" || msg.type() === "warning") {
        consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
      }
    });
    // A WASM panic also surfaces as an uncaught RuntimeError (pageerror).
    page.on("pageerror", (err) => {
      pageErrors.push(`[pageerror] ${err.message}`);
    });
    await page.addInitScript(tauriMockScript);
    await request.post("http://127.0.0.1:8910/api/v1/__reset");
  });

  test("dashboard -> settings -> tab switches produce ZERO console errors", async ({
    page,
  }) => {
    // 1. Full load of the dashboard so ControlBar (and its leaked 1 s tick +
    //    2 s bitrate intervals) actually mounts.
    await page.goto("/");
    await expect(page.locator(".operator-dashboard")).toBeVisible({
      timeout: 10000,
    });
    await expect(page.locator(".state-badge")).toBeVisible({ timeout: 10000 });

    // 2. CLIENT-SIDE route change (the actual trigger) — click the header's
    //    router <A>, do NOT page.goto. This disposes OperatorDashboard/ControlBar
    //    while the forgotten intervals keep ticking.
    await page.locator('.header-nav-btn:has-text("Settings")').click();
    await expect(page.locator(".settings-page")).toBeVisible({ timeout: 10000 });

    // 3. Idle long enough for the leaked 1 s tick and 2 s bitrate intervals to
    //    fire at least twice against the now-disposed ControlBar signals. On the
    //    unfixed build this is where the panics accumulate.
    await page.waitForTimeout(3500);

    // 4. Switch Settings tabs Events -> Config -> Templates -> Events. Each tab
    //    swap disposes the previous tab's sub-tree; on the unfixed build any
    //    still-in-flight on-mount fetch panics on its disposed signal.
    for (const tab of ["Config", "Templates", "Events", "Config"]) {
      await page.locator(`.settings-tabs .tab:has-text("${tab}")`).click();
      await page.waitForTimeout(400);
    }

    // 5. Navigate back to the dashboard client-side and idle once more — the
    //    settings-route timers/tasks must not panic either.
    await page.locator('.header-nav-btn:has-text("Dashboard")').click();
    await expect(page.locator(".operator-dashboard")).toBeVisible({
      timeout: 10000,
    });
    await page.waitForTimeout(2500);

    const realConsole = consoleMessages.filter(
      (m) => !ALLOWED_CONSOLE.some((r) => r.test(m)),
    );
    expect(
      realConsole,
      `console errors/warnings during SPA navigation:\n${realConsole.join("\n")}`,
    ).toEqual([]);
    expect(
      pageErrors,
      `uncaught page errors during SPA navigation:\n${pageErrors.join("\n")}`,
    ).toEqual([]);
  });
});
