import { test, expect, chromium } from "@playwright/test";
import * as path from "path";
import * as os from "os";

/**
 * YouTube Studio live broadcast verification.
 *
 * Uses a persistent Chrome profile with saved Google OAuth session to check
 * YouTube Studio for an active live broadcast.  HARD FAILS if no live
 * broadcast is detected — no informational-only nonsense.
 *
 * Setup (one-time, on stream.lan):
 *   HEADED=1 npx playwright test youtube-studio-check
 *   → Log into Google/YouTube in the opened browser window
 *   → The session is saved to the persistent profile directory
 *
 * CI runs use the saved session in headless mode automatically.
 */

const PROFILE_DIR =
  process.env.YT_PROFILE_DIR ||
  (os.platform() === "win32"
    ? "C:\\Users\\newlevel\\.playwright-yt-profile"
    : path.join(os.homedir(), ".playwright-yt-profile"));

const YOUTUBE_STUDIO_URL = "https://studio.youtube.com";
const MAX_RETRIES = 6;
const RETRY_DELAY_MS = 10_000;

test("YouTube Studio shows an active live broadcast", async () => {
  const headed = !!process.env.HEADED;

  const context = await chromium.launchPersistentContext(PROFILE_DIR, {
    headless: !headed,
    channel: "chrome",
    args: [
      "--disable-blink-features=AutomationControlled",
      "--disable-features=LockProfileCookieDatabase",
      "--no-first-run",
      "--no-default-browser-check",
    ],
    viewport: { width: 1280, height: 720 },
    // Give pages plenty of time for YouTube Studio's heavy JS
    timeout: 60_000,
  });

  const page = context.pages()[0] || (await context.newPage());

  try {
    // Navigate to YouTube Studio
    await page.goto(YOUTUBE_STUDIO_URL, {
      waitUntil: "domcontentloaded",
      timeout: 30_000,
    });

    // Wait for initial load
    await page.waitForTimeout(3_000);

    // Check if we got redirected to a login page — means session expired
    const currentUrl = page.url();
    if (
      currentUrl.includes("accounts.google.com") ||
      currentUrl.includes("signin")
    ) {
      if (headed) {
        // In headed/setup mode, let user log in manually
        console.log("========================================");
        console.log("  MANUAL LOGIN REQUIRED");
        console.log("  Log into YouTube Studio in the browser window.");
        console.log("  After login, close the browser or press Ctrl+C.");
        console.log("========================================");
        // Wait a long time for manual login
        await page.waitForURL("**/studio.youtube.com/**", {
          timeout: 300_000,
        });
      } else {
        throw new Error(
          "FAILED: Google OAuth session expired. " +
            "Re-run with HEADED=1 to log in: HEADED=1 npx playwright test youtube-studio-check",
        );
      }
    }

    // We're on YouTube Studio — navigate to live control room
    // YouTube Studio URL pattern: studio.youtube.com/channel/CHANNEL_ID/livestreaming
    // Or use the direct "Go Live" dashboard
    await page.goto(`${YOUTUBE_STUDIO_URL}/channel`, {
      waitUntil: "domcontentloaded",
      timeout: 30_000,
    });
    await page.waitForTimeout(2_000);

    // Try the livestreaming dashboard
    // YouTube Studio may redirect to channel/<ID>/livestreaming/stream
    const channelUrl = page.url();
    const channelIdMatch = channelUrl.match(/channel\/([^/]+)/);
    if (channelIdMatch) {
      await page.goto(
        `${YOUTUBE_STUDIO_URL}/channel/${channelIdMatch[1]}/livestreaming/stream`,
        { waitUntil: "domcontentloaded", timeout: 30_000 },
      );
    } else {
      // Fallback: click on "Go live" or "Content" > "Live" in the sidebar
      await page.goto(`${YOUTUBE_STUDIO_URL}`, {
        waitUntil: "domcontentloaded",
        timeout: 30_000,
      });
    }

    await page.waitForTimeout(3_000);

    // Retry loop: look for live broadcast indicators
    let foundLive = false;
    let lastError = "";

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      console.log(
        `--- Attempt ${attempt}/${MAX_RETRIES}: checking for live broadcast ---`,
      );

      try {
        // Strategy 1: Check for "LIVE" badge/indicator on the livestreaming page
        // YouTube Studio shows a red "LIVE" pill/badge when broadcasting
        const liveIndicators = await page.locator(
          [
            // Red "LIVE" badge text
            'text="LIVE"',
            // Live indicator elements (various YouTube Studio versions)
            '[class*="live-indicator"]',
            '[class*="live-badge"]',
            '[class*="is-live"]',
            // Stream health panel visible during live broadcast
            '[class*="stream-health"]',
            // "Live now" text variants
            'text="Live now"',
            'text="LIVE NOW"',
            // The live streaming status indicator
            '[data-is-live="true"]',
          ].join(", "),
        );

        const count = await liveIndicators.count();
        if (count > 0) {
          // Verify at least one is visible
          for (let i = 0; i < count; i++) {
            const el = liveIndicators.nth(i);
            if (await el.isVisible()) {
              const text = await el.textContent();
              console.log("==========================================");
              console.log("  YOUTUBE BROADCAST IS LIVE!");
              console.log(`  Indicator found: "${text?.trim()}"`);
              console.log("==========================================");
              foundLive = true;
              break;
            }
          }
        }

        if (foundLive) break;

        // Strategy 2: Check page content for live-stream-related text
        const pageContent = await page.textContent("body");
        if (pageContent) {
          // YouTube Studio shows specific text during a live broadcast
          const livePatterns = [
            /\bLIVE\b.*\bstream\b/i,
            /\bstreaming\s+live\b/i,
            /\bcurrently\s+live\b/i,
            /\blive\s+now\b/i,
          ];
          for (const pattern of livePatterns) {
            if (pattern.test(pageContent)) {
              console.log("==========================================");
              console.log("  YOUTUBE BROADCAST IS LIVE!");
              console.log(`  Matched pattern: ${pattern}`);
              console.log("==========================================");
              foundLive = true;
              break;
            }
          }
        }

        if (foundLive) break;

        lastError = `No live broadcast indicators found (attempt ${attempt})`;
        console.log(lastError);
      } catch (err) {
        lastError = `Check failed on attempt ${attempt}: ${err}`;
        console.log(lastError);
      }

      if (attempt < MAX_RETRIES) {
        console.log(`Waiting ${RETRY_DELAY_MS / 1000}s before retry...`);
        await page.waitForTimeout(RETRY_DELAY_MS);
        // Reload the page for fresh state
        await page.reload({ waitUntil: "domcontentloaded", timeout: 30_000 });
        await page.waitForTimeout(3_000);
      }
    }

    // HARD FAIL if no live broadcast detected
    expect(
      foundLive,
      `FAILED: No live broadcast detected on YouTube Studio after ${MAX_RETRIES} attempts. ` +
        `Last error: ${lastError}. ` +
        `The delivering server confirmed chunk progression (previous CI step), ` +
        `but YouTube Studio does not show an active live stream.`,
    ).toBe(true);
  } finally {
    await context.close();
  }
});
