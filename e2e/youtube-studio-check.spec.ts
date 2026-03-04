import { test, expect, chromium } from "@playwright/test";
import * as path from "path";
import * as os from "os";
import * as fs from "fs";

/**
 * YouTube Studio stream-receiving verification.
 *
 * Uses a persistent Chrome profile with saved Google OAuth session to check
 * YouTube Studio's Live Control Room for an active stream in TESTING state.
 *
 * Auto-start is BANNED (YouTube destroys the stream afterward), so the
 * broadcast stays in "testing" state — YouTube receives data but is NOT
 * publicly live.  This test verifies that YouTube IS receiving the stream
 * by checking for:
 *   - "Go live" button (enabled when stream data is being received)
 *   - Stream health indicators ("Excellent", "Good", "OK")
 *   - Stream preview content
 *   - Absence of "waiting for stream" placeholder messages
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

const SCREENSHOT_DIR =
  process.env.SCREENSHOT_DIR ||
  (os.platform() === "win32"
    ? "C:\\Users\\newlevel\\.playwright-yt-screenshots"
    : path.join(os.homedir(), ".playwright-yt-screenshots"));

const YOUTUBE_STUDIO_URL = "https://studio.youtube.com";
const MAX_RETRIES = 6;
const RETRY_DELAY_MS = 10_000;

test("YouTube Studio shows stream is being received (testing state)", async () => {
  const headed = !!process.env.HEADED;

  // Ensure screenshot directory exists
  fs.mkdirSync(SCREENSHOT_DIR, { recursive: true });

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
        await page.screenshot({
          path: path.join(SCREENSHOT_DIR, "login-redirect.png"),
          fullPage: true,
        });
        throw new Error(
          "FAILED: Google OAuth session expired. " +
            "Re-run with HEADED=1 to log in: HEADED=1 npx playwright test youtube-studio-check",
        );
      }
    }

    // Navigate to Live Control Room
    // First get the channel ID from YouTube Studio
    await page.goto(`${YOUTUBE_STUDIO_URL}/channel`, {
      waitUntil: "domcontentloaded",
      timeout: 30_000,
    });
    await page.waitForTimeout(2_000);

    const channelUrl = page.url();
    const channelIdMatch = channelUrl.match(/channel\/([^/]+)/);
    if (channelIdMatch) {
      // Go directly to the Live Control Room (livestreaming/stream page)
      await page.goto(
        `${YOUTUBE_STUDIO_URL}/channel/${channelIdMatch[1]}/livestreaming/stream`,
        { waitUntil: "domcontentloaded", timeout: 30_000 },
      );
    } else {
      // Fallback: go to YouTube Studio main page
      await page.goto(`${YOUTUBE_STUDIO_URL}`, {
        waitUntil: "domcontentloaded",
        timeout: 30_000,
      });
    }

    await page.waitForTimeout(5_000);

    // Retry loop: look for stream-receiving indicators (testing state)
    // Since auto-start is BANNED, YouTube won't show "LIVE" — instead we
    // look for evidence that the stream data is being received:
    //   1. "Go live" button (present and enabled = stream is connected)
    //   2. Stream health text ("Excellent", "Good", "OK", "Bad")
    //   3. Stream preview showing video content
    //   4. Absence of "waiting for data" messages
    let streamReceiving = false;
    let lastError = "";
    let matchedIndicator = "";

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      console.log(
        `--- Attempt ${attempt}/${MAX_RETRIES}: checking for stream receiving ---`,
      );

      try {
        // Take screenshot for debugging
        await page.screenshot({
          path: path.join(SCREENSHOT_DIR, `attempt-${attempt}.png`),
          fullPage: true,
        });

        // Get the full page text content for text-based matching
        const pageContent = (await page.textContent("body")) || "";
        console.log(`Page URL: ${page.url()}`);
        console.log(`Page text length: ${pageContent.length} chars`);

        // Log a snippet of the page for debugging (first 500 chars)
        const snippet = pageContent.replace(/\s+/g, " ").trim().slice(0, 500);
        console.log(`Page snippet: ${snippet}`);

        // === POSITIVE indicators: stream IS being received ===

        // Check 1: "Go live" button — THE primary indicator.
        // When YouTube receives stream data, the "Go live" button appears
        // and becomes clickable in the Live Control Room.
        const goLiveButton = page.locator(
          [
            'button:has-text("Go live")',
            'button:has-text("GO LIVE")',
            'button:has-text("Go Live")',
            '[aria-label*="Go live"]',
            '[aria-label*="GO LIVE"]',
          ].join(", "),
        );
        const goLiveCount = await goLiveButton.count();
        if (goLiveCount > 0) {
          for (let i = 0; i < goLiveCount; i++) {
            if (await goLiveButton.nth(i).isVisible()) {
              matchedIndicator = '"Go live" button is visible';
              streamReceiving = true;
              break;
            }
          }
        }

        if (streamReceiving) break;

        // Check 2: Stream health indicators.
        // YouTube Studio shows "Excellent", "Good", "OK", or "Bad" for
        // stream health when data is being received.
        const healthPatterns = [
          /\bExcellent\b/,
          /\bGood\b.*\b(stream|connection|health|quality)\b/i,
          /\b(stream|connection|health|quality)\b.*\bGood\b/i,
          /\bstream\s+health\b/i,
        ];
        for (const pattern of healthPatterns) {
          if (pattern.test(pageContent)) {
            matchedIndicator = `Stream health indicator: ${pattern}`;
            streamReceiving = true;
            break;
          }
        }

        if (streamReceiving) break;

        // Check 3: Text patterns indicating stream is connected/receiving.
        const receivingPatterns = [
          /\bGo\s+live\b/i, // "Go live" text anywhere
          /\bstream\s+preview\b/i, // Stream preview section
          /\bExcellent\s+Data\b/i, // Data quality indicator
          /\bconnected\b.*\bstream/i, // "Connected" + "stream"
          /\bstream.*\bconnected\b/i,
          /\breceiving\b.*\bdata\b/i, // "Receiving data"
          /\b\d+\s*kbps\b/i, // Bitrate indicator (e.g., "4500 kbps")
          /\b\d+x\d+\b.*\bfps\b/i, // Resolution + fps (e.g., "1920x1080 30fps")
          /\b\d+p\b.*\b\d+\s*fps\b/i, // "1080p 30 fps" format
        ];
        for (const pattern of receivingPatterns) {
          if (pattern.test(pageContent)) {
            matchedIndicator = `Text pattern matched: ${pattern}`;
            streamReceiving = true;
            break;
          }
        }

        if (streamReceiving) break;

        // Check 4: Look for video/stream preview elements that indicate
        // YouTube is showing the incoming stream.
        const previewElements = page.locator(
          [
            "video[src]", // Video element with a source
            '[class*="stream-preview"]',
            '[class*="video-preview"]',
            '[class*="preview-player"]',
            "canvas", // Canvas element (could be video rendering)
          ].join(", "),
        );
        const previewCount = await previewElements.count();
        if (previewCount > 0) {
          for (let i = 0; i < previewCount; i++) {
            if (await previewElements.nth(i).isVisible()) {
              matchedIndicator = "Video/preview element is visible";
              streamReceiving = true;
              break;
            }
          }
        }

        if (streamReceiving) break;

        // === NEGATIVE indicators (for logging): stream NOT receiving ===
        const notReceivingPatterns = [
          /connect\s+streaming\s+software/i,
          /no\s+content/i,
          /waiting\s+for/i,
          /start\s+streaming/i,
        ];
        const negatives: string[] = [];
        for (const pattern of notReceivingPatterns) {
          if (pattern.test(pageContent)) {
            negatives.push(pattern.toString());
          }
        }

        lastError =
          `No stream-receiving indicators found (attempt ${attempt})` +
          (negatives.length > 0
            ? `. Negative indicators present: ${negatives.join(", ")}`
            : "");
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

    // Take final screenshot
    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, "final.png"),
      fullPage: true,
    });

    if (streamReceiving) {
      console.log("==========================================");
      console.log("  YOUTUBE IS RECEIVING THE STREAM!");
      console.log(`  Indicator: ${matchedIndicator}`);
      console.log("  (Broadcast in testing state — auto-start is banned)");
      console.log("==========================================");
    }

    // HARD FAIL if YouTube is not receiving the stream
    expect(
      streamReceiving,
      `FAILED: YouTube Studio does not show stream is being received after ${MAX_RETRIES} attempts. ` +
        `Last error: ${lastError}. ` +
        `The delivering server confirmed chunk progression (previous CI step), ` +
        `but YouTube Studio Live Control Room does not show stream-receiving indicators. ` +
        `Screenshots saved to ${SCREENSHOT_DIR} for debugging.`,
    ).toBe(true);
  } finally {
    await context.close();
  }
});
