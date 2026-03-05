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
 * Modes:
 *   - Default: Verify stream IS being received (used after delivery starts)
 *   - EXPECT_NO_STREAM=1: Verify stream is NOT being received (baseline
 *     check before delivery starts, to prove state transition)
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

// Force English UI via hl parameter — the machine locale is Slovak
const YOUTUBE_STUDIO_URL = "https://studio.youtube.com";
// EXPECT_NO_STREAM=1 inverts the assertion: verifies YouTube is NOT receiving.
// Used as a baseline check before delivery starts to prove state transition.
const EXPECT_NO_STREAM = !!process.env.EXPECT_NO_STREAM;
const MAX_RETRIES = EXPECT_NO_STREAM ? 2 : 6;
const RETRY_DELAY_MS = 10_000;

const testName = EXPECT_NO_STREAM
  ? "YouTube Studio shows stream is NOT being received (baseline)"
  : "YouTube Studio shows stream is being received (testing state)";

test(testName, async () => {
  const headed = !!process.env.HEADED;

  // Ensure screenshot directory exists
  fs.mkdirSync(SCREENSHOT_DIR, { recursive: true });

  const context = await chromium.launchPersistentContext(PROFILE_DIR, {
    headless: !headed,
    channel: "chrome",
    locale: "en-US",
    args: [
      "--disable-blink-features=AutomationControlled",
      "--disable-features=LockProfileCookieDatabase",
      "--no-first-run",
      "--no-default-browser-check",
      "--lang=en-US",
    ],
    viewport: { width: 1280, height: 720 },
    // Give pages plenty of time for YouTube Studio's heavy JS
    timeout: 60_000,
  });

  const page = context.pages()[0] || (await context.newPage());

  try {
    // Navigate to YouTube Studio (force English with hl=en)
    await page.goto(`${YOUTUBE_STUDIO_URL}/?hl=en`, {
      waitUntil: "networkidle",
      timeout: 60_000,
    });

    // Wait for initial load — YouTube Studio is a very heavy SPA
    await page.waitForTimeout(5_000);

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, "01-initial-load.png"),
      fullPage: true,
    });

    // Handle "unsupported browser" interstitial page.
    // YouTube Studio may show a page saying "Upgrade your browser" with
    // a "SKIP TO YOUTUBE STUDIO" link at the bottom (or in Slovak:
    // "PRESKOČIŤ NA ŠTÚDIO YOUTUBE").
    const skipLink = page.locator(
      [
        'a:has-text("SKIP TO YOUTUBE STUDIO")',
        'a:has-text("Skip to YouTube Studio")',
        'a:has-text("PRESKOČIŤ NA ŠTÚDIO YOUTUBE")',
        // Also try button variants
        'button:has-text("SKIP TO YOUTUBE STUDIO")',
        'button:has-text("PRESKOČIŤ NA ŠTÚDIO YOUTUBE")',
        // Generic text link at bottom of browser upgrade page
        ':text("SKIP TO YOUTUBE")',
        ':text("PRESKOČIŤ")',
      ].join(", "),
    );

    const skipCount = await skipLink.count();
    if (skipCount > 0) {
      console.log(
        "Detected 'unsupported browser' interstitial — clicking skip link...",
      );
      await skipLink.first().click();
      // Wait for YouTube Studio to actually load after clicking skip
      await page.waitForTimeout(8_000);
      await page.screenshot({
        path: path.join(SCREENSHOT_DIR, "02-after-skip.png"),
        fullPage: true,
      });
    }

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

    // Navigate to Live Control Room.
    // First get the channel ID from YouTube Studio URL (it redirects to
    // studio.youtube.com/channel/CHANNEL_ID/...)
    console.log(`Current URL after Studio load: ${page.url()}`);

    // Try to extract channel ID from current URL
    let channelId = page.url().match(/channel\/([^/?]+)/)?.[1];

    if (!channelId) {
      // Navigate to /channel to trigger redirect that reveals channel ID
      await page.goto(`${YOUTUBE_STUDIO_URL}/channel?hl=en`, {
        waitUntil: "networkidle",
        timeout: 30_000,
      });
      await page.waitForTimeout(5_000);

      // Handle skip link again if it appears
      const skipLink2 = page.locator(
        ':text("SKIP TO YOUTUBE"), :text("PRESKOČIŤ")',
      );
      if ((await skipLink2.count()) > 0) {
        await skipLink2.first().click();
        await page.waitForTimeout(5_000);
      }

      channelId = page.url().match(/channel\/([^/?]+)/)?.[1];
      console.log(`Channel URL: ${page.url()}`);
    }

    if (channelId) {
      console.log(`Found channel ID: ${channelId}`);
      // Go directly to the Live Control Room
      await page.goto(
        `${YOUTUBE_STUDIO_URL}/channel/${channelId}/livestreaming/stream?hl=en`,
        { waitUntil: "networkidle", timeout: 30_000 },
      );
    } else {
      console.log("Could not extract channel ID — staying on current page");
    }

    await page.waitForTimeout(5_000);

    // Handle skip link one more time if we navigated to a new URL
    const skipLink3 = page.locator(
      ':text("SKIP TO YOUTUBE"), :text("PRESKOČIŤ")',
    );
    if ((await skipLink3.count()) > 0) {
      await skipLink3.first().click();
      await page.waitForTimeout(5_000);
    }

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, "03-live-control-room.png"),
      fullPage: true,
    });

    console.log(`Live Control Room URL: ${page.url()}`);

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

        // Log a snippet of the page for debugging (first 1000 chars)
        const snippet = pageContent.replace(/\s+/g, " ").trim().slice(0, 1000);
        console.log(`Page snippet: ${snippet}`);

        // === POSITIVE indicators: stream IS being received ===

        // Check 1: "Go live" button — must be ENABLED (not just visible).
        // The button is always visible on the Live Control Room page, but
        // it's disabled/greyed when no stream data is arriving.  Only when
        // YouTube actually receives stream data does the button become enabled.
        const goLiveButton = page.locator(
          [
            'button:has-text("Go live")',
            'button:has-text("GO LIVE")',
            'button:has-text("Go Live")',
            'button:has-text("naživo")', // Slovak: "Vysielať naživo" = "Go live"
            'button:has-text("Vysielať")', // Slovak: "Broadcast"
            '[aria-label*="Go live"]',
            '[aria-label*="GO LIVE"]',
            '[aria-label*="naživo"]',
          ].join(", "),
        );
        const goLiveCount = await goLiveButton.count();
        if (goLiveCount > 0) {
          for (let i = 0; i < goLiveCount; i++) {
            const btn = goLiveButton.nth(i);
            if ((await btn.isVisible()) && (await btn.isEnabled())) {
              const text = await btn.textContent();
              matchedIndicator = `"Go live" button is visible AND enabled: "${text?.trim()}"`;
              streamReceiving = true;
              break;
            }
          }
        }

        if (streamReceiving) break;

        // Check 2: Stream health indicators — only with specific context.
        // YouTube Studio shows "Excellent", "Good", etc. for stream health,
        // but "Good" alone is too generic.  Use patterns that combine the
        // health word with nearby stream-related context.
        const healthPatterns = [
          /Výborn/i, // Slovak: "Excellent" (Výborný/Výborná) — specific enough
          /stream\s*health.*(?:excellent|good|ok|bad)/i,
          /(?:excellent|good|ok|bad).*stream\s*health/i,
          /Stav\s*streamu.*(?:Výborn|Dobr|OK)/i, // Slovak: "Stream status: ..."
        ];
        for (const pattern of healthPatterns) {
          if (pattern.test(pageContent)) {
            matchedIndicator = `Stream health indicator matched: ${pattern}`;
            streamReceiving = true;
            break;
          }
        }

        if (streamReceiving) break;

        // Check 3: Text patterns that ONLY appear when stream data arrives.
        // IMPORTANT: Do NOT include static UI text like "naživo", "Vysielať",
        // "priamy prenos", "Go live", "stream preview", "live control room" —
        // these are always present on the Live Control Room page even without
        // an active stream and would cause false positives.
        const receivingPatterns = [
          /\d+\s*kbps/i, // Bitrate (e.g., "4500 kbps") — only when stream active
          /\d+p\s+\d+\s*fps/i, // "1080p 30 fps" — only when stream active
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
          /upgrade.*browser/i,
          /unsupported.*browser/i,
          /prehliadač/i, // Slovak: "browser"
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
        await page.reload({ waitUntil: "networkidle", timeout: 30_000 });
        await page.waitForTimeout(5_000);
        // Handle skip link again after reload
        const skipLinkRetry = page.locator(
          ':text("SKIP TO YOUTUBE"), :text("PRESKOČIŤ")',
        );
        if ((await skipLinkRetry.count()) > 0) {
          await skipLinkRetry.first().click();
          await page.waitForTimeout(5_000);
        }
      }
    }

    // Take final screenshot
    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, "final.png"),
      fullPage: true,
    });

    if (EXPECT_NO_STREAM) {
      // BASELINE mode: verify YouTube is NOT receiving
      if (!streamReceiving) {
        console.log("==========================================");
        console.log("  BASELINE OK: YouTube is NOT receiving stream");
        console.log("  (Expected — delivery has not started yet)");
        console.log("==========================================");
      }

      expect(
        streamReceiving,
        `BASELINE FAILED: YouTube Studio shows stream-receiving indicators ` +
          `BEFORE delivery started. Matched: ${matchedIndicator}. ` +
          `This means a stale stream from a previous run is still active, ` +
          `which would make the post-delivery check meaningless. ` +
          `Screenshots saved to ${SCREENSHOT_DIR} for debugging.`,
      ).toBe(false);
    } else {
      // NORMAL mode: verify YouTube IS receiving
      if (streamReceiving) {
        console.log("==========================================");
        console.log("  YOUTUBE IS RECEIVING THE STREAM!");
        console.log(`  Indicator: ${matchedIndicator}`);
        console.log("  (Broadcast in testing state — auto-start is banned)");
        console.log("==========================================");
      }

      expect(
        streamReceiving,
        `FAILED: YouTube Studio does not show stream is being received after ${MAX_RETRIES} attempts. ` +
          `Last error: ${lastError}. ` +
          `The delivering server confirmed chunk progression (previous CI step), ` +
          `but YouTube Studio Live Control Room does not show stream-receiving indicators. ` +
          `Screenshots saved to ${SCREENSHOT_DIR} for debugging.`,
      ).toBe(true);
    }
  } finally {
    await context.close();
  }
});
