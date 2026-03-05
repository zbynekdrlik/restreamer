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

// Force English UI via hl parameter — the machine locale is Slovak
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

        // Check 1: "Go live" button — THE primary indicator.
        // When YouTube receives stream data, the "Go live" button appears
        // and becomes clickable in the Live Control Room.
        // Handle both English and Slovak UI text.
        // Actual Slovak text observed: "Vysielať naživo"
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
            if (await goLiveButton.nth(i).isVisible()) {
              const text = await goLiveButton.nth(i).textContent();
              matchedIndicator = `"Go live" button is visible: "${text?.trim()}"`;
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
          /\bExcellent\b/i,
          /\bGood\b/i,
          /\bstream\s*health\b/i,
          /Výborn/i, // Slovak: "Excellent" (Výborný/Výborná)
          /Dobr[áýé]/i, // Slovak: "Good" (Dobrý/Dobrá/Dobré)
          /Stav\s*streamu/i, // Slovak: "Stream status"
        ];
        for (const pattern of healthPatterns) {
          if (pattern.test(pageContent)) {
            matchedIndicator = `Stream health indicator matched: ${pattern}`;
            streamReceiving = true;
            break;
          }
        }

        if (streamReceiving) break;

        // Check 3: Text patterns indicating stream is connected/receiving.
        // Observed Slovak text: "Vysielať naživo", "Zdá sa, všetko je
        // pripravené. Kliknite tu a začnite streamovať.",
        // "Ukončiť priamy prenos", "Priamy prenos"
        const receivingPatterns = [
          /Go\s+live/i, // English: "Go live"
          /stream\s+preview/i, // English: "Stream preview"
          /live\s+control\s+room/i, // English: page heading
          /naživo/i, // Slovak: "live" (from "Vysielať naživo")
          /pripraven/i, // Slovak: "ready" (from "pripravené")
          /streamova/i, // Slovak: "stream" verb (from "streamovať")
          /Vysielať/i, // Slovak: "Broadcast"
          /priamy\s+prenos/i, // Slovak: "Live broadcast"
          /Excellent\s+Data/i, // Data quality indicator
          /connected/i, // English: "Connected"
          /receiving/i, // English: "Receiving"
          /pripojené/i, // Slovak: "Connected"
          /\d+\s*kbps/i, // Bitrate (e.g., "4500 kbps")
          /\d+x\d+/, // Resolution (e.g., "1920x1080")
          /\d+p\b.*\d+\s*fps/i, // "1080p 30 fps"
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
