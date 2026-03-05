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
 * by checking the "Stream" tab of the Live Control Room for:
 *   - Stream health indicators ("Excellent", "Good", "OK", bitrate, fps)
 *   - "Go live" button state (enabled when stream data is received)
 *   - Stream preview content
 *
 * IMPORTANT: YouTube Studio is a heavy SPA with custom web components
 * (ytcp-button, etc.) and Shadow DOM.  It has three tabs:
 *   - "Stream" / "Prenos" — live preview + health (what we need)
 *   - "Webcam" / "Webkamera"
 *   - "Manage" / "Správa" — list of streams (NOT useful for detection)
 * YouTube may redirect /livestreaming/stream to /livestreaming/manage,
 * so we explicitly click the "Stream" tab after navigation.
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
    const skipLink = page.locator(
      [
        'a:has-text("SKIP TO YOUTUBE STUDIO")',
        'a:has-text("Skip to YouTube Studio")',
        'a:has-text("PRESKOČIŤ NA ŠTÚDIO YOUTUBE")',
        'button:has-text("SKIP TO YOUTUBE STUDIO")',
        'button:has-text("PRESKOČIŤ NA ŠTÚDIO YOUTUBE")',
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
    console.log(`Current URL after Studio load: ${page.url()}`);

    // Try to extract channel ID from current URL
    let channelId = page.url().match(/channel\/([^/?]+)/)?.[1];

    if (!channelId) {
      await page.goto(`${YOUTUBE_STUDIO_URL}/channel?hl=en`, {
        waitUntil: "networkidle",
        timeout: 30_000,
      });
      await page.waitForTimeout(5_000);

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
      // Go to the livestreaming section
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

    // YouTube Studio may redirect /stream to /manage.  The "Manage" tab shows
    // a list of streams with no health data.  We need the "Stream" tab which
    // shows the live preview and health indicators.  Explicitly click it.
    console.log(`URL after navigation: ${page.url()}`);
    const streamTab = page.locator(
      [
        // English
        'a:has-text("Stream")',
        'div[role="tab"]:has-text("Stream")',
        // Slovak
        'a:has-text("Prenos")',
        'div[role="tab"]:has-text("Prenos")',
        // Try paper-tab / ytcp-tab (YouTube custom elements)
        'paper-tab:has-text("Stream")',
        'paper-tab:has-text("Prenos")',
      ].join(", "),
    );

    const streamTabCount = await streamTab.count();
    console.log(`Found ${streamTabCount} 'Stream/Prenos' tab elements`);
    if (streamTabCount > 0) {
      console.log("Clicking 'Stream/Prenos' tab to switch to stream view...");
      await streamTab.first().click();
      await page.waitForTimeout(5_000);
      console.log(`URL after tab click: ${page.url()}`);
    }

    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, "03-live-control-room.png"),
      fullPage: true,
    });

    console.log(`Live Control Room URL: ${page.url()}`);

    // Retry loop: look for stream-receiving indicators (testing state).
    // Since auto-start is BANNED, YouTube won't show "LIVE" — instead we
    // look for evidence that the stream data is being received on the
    // "Stream" tab of the Live Control Room.
    let streamReceiving = false;
    let lastError = "";
    let matchedIndicator = "";

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      console.log(
        `--- Attempt ${attempt}/${MAX_RETRIES}: checking for stream receiving ---`,
      );

      try {
        await page.screenshot({
          path: path.join(SCREENSHOT_DIR, `attempt-${attempt}.png`),
          fullPage: true,
        });

        console.log(`Page URL: ${page.url()}`);

        // Use Playwright's built-in locators which handle Shadow DOM and
        // custom web components better than raw querySelectorAll.

        // Check 1: Look for "Go live" / "Vysielať naživo" button using
        // Playwright locator (handles ytcp-button, paper-button, etc.)
        const goLiveBtn = page.locator(
          [
            'button:has-text("Go live")',
            'button:has-text("Vysielať naživo")',
            // YouTube custom button elements
            ':has-text("Go live"):visible',
          ].join(", "),
        );

        const goLiveBtnCount = await goLiveBtn.count();
        console.log(`"Go live" buttons found: ${goLiveBtnCount}`);

        for (let i = 0; i < goLiveBtnCount; i++) {
          const btn = goLiveBtn.nth(i);
          const btnText = await btn.textContent();
          const isDisabled = await btn.evaluate((el) => {
            // Check multiple ways an element can be disabled
            const htmlEl = el as HTMLElement;
            return (
              (htmlEl as HTMLButtonElement).disabled ||
              htmlEl.getAttribute("aria-disabled") === "true" ||
              htmlEl.classList.contains("disabled") ||
              htmlEl.hasAttribute("disabled")
            );
          });
          const isVisible = await btn.isVisible();
          console.log(
            `  GoLive button ${i}: text="${btnText?.trim()}", disabled=${isDisabled}, visible=${isVisible}`,
          );

          // The button being enabled (not disabled) means YouTube is
          // receiving stream data and is ready for "Go live"
          if (isVisible && !isDisabled) {
            matchedIndicator = `"Go live" button is enabled: "${btnText?.trim()}"`;
            streamReceiving = true;
            break;
          }
        }

        if (streamReceiving) break;

        // Check 2: Deep DOM text inspection for stream health indicators.
        // These only appear when YouTube is actually processing stream data.
        const domInfo = await page.evaluate(() => {
          const SKIP_TAGS = new Set(["SCRIPT", "STYLE", "NOSCRIPT", "SVG"]);
          function getDeepText(node: Node): string {
            let text = "";
            if (node.nodeType === Node.TEXT_NODE) {
              const t = (node.textContent || "").trim();
              if (t) text += t + " ";
            }
            if (node instanceof HTMLElement && SKIP_TAGS.has(node.tagName)) {
              return text;
            }
            if (node instanceof HTMLElement && node.shadowRoot) {
              text += getDeepText(node.shadowRoot);
            }
            for (const child of node.childNodes) {
              text += getDeepText(child);
            }
            return text;
          }

          const allText = getDeepText(document.body);

          // Also get innerHTML of specific YouTube Studio elements
          // that might contain stream health data
          const streamPreview =
            document.querySelector("ytcp-live-streaming-stream-preview")
              ?.textContent || "";
          const healthInfo =
            document.querySelector("ytcp-live-streaming-stream-health")
              ?.textContent || "";
          const streamStatus =
            document.querySelector("ytcp-live-streaming-stream-status")
              ?.textContent || "";

          // Get all clickable/button-like elements
          const allClickable = Array.from(
            document.querySelectorAll(
              'button, [role="button"], ytcp-button, paper-button, [aria-role="button"]',
            ),
          ).map((el) => ({
            tag: el.tagName.toLowerCase(),
            text: (el.textContent || "").trim().substring(0, 100),
            disabled:
              (el as HTMLButtonElement).disabled ||
              el.getAttribute("aria-disabled") === "true" ||
              el.hasAttribute("disabled"),
            visible:
              (el as HTMLElement).offsetParent !== null &&
              getComputedStyle(el as HTMLElement).display !== "none",
          }));

          return {
            textLength: allText.length,
            textSnippet: allText.replace(/\s+/g, " ").substring(0, 5000),
            streamPreview: streamPreview.trim().substring(0, 500),
            healthInfo: healthInfo.trim().substring(0, 500),
            streamStatus: streamStatus.trim().substring(0, 500),
            clickableElements: allClickable.filter((e) => e.visible),
          };
        });

        console.log(`Deep text length: ${domInfo.textLength} chars`);
        console.log(
          `Visible text (first 3000): ${domInfo.textSnippet.substring(0, 3000)}`,
        );
        console.log(`Stream preview element: "${domInfo.streamPreview}"`);
        console.log(`Health info element: "${domInfo.healthInfo}"`);
        console.log(`Stream status element: "${domInfo.streamStatus}"`);
        console.log(
          `Clickable elements (${domInfo.clickableElements.length}):`,
        );
        for (const el of domInfo.clickableElements) {
          if (
            el.text.toLowerCase().includes("live") ||
            el.text.toLowerCase().includes("naživo") ||
            el.text.toLowerCase().includes("vysielať") ||
            el.text.toLowerCase().includes("stream") ||
            el.text.toLowerCase().includes("prenos")
          ) {
            console.log(
              `  [${el.disabled ? "DISABLED" : "ENABLED"}] <${el.tag}> "${el.text}"`,
            );
          }
        }

        const deepText = domInfo.textSnippet;

        // Check health patterns in deep text
        const receivingPatterns = [
          /\d+\s*kbps/i, // Bitrate (e.g., "4500 kbps")
          /\d+p\s+\d+\s*fps/i, // "1080p 30 fps"
          /stream\s*health.*(?:excellent|good|ok|bad)/i, // English health
          /Výborn/i, // Slovak: "Excellent" stream health
          /Stav\s*streamu/i, // Slovak: "Stream status"
          /Kvalita\s*streamu/i, // Slovak: "Stream quality"
        ];
        for (const pattern of receivingPatterns) {
          if (pattern.test(deepText)) {
            matchedIndicator = `Text pattern matched in deep DOM: ${pattern}`;
            streamReceiving = true;
            break;
          }
        }

        if (streamReceiving) break;

        // Check 3: YouTube custom web component content
        if (
          domInfo.streamPreview &&
          domInfo.streamPreview !== "" &&
          !domInfo.streamPreview.match(/^[\s]*$/)
        ) {
          matchedIndicator = `Stream preview element has content: "${domInfo.streamPreview.substring(0, 100)}"`;
          streamReceiving = true;
        }
        if (streamReceiving) break;

        if (
          domInfo.healthInfo &&
          domInfo.healthInfo !== "" &&
          !domInfo.healthInfo.match(/^[\s]*$/)
        ) {
          matchedIndicator = `Health info element has content: "${domInfo.healthInfo.substring(0, 100)}"`;
          streamReceiving = true;
        }
        if (streamReceiving) break;

        // Check 4: Enabled "Go live" style clickable elements (custom components)
        for (const el of domInfo.clickableElements) {
          const text = el.text.toLowerCase();
          if (
            (text.includes("go live") || text.includes("vysielať naživo")) &&
            !el.disabled
          ) {
            matchedIndicator = `Enabled clickable <${el.tag}>: "${el.text}"`;
            streamReceiving = true;
            break;
          }
        }

        if (streamReceiving) break;

        lastError = `No stream-receiving indicators found (attempt ${attempt})`;
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
        // Re-click Stream tab after reload
        const streamTabRetry = page.locator(
          [
            'a:has-text("Stream")',
            'div[role="tab"]:has-text("Stream")',
            'a:has-text("Prenos")',
            'div[role="tab"]:has-text("Prenos")',
            'paper-tab:has-text("Stream")',
            'paper-tab:has-text("Prenos")',
          ].join(", "),
        );
        if ((await streamTabRetry.count()) > 0) {
          await streamTabRetry.first().click();
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
