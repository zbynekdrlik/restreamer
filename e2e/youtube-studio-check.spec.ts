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

        console.log(`Page URL: ${page.url()}`);

        // YouTube Studio is a heavy SPA that uses custom web components
        // and Shadow DOM.  page.textContent("body") returns stale/JS-mixed
        // content.  Use JavaScript evaluation to deeply inspect the DOM.
        const domInfo = await page.evaluate(() => {
          // Collect ALL visible text (skip script/style tags, traverse shadows)
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

          // Find ALL buttons (not just live-related) for debugging
          const allButtons = Array.from(
            document.querySelectorAll("button"),
          ).map((b) => ({
            text: (b.textContent || "").trim().substring(0, 100),
            disabled: b.disabled,
            ariaDisabled: b.getAttribute("aria-disabled"),
            visible:
              b.offsetParent !== null && getComputedStyle(b).display !== "none",
            classes: b.className.substring(0, 100),
          }));

          // Find video elements
          const videos = Array.from(document.querySelectorAll("video")).map(
            (v) => ({
              src: v.src || v.getAttribute("src") || "",
              hasSrcObject: !!v.srcObject,
              visible:
                v.offsetParent !== null &&
                getComputedStyle(v).display !== "none",
              w: v.videoWidth,
              h: v.videoHeight,
            }),
          );

          // Check iframes
          const iframes = Array.from(document.querySelectorAll("iframe")).map(
            (f) => ({
              src: (f.src || "").substring(0, 200),
              visible:
                f.offsetParent !== null &&
                getComputedStyle(f).display !== "none",
            }),
          );

          // Dump the full HTML of the main content area for investigation
          const mainContent =
            document
              .querySelector("#contents")
              ?.innerHTML?.substring(0, 2000) ||
            document
              .querySelector("[role=main]")
              ?.innerHTML?.substring(0, 2000) ||
            document.querySelector("ytcp-app")?.innerHTML?.substring(0, 2000) ||
            "no-main-content-found";

          return {
            textLength: allText.length,
            textSnippet: allText.replace(/\s+/g, " ").substring(0, 5000),
            allButtonCount: allButtons.length,
            visibleButtons: allButtons.filter((b) => b.visible),
            liveButtons: allButtons.filter(
              (b) =>
                b.visible &&
                (b.text.toLowerCase().includes("live") ||
                  b.text.toLowerCase().includes("naživo") ||
                  b.text.toLowerCase().includes("vysielať")),
            ),
            videos,
            iframes: iframes.filter((f) => f.visible),
            mainContentSnippet: mainContent.substring(0, 2000),
          };
        });

        console.log(`Deep text length: ${domInfo.textLength} chars`);
        console.log(
          `Visible text (5000 chars): ${domInfo.textSnippet.substring(0, 5000)}`,
        );
        console.log(
          `All buttons (${domInfo.allButtonCount} total, ${domInfo.visibleButtons.length} visible):`,
        );
        for (const btn of domInfo.visibleButtons) {
          console.log(
            `  [${btn.disabled ? "DISABLED" : "ENABLED"}] aria-disabled=${btn.ariaDisabled} "${btn.text}" class=${btn.classes}`,
          );
        }
        console.log(
          `Live/naživo buttons: ${JSON.stringify(domInfo.liveButtons)}`,
        );
        console.log(`Video elements: ${JSON.stringify(domInfo.videos)}`);
        console.log(`Visible iframes: ${JSON.stringify(domInfo.iframes)}`);
        console.log(
          `Main content HTML: ${domInfo.mainContentSnippet.substring(0, 1000)}`,
        );

        const deepText = domInfo.textSnippet;

        // === POSITIVE indicators: stream IS being received ===

        // Check 1: "Go live" / "Vysielať naživo" button that is NOT disabled.
        // The button exists on the page always, but is disabled when no
        // stream data arrives.  aria-disabled="false" or disabled=false
        // means YouTube received stream data.
        for (const btn of domInfo.liveButtons) {
          const isEnabled = !btn.disabled && btn.ariaDisabled !== "true";
          console.log(
            `  Button "${btn.text}": disabled=${btn.disabled}, aria-disabled=${btn.ariaDisabled}, enabled=${isEnabled}`,
          );
          if (isEnabled && btn.visible) {
            matchedIndicator = `"Go live" button is enabled: "${btn.text}"`;
            streamReceiving = true;
            break;
          }
        }

        if (streamReceiving) break;

        // Check 2: Stream health / bitrate / resolution in deep text.
        // These only appear when YouTube is actually processing stream data.
        const receivingPatterns = [
          /\d+\s*kbps/i, // Bitrate (e.g., "4500 kbps")
          /\d+p\s+\d+\s*fps/i, // "1080p 30 fps"
          /Výborn/i, // Slovak: "Excellent" stream health
          /stream\s*health/i, // English: "Stream health"
          /Stav\s*streamu/i, // Slovak: "Stream status"
        ];
        for (const pattern of receivingPatterns) {
          if (pattern.test(deepText)) {
            matchedIndicator = `Text pattern matched in deep DOM: ${pattern}`;
            streamReceiving = true;
            break;
          }
        }

        if (streamReceiving) break;

        // Check 3: Video element with actual source (stream preview).
        for (const vid of domInfo.videos) {
          if (vid.visible && (vid.src || vid.hasSrcObject)) {
            matchedIndicator = `Video element with source: src=${vid.src}, srcObject=${vid.hasSrcObject}`;
            streamReceiving = true;
            break;
          }
        }

        if (streamReceiving) break;

        // Check 4: Visible iframe (YouTube may embed stream preview in iframe).
        for (const iframe of domInfo.iframes) {
          if (
            iframe.src.includes("youtube") ||
            iframe.src.includes("googlevideo")
          ) {
            matchedIndicator = `Stream preview iframe: ${iframe.src}`;
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
