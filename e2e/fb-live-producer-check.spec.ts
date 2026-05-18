import { test, expect, chromium, Page } from "@playwright/test";
import * as path from "path";
import * as os from "os";
import * as fs from "fs";

/**
 * Facebook Live Producer stream-receiving verification.
 *
 * Architectural twin of `youtube-studio-check.spec.ts`. Uses a persistent
 * Chrome profile with a saved Facebook session to open the configured FB
 * Live Producer broadcast and poll for the three signals that prove FB
 * is receiving our rust-pusher feed:
 *
 *   1. A `<video>` element exists, `readyState >= 3` (HAVE_FUTURE_DATA),
 *      and `currentTime` advances between polls (preview is playing).
 *   2. A non-empty stream-health label that is NOT a known error state
 *      ("No signal", "Connecting", "Disconnected").
 *   3. A bitrate readout matching `\d+ kbps` with a non-zero value.
 *
 * All three must hold for `SOAK_MINUTES` continuous minutes, polled every
 * `POLL_INTERVAL_MS` milliseconds. Any single failure during the soak
 * fails the test loud. No retry, no flake-tolerance, per `test-strictness.md`.
 *
 * Setup (one-time, on stream.lan via MCP `win-stream-snv`):
 *   pwsh.exe -File C:\restreamer\scripts\setup-fb-profile.ps1
 *   -> a HEADED Chromium opens
 *   -> operator signs into Facebook with the dedicated test-account
 *   -> close the browser; session is saved to PROFILE_DIR
 *
 * CI runs in headless mode using the saved session automatically.
 */

const PROFILE_DIR =
  process.env.FB_PROFILE_DIR ||
  (os.platform() === "win32"
    ? "C:\\Users\\newlevel\\.playwright-fb-profile"
    : path.join(os.homedir(), ".playwright-fb-profile"));

const SCREENSHOT_DIR =
  process.env.FB_SCREENSHOT_DIR ||
  (os.platform() === "win32"
    ? "C:\\Users\\newlevel\\.playwright-fb-screenshots"
    : path.join(os.homedir(), ".playwright-fb-screenshots"));

const FB_BROADCAST_URL =
  process.env.FB_BROADCAST_URL ||
  "https://www.facebook.com/live/producer";

const SOAK_MINUTES = parseInt(process.env.FB_SOAK_MINUTES || "30", 10);
const POLL_INTERVAL_MS = parseInt(
  process.env.FB_POLL_INTERVAL_MS || "60000",
  10,
);
const SCREENSHOT_MINUTES = [0, 5, 15, 30];

const BANNED_HEALTH_PATTERNS = /no signal|connecting|disconnected|offline/i;

async function readHealthSnapshot(page: Page): Promise<{
  videoCurrentTime: number;
  videoReadyState: number;
  healthLabel: string;
  bitrateKbps: number;
}> {
  // Preview <video> element
  const videoLocator = page.locator("video").first();
  await videoLocator.waitFor({ state: "attached", timeout: 30_000 });
  const videoCurrentTime = await videoLocator.evaluate(
    (v: HTMLVideoElement) => v.currentTime,
  );
  const videoReadyState = await videoLocator.evaluate(
    (v: HTMLVideoElement) => v.readyState,
  );

  // Health label (FB renders this with various wrappers; selector list
  // is intentionally broad and tuned during T5 against the real DOM).
  const healthLocator = page
    .locator(
      [
        '[data-testid="live-producer-stream-health"]',
        '[data-testid="stream-health"]',
        '[aria-label*="stream health" i]',
        '[aria-label*="ingest" i]',
        'div:has-text("Stream Health")',
      ].join(", "),
    )
    .first();
  const healthLabel = ((await healthLocator.textContent()) || "").trim();

  // Bitrate readout — match the first "<number> kbps" text on page.
  const bitrateText =
    (await page.locator("text=/\\d+\\s*kbps/i").first().textContent()) || "";
  const bitrateMatch = bitrateText.match(/(\d+)\s*kbps/i);
  const bitrateKbps = bitrateMatch ? parseInt(bitrateMatch[1], 10) : 0;

  return { videoCurrentTime, videoReadyState, healthLabel, bitrateKbps };
}

test(`FB Live Producer receives rust-pusher feed for ${SOAK_MINUTES} min`, async () => {
  const headed = !!process.env.HEADED;

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
      "--disable-infobars",
      "--disable-dev-shm-usage",
      "--disable-backgrounding-occluded-windows",
      "--disable-renderer-backgrounding",
    ],
    viewport: { width: 1280, height: 720 },
    timeout: 60_000,
    ignoreDefaultArgs: ["--enable-automation"],
  });

  const page = context.pages()[0] || (await context.newPage());

  // Collect console errors throughout the soak per `browser-console-zero-errors.md`.
  const consoleErrors: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() === "error" || msg.type() === "warning") {
      consoleErrors.push(`[${msg.type()}] ${msg.text()}`);
    }
  });

  try {
    await page.goto(FB_BROADCAST_URL, {
      waitUntil: "networkidle",
      timeout: 60_000,
    });

    if (page.url().includes("/login")) {
      throw new Error(
        "FB session expired or missing. Operator must rerun setup-fb-profile.ps1.",
      );
    }

    // Initial settle for the FB SPA.
    await page.waitForTimeout(5_000);
    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, `00-initial-load.png`),
      fullPage: true,
    });

    let prevVideoTime = -1;
    const startMs = Date.now();
    const endMs = startMs + SOAK_MINUTES * 60 * 1000;
    let pollIdx = 0;
    let nextScreenshotIdx = 0;

    while (Date.now() < endMs) {
      const elapsedMin = Math.floor((Date.now() - startMs) / 60000);

      const snap = await readHealthSnapshot(page);

      // Optional screenshot at minute boundaries.
      while (
        nextScreenshotIdx < SCREENSHOT_MINUTES.length &&
        elapsedMin >= SCREENSHOT_MINUTES[nextScreenshotIdx]
      ) {
        await page.screenshot({
          path: path.join(
            SCREENSHOT_DIR,
            `min-${String(SCREENSHOT_MINUTES[nextScreenshotIdx]).padStart(2, "0")}.png`,
          ),
          fullPage: true,
        });
        nextScreenshotIdx += 1;
      }

      // Assertions — any failure kills the test loud.
      expect(
        snap.videoReadyState,
        `poll ${pollIdx} (${elapsedMin} min): videoReadyState must be >= 3 (HAVE_FUTURE_DATA), got ${snap.videoReadyState}`,
      ).toBeGreaterThanOrEqual(3);

      if (prevVideoTime >= 0) {
        expect(
          snap.videoCurrentTime,
          `poll ${pollIdx} (${elapsedMin} min): video currentTime did not advance (prev=${prevVideoTime}, now=${snap.videoCurrentTime})`,
        ).toBeGreaterThan(prevVideoTime);
      }
      prevVideoTime = snap.videoCurrentTime;

      expect(
        snap.healthLabel.length,
        `poll ${pollIdx} (${elapsedMin} min): empty health label`,
      ).toBeGreaterThan(0);

      expect(
        snap.healthLabel,
        `poll ${pollIdx} (${elapsedMin} min): banned health state "${snap.healthLabel}"`,
      ).not.toMatch(BANNED_HEALTH_PATTERNS);

      expect(
        snap.bitrateKbps,
        `poll ${pollIdx} (${elapsedMin} min): bitrate not positive (got ${snap.bitrateKbps} kbps)`,
      ).toBeGreaterThan(0);

      pollIdx += 1;
      const sleepUntil = startMs + pollIdx * POLL_INTERVAL_MS;
      const sleepMs = Math.max(0, sleepUntil - Date.now());
      if (sleepMs > 0) {
        await page.waitForTimeout(sleepMs);
      }
    }

    // Zero console errors over the full soak.
    expect(
      consoleErrors,
      `FB Live Producer produced console errors/warnings during soak: ${consoleErrors.join(" | ")}`,
    ).toEqual([]);
  } finally {
    await page.screenshot({
      path: path.join(SCREENSHOT_DIR, "99-final.png"),
      fullPage: true,
    });
    await context.close();
  }
});
