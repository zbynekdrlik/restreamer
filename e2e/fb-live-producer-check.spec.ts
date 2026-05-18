import { test, expect, chromium, Page } from "@playwright/test";
import * as path from "path";
import * as os from "os";
import * as fs from "fs";

/**
 * Facebook Live Producer stream-receiving verification.
 *
 * Architectural twin of `youtube-studio-check.spec.ts`. Uses a persistent
 * Chrome profile with a saved Facebook session to open the configured FB
 * Live Producer broadcast and poll for the signals that prove FB is
 * receiving our rust-pusher feed.
 *
 * `FB_BROADCAST_URL` must point at a Page's Live Producer
 * (`https://www.facebook.com/live/producer/<page-id>`). The default
 * `/live/producer` URL redirects to the LOGGED-IN account's PERSONAL
 * profile target, where the broadcast does NOT appear regardless of
 * what the rust pusher sends — FB routes persistent-key streams to
 * the Page that minted the key.
 *
 * Required signals (assertions every poll):
 *   - `<video>` element present in DOM
 *   - `video.readyState >= 3` (HAVE_FUTURE_DATA) — FB has buffered frames
 *   - `video.videoWidth > 0` and `video.videoHeight > 0` — FB decoded codec
 *   - `video.paused === false` — preview is playing
 *   - `video.currentTime` strictly advances between consecutive polls
 *
 * Notes deliberately NOT asserted:
 *   - Stream-health badges, bitrate readouts, "Receiving"/"Live" labels:
 *     these only render once the broadcast is "Live" (publicly aired).
 *     Auto-Go-Live is banned because FB destroys the broadcast on stop,
 *     making subsequent CI runs fail — see `youtube-studio-check.spec.ts`
 *     for the same constraint on YT.
 *
 * Setup (one-time, on stream.lan via MCP `win-stream-snv`):
 *   pwsh.exe -File C:\restreamer\scripts\setup-fb-profile.ps1
 *   -> a HEADED Chromium opens
 *   -> operator signs into FB with the account that admins the Page
 *      that minted the persistent stream key
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
  process.env.FB_BROADCAST_URL || "https://www.facebook.com/live/producer";

const SOAK_MINUTES = parseInt(process.env.FB_SOAK_MINUTES || "30", 10);
const POLL_INTERVAL_MS = parseInt(
  process.env.FB_POLL_INTERVAL_MS || "60000",
  10,
);
const SCREENSHOT_MINUTES = [0, 5, 15, 30];

interface FbVideoSnapshot {
  videoCurrentTime: number;
  videoReadyState: number;
  videoWidth: number;
  videoHeight: number;
  videoPaused: boolean;
}

async function readVideoSnapshot(page: Page): Promise<FbVideoSnapshot> {
  const videoLocator = page.locator("video").first();
  await videoLocator.waitFor({ state: "attached", timeout: 30_000 });
  return await videoLocator.evaluate((v: HTMLVideoElement) => ({
    videoCurrentTime: v.currentTime,
    videoReadyState: v.readyState,
    videoWidth: v.videoWidth,
    videoHeight: v.videoHeight,
    videoPaused: v.paused,
  }));
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
  // Filter out known FB-internal noise that does NOT reflect a fault on our
  // rust pusher or our stream-delivery pipeline:
  //   - "Permissions policy violation: unload is not allowed" — FB's own
  //     SDKs still call addEventListener("unload") under their own
  //     Permissions-Policy ban, so Chrome logs a violation. Not ours.
  //   - WebSocket failures to `gateway.facebook.com/ws/...` — FB Live Producer
  //     opens auxiliary realtime channels (rpsignaling / streamcontroller /
  //     realtime / lightspeed) that fail to resolve / connect from headless
  //     Chrome (no DNS for the regional gateway hostnames). The stream
  //     itself is served separately and is unaffected; video.currentTime
  //     continues to advance even with all four WS endpoints down.
  const FB_INTERNAL_NOISE_PATTERNS = [
    /Permissions policy violation: unload is not allowed/i,
    /WebSocket connection to 'wss:\/\/gateway\.facebook\.com\/ws\//i,
    /WebSocket is closed before the connection is established/i,
  ];
  const consoleErrors: string[] = [];
  page.on("console", (msg) => {
    if (msg.type() !== "error" && msg.type() !== "warning") return;
    const text = msg.text();
    if (FB_INTERNAL_NOISE_PATTERNS.some((p) => p.test(text))) return;
    consoleErrors.push(`[${msg.type()}] ${text}`);
  });

  try {
    await page.goto(FB_BROADCAST_URL, {
      waitUntil: "domcontentloaded",
      timeout: 60_000,
    });

    if (page.url().includes("/login")) {
      throw new Error(
        "FB session expired or missing. Operator must rerun setup-fb-profile.ps1.",
      );
    }

    // Initial settle for the FB SPA (heavy React tree + WebRTC preview).
    await page.waitForTimeout(8_000);
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

      const snap = await readVideoSnapshot(page);

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

      expect(
        snap.videoReadyState,
        `poll ${pollIdx} (${elapsedMin} min): videoReadyState must be >= 3 (HAVE_FUTURE_DATA), got ${snap.videoReadyState}`,
      ).toBeGreaterThanOrEqual(3);

      expect(
        snap.videoWidth,
        `poll ${pollIdx} (${elapsedMin} min): videoWidth must be > 0, got ${snap.videoWidth}`,
      ).toBeGreaterThan(0);

      expect(
        snap.videoHeight,
        `poll ${pollIdx} (${elapsedMin} min): videoHeight must be > 0, got ${snap.videoHeight}`,
      ).toBeGreaterThan(0);

      expect(
        snap.videoPaused,
        `poll ${pollIdx} (${elapsedMin} min): video.paused must be false`,
      ).toBe(false);

      if (prevVideoTime >= 0) {
        expect(
          snap.videoCurrentTime,
          `poll ${pollIdx} (${elapsedMin} min): video currentTime did not advance (prev=${prevVideoTime}, now=${snap.videoCurrentTime})`,
        ).toBeGreaterThan(prevVideoTime);
      }
      prevVideoTime = snap.videoCurrentTime;

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
