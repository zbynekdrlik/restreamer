import { test, expect, chromium, type Browser } from "@playwright/test";
import { spawn, type ChildProcess } from "child_process";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// Chrome's own warnings (deprecations, interventions, failed loads) go to
// the CDP Log domain, not to `console`, so page.on("console") never sees
// them. On stream-pp 0.29.49 the dashboard logged a deprecated
// `apple-mobile-web-app-capable` meta warning that every E2E missed.
//
// The browser is plain Google Chrome stable, started here and driven over
// CDP: neither Playwright's bundled Chromium (145) nor Chrome launched by
// Playwright (its default flags) logs that deprecation, Chrome started as
// the boxes start it does. ubuntu-latest ships Chrome; without it this fails.
const CHROME = process.env.CHROME_BIN ?? "google-chrome";

async function startChrome(): Promise<{
  browser: Browser;
  proc: ChildProcess;
  dir: string;
}> {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "browser-log-"));
  const proc = spawn(
    CHROME,
    [
      "--headless=new",
      "--remote-debugging-port=0",
      `--user-data-dir=${dir}`,
      "--no-first-run",
      // Without a desktop session the first request otherwise waits ~25 s
      // (the D-Bus timeout) for a keyring to encrypt cookies with.
      "--password-store=basic",
      "about:blank",
    ],
    { stdio: "ignore" },
  );
  const spawnError = new Promise<never>((_, reject) =>
    proc.once("error", (e) => reject(new Error(`${CHROME}: ${e.message}`))),
  );
  // Chrome writes the port it picked to DevToolsActivePort.
  const portFile = path.join(dir, "DevToolsActivePort");
  const port = await Promise.race([
    spawnError,
    (async () => {
      for (let i = 0; i < 100; i++) {
        if (fs.existsSync(portFile)) {
          const first = fs.readFileSync(portFile, "utf-8").split("\n")[0];
          if (first) return first;
        }
        await new Promise((r) => setTimeout(r, 100));
      }
      throw new Error(`${CHROME} wrote no DevToolsActivePort within 10 s`);
    })(),
  ]);
  const browser = await chromium.connectOverCDP(`http://127.0.0.1:${port}`);
  return { browser, proc, dir };
}

test.describe("Browser log", () => {
  test.beforeEach(async ({ request }) => {
    await request.post("http://127.0.0.1:8910/api/v1/__reset");
  });

  test("the dashboard makes Chrome log no warning or error", async () => {
    const { browser, proc, dir } = await startChrome();
    try {
      const context = browser.contexts()[0];
      await context.addInitScript(tauriMockScript);
      const page = context.pages()[0] ?? (await context.newPage());
      const entries: string[] = [];
      page.on("console", (msg) => {
        if (msg.type() === "error" || msg.type() === "warning") {
          entries.push(`[console ${msg.type()}] ${msg.text()}`);
        }
      });
      const cdp = await context.newCDPSession(page);
      cdp.on("Log.entryAdded", ({ entry }) => {
        if (entry.level === "warning" || entry.level === "error") {
          entries.push(`[log ${entry.level} ${entry.source}] ${entry.text}`);
        }
      });
      await cdp.send("Log.enable");

      for (const route of ["/", "/settings"]) {
        await page.goto(`http://127.0.0.1:8910${route}`);
        await expect(page.locator('[data-testid="version"]')).toBeVisible();
      }

      expect(entries).toEqual([]);
    } finally {
      // Browser.close lets every Chrome process finish writing the profile.
      const exited = new Promise((r) => proc.once("exit", r));
      const session = await browser.newBrowserCDPSession();
      await session.send("Browser.close").catch(() => {});
      await exited;
      fs.rmSync(dir, {
        recursive: true,
        force: true,
        maxRetries: 10,
        retryDelay: 200,
      });
    }
  });
});
