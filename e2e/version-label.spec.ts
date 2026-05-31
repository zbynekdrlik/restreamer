import { test, expect } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";

// Inject Tauri mock before each page navigation (matches frontend.spec.ts).
const tauriMockScript = fs.readFileSync(
  path.join(__dirname, "tauri-mock.js"),
  "utf-8",
);

// Foundation gate per ~/devel/airuleset/modules/quality/version-on-dashboard.md:
// every web dashboard MUST display a visible version label. Without it,
// post-deploy verification cannot confirm new code is live and
// frontend/backend drift ships silently.
//
// The build-time injection comes from the BUILD_VERSION env var read by
// `leptos-ui/src/components/header.rs::header()` via `option_env!`. In CI
// the trunk build sets BUILD_VERSION from the workspace version; locally
// (no env) it falls back to "dev".
test.describe("Dashboard version label", () => {
  test.beforeEach(async ({ page, request }) => {
    await page.addInitScript(tauriMockScript);
    await request.post("http://127.0.0.1:8910/api/v1/__reset");
  });

  test("visible on every route, matches v<semver>(-dev.N)? format or 'dev' fallback", async ({
    page,
  }) => {
    for (const route of ["/", "/settings"]) {
      await page.goto(route);
      const versionLocator = page.locator('[data-testid="version"]');
      await expect(versionLocator).toBeVisible();
      const text = (await versionLocator.textContent())?.trim() ?? "";
      // Accept all real BUILD_VERSION shapes we ship:
      //   * "dev"                       — local trunk build with no env
      //   * "0.22.3-dev"                — CI dev-branch build (`<semver>-dev`)
      //   * "0.22.3-dev.9"              — release-candidate enumerated
      //   * "0.22.3" / "v0.22.3"        — tagged release
      //   * "<...> (abc1234)" / "<...> (abc1234, 2026-04-27)" — optional SHA suffix
      // CI sets `BUILD_VERSION` to `<Cargo.toml version>-dev` for every
      // dev push (no enumerated `.N`), so `-dev` MUST match without the
      // trailing number; the enumerated form is for release candidates.
      expect(text).toMatch(
        /^(dev|v?\d+\.\d+\.\d+(-dev(\.\d+)?)?(\s\([0-9a-f]{7}(,\s\d{4}-\d{2}-\d{2})?\))?)$/,
      );
    }
  });
});
