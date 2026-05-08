// Phase 1 (#176) shared helper. Imported by both
// `youtube-studio-check.spec.ts` (live) and `frontend.spec.ts` (fixture).
//
// Asserts the live YT health is "good" with no configuration_issues.
// Throws on first failure with a message matching `/YT health must be 'good'/`.

import type { Page } from "@playwright/test";

export async function assertYtHealthGood(page: Page): Promise<void> {
  const ytStatus = await page.evaluate(async () => {
    const res = await fetch("http://10.77.9.204:8910/api/v1/youtube/status");
    return res.json();
  });
  const activeStreams = (
    (ytStatus as { streams?: unknown[] })?.streams || []
  ).filter((s: any) => s.stream_status === "active");
  if (activeStreams.length === 0) {
    throw new Error("no active YT stream observed");
  }
  for (const s of activeStreams as any[]) {
    if (s.health_status !== "good") {
      throw new Error(
        `YT health must be 'good' (got '${s.health_status}' on stream '${s.title}')`,
      );
    }
    if (
      Array.isArray(s.configuration_issues) &&
      s.configuration_issues.length > 0
    ) {
      throw new Error(
        `YT configuration_issues must be empty (got ${JSON.stringify(s.configuration_issues)} on '${s.title}')`,
      );
    }
  }
}
