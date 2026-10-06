// #249 content-level picture gate — the browser-coupled half (the pure pixel
// maths lives in ./frame-analysis.ts, unit-tested in frame-analysis.unit.spec.ts).
//
// Captures the YouTube Studio Live Control Room preview <video> onto a tiny
// canvas via in-page getImageData and asserts it is NOT a sustained
// quasi-uniform colour field (the 2026-06-11 green-video incident). Extracted
// from youtube-studio-check.spec.ts so the spec body stays thin and this logic
// is importable/reusable (e.g. a future FB preview gate).
import { expect, type Page } from "@playwright/test";
import {
  analyzeFrame,
  isFlat,
  frameDiff,
  allSamplesFlat,
  type FrameAnalysis,
} from "./frame-analysis";

export const PICTURE_W = 32;
export const PICTURE_H = 18;
export const PICTURE_SAMPLES = 3;
export const PICTURE_SAMPLE_GAP_MS = 10_000;
export const PICTURE_READY_TIMEOUT_MS = 60_000;
export const PICTURE_MODES = ["shadow", "enforce", "off"] as const;
export type PictureMode = (typeof PICTURE_MODES)[number];

export function resolvePictureMode(raw: string | undefined): PictureMode {
  const m = (raw || "shadow").toLowerCase();
  if (!(PICTURE_MODES as readonly string[]).includes(m)) {
    throw new Error(
      `YT_PICTURE_GATE='${raw}' is invalid; expected one of ${PICTURE_MODES.join(
        " | ",
      )}`,
    );
  }
  return m as PictureMode;
}

export interface FrameProbe {
  ok: boolean;
  reason?: string;
  rgba?: number[];
  drawErr?: string | null;
  readyState?: number;
  currentTime?: number;
  totalVideoFrames?: number;
  videoWidth?: number;
  videoHeight?: number;
}

// Capture the Studio preview <video> onto a tiny canvas IN-PAGE (no control-
// bar/badge pixels, unlike an element screenshot) and return the raw RGBA
// bytes + decode counters. MSE (blob:-sourced) video does not taint the
// canvas; a genuine cross-origin taint surfaces as `drawErr`.
export async function probePreviewFrame(page: Page): Promise<FrameProbe> {
  return page.evaluate(
    ({ w, h }: { w: number; h: number }) => {
      function findVideo(root: any): HTMLVideoElement | null {
        if (!root || !root.querySelector) return null;
        const direct = root.querySelector("video");
        if (direct) return direct as HTMLVideoElement;
        const all = root.querySelectorAll ? root.querySelectorAll("*") : [];
        for (const el of all as any) {
          if (el.shadowRoot) {
            const found = findVideo(el.shadowRoot);
            if (found) return found;
          }
        }
        return null;
      }
      const host = document.querySelector("ytcp-live-streaming-stream-preview");
      const video =
        findVideo(host ? (host as any).shadowRoot || host : document) ||
        findVideo(document);
      if (!video) return { ok: false, reason: "no <video> in Studio preview" };
      const q =
        typeof (video as any).getVideoPlaybackQuality === "function"
          ? (video as any).getVideoPlaybackQuality()
          : { totalVideoFrames: 0 };
      let rgba: number[] | null = null;
      let drawErr: string | null = null;
      try {
        const c = document.createElement("canvas");
        c.width = w;
        c.height = h;
        const ctx = c.getContext("2d")!;
        ctx.drawImage(video, 0, 0, w, h);
        rgba = Array.from(ctx.getImageData(0, 0, w, h).data);
      } catch (e: any) {
        drawErr = String((e && e.message) || e);
      }
      return {
        ok: true,
        rgba: rgba as number[],
        drawErr,
        readyState: video.readyState,
        currentTime: video.currentTime,
        totalVideoFrames: q.totalVideoFrames,
        videoWidth: video.videoWidth,
        videoHeight: video.videoHeight,
      };
    },
    { w: PICTURE_W, h: PICTURE_H },
  );
}

// The full #249 picture gate. `mode` is "enforce" | "shadow".
export async function verifyPicture(page: Page, mode: PictureMode): Promise<void> {
  const enforce = mode === "enforce";
  const warnOrFail = (msg: string) => {
    if (enforce) {
      expect(false, msg).toBe(true);
    } else {
      console.warn(`[picture-gate shadow] WOULD FAIL: ${msg}`);
    }
  };

  // 1) Readiness: poll until a decoding <video> whose frame counter + clock
  //    advance, OR the deadline expires. A missing preview surface is NOT a
  //    first-poll giveup -- the <video> mounts a few seconds in -- so we keep
  //    polling and only judge at the deadline: a surface that never appears is
  //    a rollout-safe warning (shadow) / fail (enforce); a <video> that is
  //    present but never advances is a real decode stall/freeze (hard-fails in
  //    both modes -- the one freeze-class check safe to enforce now).
  const deadline = Date.now() + PICTURE_READY_TIMEOUT_MS;
  let prev: FrameProbe | null = null;
  let ready = false;
  let lastReason = "no probe yet";
  while (Date.now() < deadline) {
    const probe = await probePreviewFrame(page);
    if (!probe.ok) {
      lastReason = probe.reason || "preview surface not ready";
      prev = null; // reset the advance-pair on a surface miss
      await page.waitForTimeout(5_000);
      continue;
    }
    if (prev && prev.ok) {
      const framesAdvanced =
        (probe.totalVideoFrames ?? 0) > (prev.totalVideoFrames ?? 0);
      const clockAdvanced =
        (probe.currentTime ?? 0) - (prev.currentTime ?? 0) >= 1.5;
      if ((probe.readyState ?? 0) >= 3 && framesAdvanced && clockAdvanced) {
        ready = true;
        break;
      }
      lastReason = `readyState=${probe.readyState} framesAdvanced=${framesAdvanced} clockAdvanced=${clockAdvanced}`;
    }
    prev = probe;
    await page.waitForTimeout(5_000);
  }
  if (!ready) {
    const finalProbe = await probePreviewFrame(page);
    if (!finalProbe.ok) {
      warnOrFail(
        `#249 picture gate: the Studio preview surface never became available within ` +
          `${PICTURE_READY_TIMEOUT_MS / 1000}s (${lastReason}). The pixel check could not run.`,
      );
      return;
    }
    // A present-but-non-advancing <video> is a genuine decode stall/freeze.
    expect(
      false,
      `#249 picture gate: Studio preview <video> never advanced its decoded-frame counter + ` +
        `clock within ${PICTURE_READY_TIMEOUT_MS / 1000}s (${lastReason}). YouTube is not decoding ` +
        `the ingested stream into moving pictures (decode stall / freeze).`,
    ).toBe(true);
    return;
  }

  // 2) Sample N frames spaced apart and analyze the pure pixels.
  const analyses: FrameAnalysis[] = [];
  const rawFrames: number[][] = [];
  let frameCounterStart: number | null = null;
  let frameCounterEnd = 0;
  for (let i = 0; i < PICTURE_SAMPLES; i++) {
    if (i > 0) await page.waitForTimeout(PICTURE_SAMPLE_GAP_MS);
    const probe = await probePreviewFrame(page);
    if (!probe.ok || !probe.rgba) {
      warnOrFail(
        `#249 picture gate: preview probe ${i + 1}/${PICTURE_SAMPLES} returned no pixels ` +
          `(${probe.reason || probe.drawErr || "unknown"}).`,
      );
      return;
    }
    if (probe.drawErr) {
      // Cross-origin taint -> getImageData throws. The pngjs element-screenshot
      // fallback is a documented follow-up; do not silently pass.
      warnOrFail(
        `#249 picture gate: canvas getImageData failed (${probe.drawErr}) — the preview ` +
          `<video> is cross-origin tainted; the pngjs element-screenshot fallback is not yet ` +
          `implemented (see the filed follow-up).`,
      );
      return;
    }
    if (frameCounterStart === null)
      frameCounterStart = probe.totalVideoFrames ?? 0;
    frameCounterEnd = probe.totalVideoFrames ?? 0;
    const a = analyzeFrame(probe.rgba, PICTURE_W, PICTURE_H);
    analyses.push(a);
    rawFrames.push(probe.rgba);
    console.log(
      `[picture-gate] sample ${i + 1}/${PICTURE_SAMPLES}: maxStd=${a.maxStd.toFixed(
        2,
      )} distinct=${a.distinctColors} meanLuma=${a.meanLuma.toFixed(1)} ` +
        `flat=${isFlat(a)} totalVideoFrames=${probe.totalVideoFrames}`,
    );
  }

  // 3) meanDiff between consecutive samples — logged WARNING only (the CI OBS
  //    scene is camera-box-owned and not known to be dynamic, so a low diff is
  //    not gated here; the enforced freeze signal is the frame counter above).
  for (let i = 1; i < rawFrames.length; i++) {
    const d = frameDiff(rawFrames[i - 1], rawFrames[i], PICTURE_W, PICTURE_H);
    console.log(`[picture-gate] meanDiff sample ${i}->${i + 1}: ${d.toFixed(2)}`);
    if (d < 0.5) {
      console.warn(
        `[picture-gate] WARNING: near-zero frame diff (${d.toFixed(2)}) between ` +
          `samples ${i}->${i + 1} — possible full-frame freeze (not gated).`,
      );
    }
  }

  // 4) The verdict: a SUSTAINED flat field (every sample flat) is the
  //    green-video signature. Enforce fails; shadow warns.
  const verdict = allSamplesFlat(analyses);
  console.log(
    `[picture-gate] mode=${mode} sustainedFlat=${verdict} ` +
      `decodedFrames=${frameCounterStart}->${frameCounterEnd}`,
  );
  if (verdict) {
    warnOrFail(
      `#249 picture gate: YouTube's decoded preview is a SUSTAINED quasi-uniform colour field ` +
        `across all ${PICTURE_SAMPLES} samples (meanLuma≈${analyses[0].meanLuma.toFixed(
          0,
        )}) — the 2026-06-11 green-video signature. Health metrics can read good while the ` +
        `picture is a solid field; this is the content-level failure.`,
    );
  } else {
    console.log(
      "[picture-gate] PASS: decoded picture is NOT a sustained uniform field.",
    );
  }
}
