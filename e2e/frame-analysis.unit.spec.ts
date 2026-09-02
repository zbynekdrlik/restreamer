// Unit coverage for the #249 content-level picture check.
//
// These tests exercise the PURE frame-analysis logic (no browser, no real
// YouTube) so the green-video detector's thresholds are locked deterministically
// on every push in the cheap `frontend-e2e` job. The live Studio-preview
// sampling that FEEDS this logic lives in `youtube-studio-check.spec.ts` and
// runs only on stream.lan (see that spec's header + the `.claude/rules`
// playbook entry for the CI wiring).
//
// The incident this guards (2026-06-11): a rescue clip pushed first on a live
// session made YouTube lock a wrong codec config -> solid GREEN for the whole
// session, while every health metric stayed good. A per-channel-variance +
// distinct-color check on a downsampled frame is what catches it.
import { test, expect } from "@playwright/test";
import {
  analyzeFrame,
  isFlat,
  frameDiff,
  allSamplesFlat,
} from "./lib/frame-analysis";

const W = 32;
const H = 18;

// Build an RGBA buffer (4 bytes/pixel, matching canvas getImageData) from a
// per-pixel colour function.
function makeFrame(
  w: number,
  h: number,
  fn: (x: number, y: number) => [number, number, number],
): Uint8ClampedArray {
  const buf = new Uint8ClampedArray(w * h * 4);
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) {
      const i = (y * w + x) * 4;
      const [r, g, b] = fn(x, y);
      buf[i] = r;
      buf[i + 1] = g;
      buf[i + 2] = b;
      buf[i + 3] = 255;
    }
  }
  return buf;
}

// Deterministic pseudo-random in [0,1) so tests never flake.
function rng(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s * 1664525 + 1013904223) >>> 0;
    return s / 0xffffffff;
  };
}

const solidGreen = makeFrame(W, H, () => [0, 255, 0]);
const solidBlack = makeFrame(W, H, () => [0, 0, 0]);
// Green field with mild compression noise (+/- a few levels): still FLAT.
const greenNoise = (() => {
  const r = rng(1);
  return makeFrame(W, H, () => [
    Math.round(r() * 3),
    255 - Math.round(r() * 3),
    Math.round(r() * 3),
  ]);
})();
// Two-tone slide: only 2 distinct colours, but a huge channel variance -> NOT flat.
const twoTone = makeFrame(W, H, (x) =>
  x < W / 2 ? [20, 30, 40] : [200, 210, 220],
);
// Smooth gradient across the frame: many colours, high variance -> NOT flat.
const gradient = makeFrame(W, H, (x) => {
  const v = Math.round((x / (W - 1)) * 255);
  return [v, 128, 255 - v];
});
// Camera-like broadband noise: many colours, high variance -> NOT flat.
const cameraNoise = (() => {
  const r = rng(7);
  return makeFrame(W, H, () => [
    Math.round(r() * 255),
    Math.round(r() * 255),
    Math.round(r() * 255),
  ]);
})();

test.describe("frame-analysis: uniform (green-video) detection", () => {
  test("a solid green field is FLAT", () => {
    const a = analyzeFrame(solidGreen, W, H);
    expect(a.maxStd).toBeLessThan(6);
    expect(a.distinctColors).toBeLessThanOrEqual(4);
    expect(isFlat(a)).toBe(true);
  });

  test("a solid black field is FLAT (buffering/dark placeholder)", () => {
    const a = analyzeFrame(solidBlack, W, H);
    expect(isFlat(a)).toBe(true);
    expect(a.meanLuma).toBeLessThan(8);
  });

  test("a green field with compression noise is still FLAT", () => {
    const a = analyzeFrame(greenNoise, W, H);
    expect(isFlat(a)).toBe(true);
  });

  test("a two-tone slide is NOT flat (distinct<=4 but high variance)", () => {
    const a = analyzeFrame(twoTone, W, H);
    expect(a.distinctColors).toBeLessThanOrEqual(4);
    expect(a.maxStd).toBeGreaterThanOrEqual(6);
    expect(isFlat(a)).toBe(false);
  });

  test("a gradient is NOT flat", () => {
    expect(isFlat(analyzeFrame(gradient, W, H))).toBe(false);
  });

  test("camera-like broadband noise is NOT flat", () => {
    const a = analyzeFrame(cameraNoise, W, H);
    expect(a.distinctColors).toBeGreaterThan(4);
    expect(isFlat(a)).toBe(false);
  });
});

test.describe("frame-analysis: temporal verdict", () => {
  test("allSamplesFlat is true only when EVERY sample is flat", () => {
    const flat = analyzeFrame(solidGreen, W, H);
    const live = analyzeFrame(cameraNoise, W, H);
    expect(allSamplesFlat([flat, flat, flat])).toBe(true);
    // one non-flat sample rescues the verdict (fade/scene-switch tolerance).
    expect(allSamplesFlat([flat, live, flat])).toBe(false);
    expect(allSamplesFlat([live, live, live])).toBe(false);
  });

  test("allSamplesFlat is false for an empty sample set (never a vacuous pass)", () => {
    expect(allSamplesFlat([])).toBe(false);
  });
});

test.describe("frame-analysis: bucket-boundary + threshold robustness", () => {
  // A uniform field whose channel mean straddles a 16-level quantization
  // boundary (green ~ (0,135,0)) with codec noise must still be FLAT — the
  // absolute-bucketing false-negative the mean-relative count fixes.
  test("a uniform green field straddling a bucket boundary is FLAT", () => {
    const r = rng(11);
    const boundaryGreen = makeFrame(W, H, () => [
      Math.round(r() * 4),
      135 + Math.round(r() * 4 - 2),
      Math.round(r() * 4),
    ]);
    const a = analyzeFrame(boundaryGreen, W, H);
    expect(a.maxStd).toBeLessThan(6);
    expect(isFlat(a)).toBe(true);
  });

  // Lock the FLAT_MAX_STD threshold: a 2-region field with stddev 5 is FLAT
  // (few colours, low variance); stddev 7 is NOT. (Integer deltas — a
  // Uint8ClampedArray rounds, so a fractional delta would not survive.)
  test("FLAT_MAX_STD threshold: stddev 5 flat, 7 not", () => {
    const twoRegion = (d: number) =>
      makeFrame(W, H, (x) =>
        x < W / 2 ? [100 - d, 100 - d, 100 - d] : [100 + d, 100 + d, 100 + d],
      );
    // half at mean-d, half at mean+d => stddev = d.
    expect(analyzeFrame(twoRegion(5), W, H).maxStd).toBeCloseTo(5, 5);
    expect(isFlat(analyzeFrame(twoRegion(5), W, H))).toBe(true);
    expect(analyzeFrame(twoRegion(7), W, H).maxStd).toBeCloseTo(7, 5);
    expect(isFlat(analyzeFrame(twoRegion(7), W, H))).toBe(false);
  });

  test("analyzeFrame throws on a wrong-length (RGB-not-RGBA) buffer", () => {
    const rgb = new Uint8ClampedArray(W * H * 3); // 3 bytes/pixel, wrong
    expect(() => analyzeFrame(rgb, W, H)).toThrow(/RGBA/);
  });

  test("analyzeFrame throws on an empty frame", () => {
    expect(() => analyzeFrame(new Uint8ClampedArray(0), 0, 0)).toThrow();
  });
});

test.describe("frame-analysis: change/freeze detection", () => {
  test("identical frames have ~zero diff", () => {
    expect(frameDiff(solidGreen, solidGreen, W, H)).toBeLessThan(1);
  });

  test("a moved/changed scene has a clearly non-zero diff", () => {
    expect(frameDiff(solidGreen, gradient, W, H)).toBeGreaterThan(10);
  });
});
