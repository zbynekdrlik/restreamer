// Pure content-level frame analysis for the #249 green-video picture check.
//
// No browser, no I/O — the live Studio-preview sampling in
// `youtube-studio-check.spec.ts` captures a downsampled frame via
// `<canvas>`+`getImageData` and feeds the raw RGBA bytes to `analyzeFrame`
// here. Keeping the maths in a pure module lets the thresholds be locked
// deterministically by `frame-analysis.unit.spec.ts` in the cheap
// `frontend-e2e` job, independent of any real YouTube session.
//
// The incident (2026-06-11): a rescue clip pushed first on a live session
// made YouTube lock a wrong codec config -> the whole broadcast rendered as a
// solid GREEN field while bitrate/frames flowed and every health metric read
// good. A uniform-colour frame is exactly what a per-channel-variance +
// distinct-colour check catches.

export interface FrameAnalysis {
  /** max over R,G,B of that channel's stddev across all pixels (0-255 scale). */
  maxStd: number;
  /** count of distinct colours after quantizing each channel to 4 bits (16 levels). */
  distinctColors: number;
  /** mean luma (Rec.601) across all pixels, 0-255. For naming black vs coloured. */
  meanLuma: number;
}

// A frame is FLAT (a suspect uniform-colour field) iff BOTH hold. The AND is
// deliberate: a two-tone slide has few distinct colours but a huge variance,
// and a busy scene has high variance but many colours — only a genuinely
// uniform field is low on BOTH.
export const FLAT_MAX_STD = 6;
export const FLAT_MAX_DISTINCT = 4;

// Accepts an RGBA buffer (4 bytes/pixel, the shape `CanvasRenderingContext2D
// .getImageData().data` returns). Alpha is ignored.
type PixelBuffer = Uint8ClampedArray | number[];

function assertRgba(data: PixelBuffer, w: number, h: number): void {
  const expected = w * h * 4;
  if (data.length !== expected) {
    throw new Error(
      `frame-analysis: expected RGBA buffer of ${expected} bytes for ${w}x${h}, got ${data.length}`,
    );
  }
}

// Bucket width for the MEAN-RELATIVE distinct-colour count (below).
const COLOR_BUCKET = 16;

export function analyzeFrame(
  data: PixelBuffer,
  w: number,
  h: number,
): FrameAnalysis {
  assertRgba(data, w, h);
  const n = w * h;
  if (n === 0) {
    throw new Error("frame-analysis: empty frame (w*h === 0)");
  }
  // Pass 1: per-channel mean, stddev, luma.
  let sumR = 0,
    sumG = 0,
    sumB = 0;
  let sumR2 = 0,
    sumG2 = 0,
    sumB2 = 0;
  let sumLuma = 0;
  for (let p = 0; p < n; p++) {
    const i = p * 4;
    const r = data[i];
    const g = data[i + 1];
    const b = data[i + 2];
    sumR += r;
    sumG += g;
    sumB += b;
    sumR2 += r * r;
    sumG2 += g * g;
    sumB2 += b * b;
    sumLuma += 0.299 * r + 0.587 * g + 0.114 * b;
  }
  const meanR = sumR / n;
  const meanG = sumG / n;
  const meanB = sumB / n;

  const std = (sum2: number, mean: number): number => {
    // clamp tiny negative from float error before sqrt
    return Math.sqrt(Math.max(0, sum2 / n - mean * mean));
  };

  // Pass 2: distinct colours, quantized RELATIVE TO THE CHANNEL MEAN. Absolute
  // 4-bit bucketing splits a uniform field whose mean straddles a 16-level
  // boundary (e.g. a YUV-zero green ~ (0,135,0) with +/-2 codec noise) into
  // several "colours", masking the green-video signature. Bucketing the
  // deviation from the mean instead means a genuinely uniform field collapses
  // to one bucket regardless of where its mean sits, while a two-tone field
  // still splits (and is caught by maxStd anyway).
  const colors = new Set<number>();
  const q = (v: number, mean: number): number =>
    Math.round((v - mean) / COLOR_BUCKET) + 8;
  for (let p = 0; p < n; p++) {
    const i = p * 4;
    colors.add(
      (q(data[i], meanR) << 16) |
        (q(data[i + 1], meanG) << 8) |
        q(data[i + 2], meanB),
    );
  }

  return {
    maxStd: Math.max(std(sumR2, meanR), std(sumG2, meanG), std(sumB2, meanB)),
    distinctColors: colors.size,
    meanLuma: sumLuma / n,
  };
}

export function isFlat(a: FrameAnalysis): boolean {
  return a.maxStd < FLAT_MAX_STD && a.distinctColors <= FLAT_MAX_DISTINCT;
}

// Temporal verdict: report FLAT for the whole window ONLY when EVERY sample is
// flat. A single non-flat sample (a fade, a scene switch, a momentary
// rebuffer) rescues the verdict, so a transient uniform frame cannot false-
// fail while a persistently green session cannot pass. An empty set is never a
// vacuous pass.
export function allSamplesFlat(samples: FrameAnalysis[]): boolean {
  return samples.length > 0 && samples.every(isFlat);
}

// Mean absolute per-pixel difference across R,G,B between two same-size RGBA
// frames (0-255). ~0 means the picture did not change between samples (a
// freeze); a live scene yields a clearly positive value.
export function frameDiff(
  a: PixelBuffer,
  b: PixelBuffer,
  w: number,
  h: number,
): number {
  assertRgba(a, w, h);
  assertRgba(b, w, h);
  const n = w * h;
  let acc = 0;
  for (let p = 0; p < n; p++) {
    const i = p * 4;
    acc +=
      Math.abs(a[i] - b[i]) +
      Math.abs(a[i + 1] - b[i + 1]) +
      Math.abs(a[i + 2] - b[i + 2]);
  }
  return acc / (n * 3);
}
