/// FLV stream normalizer: produces a single continuous FLV byte stream
/// suitable for piping into a fresh `ffmpeg -re` process.
///
/// Two responsibilities:
///
/// 1. **Rebase-to-zero on first chunk** — xiu writes absolute RTMP session
///    timestamps, so a mid-session chunk has FLV PTS values like 1_457_000ms.
///    ffmpeg's `-re` pacer compares frame PTS to wall-clock since process
///    start; if the first frame's PTS is far in the past, `-re` concludes
///    "stream is way behind" and drains stdin as fast as possible,
///    bypassing its real-time pacing. The normalizer rebases the first
///    chunk so its first data tag lands at ts=0, letting `-re` pace
///    correctly from process start.
///
/// 2. **Monotonic continuation across chunk boundaries** — subsequent
///    chunks are rebased so their first data tag lands at
///    `last_output_ts + 1`, absorbing any xiu-side session resets
///    (which would otherwise manifest as a backward DTS jump → "Broken
///    pipe ... Conversion failed!" in ffmpeg stderr).
///
/// Also strips duplicate sequence headers on chunks after the first
/// (xiu re-emits them on every chunk boundary; ffmpeg treats repeated
/// codec config as malformed input).
pub struct FlvStreamNormalizer {
    pub sent_header: bool,
    /// Last timestamp we emitted into the output stream (ms).
    last_output_ts: u32,
}

impl Default for FlvStreamNormalizer {
    fn default() -> Self {
        Self::new()
    }
}

impl FlvStreamNormalizer {
    pub fn new() -> Self {
        Self {
            sent_header: false,
            last_output_ts: 0,
        }
    }

    /// Normalize an FLV chunk for continuous streaming.
    /// First chunk: rebase all tags so the first data tag lands at ts=0
    /// (so ffmpeg -re paces correctly from process start).
    /// Subsequent chunks: strip FLV header + duplicate sequence headers,
    /// rebase so the first data tag lands at `last_output_ts + 1`.
    pub fn normalize(&mut self, data: &[u8]) -> Vec<u8> {
        if data.len() < 13 || &data[0..3] != b"FLV" {
            return data.to_vec();
        }

        let body_start = 9 + 4;

        // On the FIRST chunk of a fresh normalizer (= fresh ffmpeg process),
        // rebase intra-chunk timestamps so the first data tag lands at ts=0.
        // Without this, ffmpeg -re sees FLV timestamps deep in the past
        // (xiu writes absolute timestamps that grow for the lifetime of the
        // ingest — e.g. 1_457_000ms for a 24-minute session). ffmpeg treats
        // such input as "way behind real time" and drains stdin as fast as
        // possible, bypassing its own real-time pacer. Starting each ffmpeg
        // process at PTS=0 restores ffmpeg's native -re pacing and makes
        // the separate Rust-side pacing layer unnecessary.
        if !self.sent_header {
            self.sent_header = true;
            let Some(first_ts) = find_first_data_ts(data) else {
                // No data tags in first chunk — pass through, normalizer
                // will pick up the base on the next chunk.
                return data.to_vec();
            };
            let rebase_offset = -(first_ts as i64);
            return self.rebase_chunk(data, body_start, rebase_offset, true);
        }

        let rebase_offset = compute_rebase_offset(data, body_start, self.last_output_ts);
        self.rebase_chunk(data, body_start, rebase_offset, false)
    }

    /// Walk the FLV tags in `data[body_start..]`, rebase timestamps by
    /// `rebase_offset`, and emit to an output `Vec`. `include_header`
    /// prepends the 9-byte FLV header + 4-byte PreviousTagSize0 so the
    /// first chunk after a new normalizer starts a valid FLV stream for
    /// ffmpeg. Sequence headers are stripped on subsequent chunks
    /// (include_header=false) but preserved on the first chunk.
    fn rebase_chunk(
        &mut self,
        data: &[u8],
        body_start: usize,
        rebase_offset: i64,
        include_header: bool,
    ) -> Vec<u8> {
        let mut offset = body_start;
        let mut result = Vec::with_capacity(data.len());
        if include_header {
            result.extend_from_slice(&data[..body_start]);
        }

        while offset + 11 <= data.len() {
            let tag_type = data[offset];
            if tag_type != 8 && tag_type != 9 && tag_type != 18 {
                break;
            }

            let data_size = ((data[offset + 1] as u32) << 16)
                | ((data[offset + 2] as u32) << 8)
                | (data[offset + 3] as u32);

            let tag_total = 11 + data_size as usize + 4;
            if offset + tag_total > data.len() {
                break;
            }

            let is_seq_header = (tag_type == 9 || tag_type == 8)
                && offset + 12 < data.len()
                && data[offset + 12] == 0x00;

            // On the first chunk we MUST keep sequence headers so ffmpeg
            // can decode the stream. On subsequent chunks xiu re-emits
            // the same sequence headers that ffmpeg has already seen —
            // stripping them avoids the "invalid packet" / muxer issues
            // caused by duplicate codec config packets.
            let keep = include_header || !is_seq_header;

            if keep {
                let mut tag = data[offset..offset + tag_total].to_vec();
                let orig_ts = read_tag_timestamp(&tag);
                let new_ts = apply_offset(orig_ts, rebase_offset);
                write_tag_timestamp(&mut tag, new_ts);
                if !is_seq_header && new_ts > self.last_output_ts {
                    self.last_output_ts = new_ts;
                }
                result.extend_from_slice(&tag);
            }

            offset += tag_total;
        }

        result
    }
}

/// Scan an FLV chunk and return the timestamp of its FIRST non-sequence tag.
/// Used to determine the rebase offset that makes the first chunk start at
/// ts=0 for each new ffmpeg process.
fn find_first_data_ts(data: &[u8]) -> Option<u32> {
    if data.len() < 13 || &data[0..3] != b"FLV" {
        return None;
    }
    let mut offset = 9 + 4;
    while offset + 11 <= data.len() {
        let tag_type = data[offset];
        if tag_type != 8 && tag_type != 9 && tag_type != 18 {
            break;
        }
        let data_size = ((data[offset + 1] as u32) << 16)
            | ((data[offset + 2] as u32) << 8)
            | (data[offset + 3] as u32);
        let tag_total = 11 + data_size as usize + 4;
        if offset + tag_total > data.len() {
            break;
        }
        let is_seq_header = (tag_type == 9 || tag_type == 8)
            && offset + 12 < data.len()
            && data[offset + 12] == 0x00;
        if !is_seq_header {
            return Some(read_tag_timestamp(&data[offset..offset + tag_total]));
        }
        offset += tag_total;
    }
    None
}

/// Scan an FLV chunk and return the timestamp of its last non-sequence tag.
#[allow(dead_code)]
fn find_last_data_ts(data: &[u8]) -> Option<u32> {
    if data.len() < 13 || &data[0..3] != b"FLV" {
        return None;
    }
    let mut offset = 9 + 4;
    let mut last: Option<u32> = None;
    while offset + 11 <= data.len() {
        let tag_type = data[offset];
        if tag_type != 8 && tag_type != 9 && tag_type != 18 {
            break;
        }
        let data_size = ((data[offset + 1] as u32) << 16)
            | ((data[offset + 2] as u32) << 8)
            | (data[offset + 3] as u32);
        let tag_total = 11 + data_size as usize + 4;
        if offset + tag_total > data.len() {
            break;
        }
        let is_seq_header = (tag_type == 9 || tag_type == 8)
            && offset + 12 < data.len()
            && data[offset + 12] == 0x00;
        if !is_seq_header {
            last = Some(read_tag_timestamp(&data[offset..offset + tag_total]));
        }
        offset += tag_total;
    }
    last
}

/// Compute the ms offset to add to every data-tag timestamp in this chunk
/// so the first non-sequence tag lands at `last_output_ts + 1`. Returns a
/// negative offset when the chunk's absolute timestamps are FORWARD of the
/// previous rebased stream (the common xiu case, where chunk N+1 carries
/// absolute session PTS while chunk 1 was already rebased to start at 0).
/// Without this negative offset, ffmpeg's `-re` pacer would see a huge
/// forward PTS jump between chunks and sleep for many minutes, eventually
/// causing the consumer's write to time out.
fn compute_rebase_offset(data: &[u8], body_start: usize, last_output_ts: u32) -> i64 {
    let mut offset = body_start;
    while offset + 11 <= data.len() {
        let tag_type = data[offset];
        if tag_type != 8 && tag_type != 9 && tag_type != 18 {
            break;
        }
        let data_size = ((data[offset + 1] as u32) << 16)
            | ((data[offset + 2] as u32) << 8)
            | (data[offset + 3] as u32);
        let tag_total = 11 + data_size as usize + 4;
        if offset + tag_total > data.len() {
            break;
        }
        let is_seq_header = (tag_type == 9 || tag_type == 8)
            && offset + 12 < data.len()
            && data[offset + 12] == 0x00;
        if !is_seq_header {
            let first_ts = read_tag_timestamp(&data[offset..offset + tag_total]);
            let target = (last_output_ts as i64) + 1;
            return target - first_ts as i64;
        }
        offset += tag_total;
    }
    0
}

fn read_tag_timestamp(tag: &[u8]) -> u32 {
    ((tag[4] as u32) << 16) | ((tag[5] as u32) << 8) | (tag[6] as u32) | ((tag[7] as u32) << 24)
}

fn write_tag_timestamp(tag: &mut [u8], ts: u32) {
    tag[4] = (ts >> 16) as u8;
    tag[5] = (ts >> 8) as u8;
    tag[6] = ts as u8;
    tag[7] = (ts >> 24) as u8;
}

fn apply_offset(ts: u32, offset: i64) -> u32 {
    let shifted = ts as i64 + offset;
    if shifted < 0 {
        0
    } else if shifted > u32::MAX as i64 {
        u32::MAX
    } else {
        shifted as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal FLV chunk with the given (tag_type, timestamp_ms, payload) tags.
    /// tag_type: 8=audio, 9=video, 18=script.
    fn build_flv(tags: &[(u8, u32, Vec<u8>)]) -> Vec<u8> {
        let mut buf = Vec::new();
        // FLV header (9 bytes): F L V 0x01 0x05 0x00000009
        buf.extend_from_slice(b"FLV");
        buf.push(0x01);
        buf.push(0x05);
        buf.extend_from_slice(&[0, 0, 0, 9]);
        // PreviousTagSize0
        buf.extend_from_slice(&[0, 0, 0, 0]);

        for (tt, ts, payload) in tags {
            let data_size = payload.len() as u32;
            // Tag header
            buf.push(*tt);
            buf.push((data_size >> 16) as u8);
            buf.push((data_size >> 8) as u8);
            buf.push(data_size as u8);
            buf.push((*ts >> 16) as u8);
            buf.push((*ts >> 8) as u8);
            buf.push(*ts as u8);
            buf.push((*ts >> 24) as u8);
            buf.extend_from_slice(&[0, 0, 0]); // StreamID
            buf.extend_from_slice(payload);
            // PreviousTagSize
            let prev_sz = 11 + data_size;
            buf.extend_from_slice(&[
                (prev_sz >> 24) as u8,
                (prev_sz >> 16) as u8,
                (prev_sz >> 8) as u8,
                prev_sz as u8,
            ]);
        }
        buf
    }

    /// Extract all tag timestamps from an FLV-formatted (or tag-only) byte stream.
    /// `has_header` indicates whether the first 13 bytes are FLV header + prev size.
    fn extract_timestamps(data: &[u8], has_header: bool) -> Vec<u32> {
        let mut out = Vec::new();
        let mut offset = if has_header { 13 } else { 0 };
        while offset + 11 <= data.len() {
            let tag_type = data[offset];
            if tag_type != 8 && tag_type != 9 && tag_type != 18 {
                break;
            }
            let data_size = ((data[offset + 1] as u32) << 16)
                | ((data[offset + 2] as u32) << 8)
                | (data[offset + 3] as u32);
            let tag_total = 11 + data_size as usize + 4;
            if offset + tag_total > data.len() {
                break;
            }
            out.push(read_tag_timestamp(&data[offset..offset + tag_total]));
            offset += tag_total;
        }
        out
    }

    // Video NALU payload: frametype|codecID=0x27 (inter frame + H.264), AVCPacketType=0x01 (NALU).
    // Using 0x01 (not 0x00) avoids the sequence-header heuristic.
    const NALU_VIDEO: [u8; 4] = [0x27, 0x01, 0x00, 0x00];
    // Audio raw payload: codec=AAC (0xA0), 0x01 = raw (not seq header).
    const RAW_AUDIO: [u8; 3] = [0xAF, 0x01, 0x00];

    #[test]
    fn first_chunk_rebased_to_start_at_zero() {
        // xiu writes absolute RTMP session timestamps; a chunk that lands
        // mid-session will have PTS values deep in the past (e.g. 1_457_000
        // for a 24-min session). The normalizer MUST rebase the first chunk
        // so the first data tag lands at ts=0, otherwise ffmpeg `-re` sees
        // "stream way behind real time" and drains stdin as fast as
        // possible, bypassing its real-time pacer.
        let mut norm = FlvStreamNormalizer::new();
        let chunk = build_flv(&[
            (9, 1_457_000, NALU_VIDEO.to_vec()),
            (8, 1_457_020, RAW_AUDIO.to_vec()),
            (9, 1_457_040, NALU_VIDEO.to_vec()),
        ]);
        let out = norm.normalize(&chunk);

        // Output must be valid FLV (header preserved) with timestamps
        // rebased to start at 0.
        assert_eq!(&out[..3], b"FLV", "FLV header must be preserved");
        let ts = extract_timestamps(&out, true);
        assert_eq!(
            ts,
            vec![0, 20, 40],
            "first chunk must rebase to start at ts=0"
        );
    }

    #[test]
    fn rebases_timestamps_when_session_resets_to_zero() {
        let mut norm = FlvStreamNormalizer::new();

        // Chunk 1: xiu session has been running for 24 min.
        // Video NALU at ts=1_457_000.
        let chunk1 = build_flv(&[(9, 1_457_000, NALU_VIDEO.to_vec())]);
        let out1 = norm.normalize(&chunk1);

        // Chunk 2: xiu's RTMP session reset — tags restart at ts=0.
        // Without rebase, the FLV muxer would see a huge backward jump
        // (1_457_000 -> 0) and drop the connection with "Broken pipe".
        let chunk2 = build_flv(&[
            (9, 0, NALU_VIDEO.to_vec()),
            (8, 20, RAW_AUDIO.to_vec()),
            (9, 40, NALU_VIDEO.to_vec()),
        ]);
        let out2 = norm.normalize(&chunk2);

        // Combine the two normalized chunks and extract every timestamp.
        let mut combined = Vec::new();
        combined.extend_from_slice(&out1);
        combined.extend_from_slice(&out2);

        let ts = extract_timestamps(&combined, true);
        assert!(
            ts.len() >= 4,
            "expected at least 4 tags, got {}: {ts:?}",
            ts.len()
        );

        for w in ts.windows(2) {
            assert!(
                w[1] >= w[0],
                "non-monotonic DTS across chunk boundary: {} -> {} (full sequence: {ts:?})",
                w[0],
                w[1]
            );
        }

        // First chunk rebased to start at 0 (first_ts=1_457_000 → 0).
        // Chunk 2 first_ts=0 must land at chunk1's last_output_ts + 1 to
        // preserve monotonicity (was 0, so chunk2's tags shift by +1).
        assert_eq!(ts[0], 0, "chunk1 first tag rebased to 0, got {}", ts[0]);
        assert!(
            ts[1] > ts[0],
            "chunk2 tags must land strictly after chunk1 (got {ts:?})"
        );
    }

    #[test]
    fn subsequent_chunk_rebased_to_continue_from_chunk1_end() {
        // After chunk 1 is rebased to start at ts=0, subsequent chunks MUST
        // also be shifted by the same (or similar) negative offset so the
        // overall stream stays close to wall-clock time. Otherwise ffmpeg's
        // `-re` pacer sees a huge forward PTS jump between chunks and sleeps
        // for many minutes, blocking the consumer's pipe write until it
        // times out (observed in production as a 30-32s ffmpeg death cycle).
        let mut norm = FlvStreamNormalizer::new();

        let chunk1 = build_flv(&[(9, 5000, NALU_VIDEO.to_vec())]);
        let _ = norm.normalize(&chunk1);

        // Chunk 2 continues the same session: ts grows naturally from 5020.
        let chunk2 = build_flv(&[
            (9, 5020, NALU_VIDEO.to_vec()),
            (9, 5040, NALU_VIDEO.to_vec()),
        ]);
        let out2 = norm.normalize(&chunk2);

        // Chunk 1's last output ts=0 (was 5000, rebased by -5000).
        // Chunk 2 targets last_output_ts+1 = 1; offset = 1 - 5020 = -5019.
        // Chunk 2 tags land at [1, 21], seamlessly continuing from chunk 1.
        let ts = extract_timestamps(&out2, false);
        assert_eq!(
            ts,
            vec![1, 21],
            "chunk 2 must rebase to land at last_output_ts+1 (was {ts:?})"
        );
    }

    #[test]
    fn ffmpeg_re_sees_no_large_forward_pts_jump_across_chunks() {
        // Regression for the production 30-32s death cycle:
        // xiu chunks carry absolute session PTS (e.g. 2_226_799 for a
        // 37-minute session). The FlvStreamNormalizer must shift EVERY
        // subsequent chunk by a compatible negative offset so the combined
        // output stream advances by ~(chunk_duration) ms per chunk, not by
        // the absolute-PTS delta. Without the fix, ffmpeg `-re` would sleep
        // for many minutes between chunks, blocking the stdin pipe.
        let mut norm = FlvStreamNormalizer::new();
        let chunk1 = build_flv(&[
            (9, 2_226_799, NALU_VIDEO.to_vec()),
            (9, 2_228_780, NALU_VIDEO.to_vec()), // ~1981ms span
        ]);
        let chunk2 = build_flv(&[
            (9, 2_228_866, NALU_VIDEO.to_vec()),
            (9, 2_230_800, NALU_VIDEO.to_vec()), // ~1934ms span
        ]);
        let out1 = norm.normalize(&chunk1);
        let out2 = norm.normalize(&chunk2);

        let mut combined = Vec::new();
        combined.extend_from_slice(&out1);
        combined.extend_from_slice(&out2);
        let ts = extract_timestamps(&combined, true);

        // All tags must be monotonic, and the gap between chunk 1's last
        // tag and chunk 2's first tag must be small (< 1 second), not the
        // absolute-PTS delta (2_228_866 - 2_228_780 = 86ms is fine; what we
        // specifically reject is a jump like 1981 -> 2_228_866).
        for w in ts.windows(2) {
            assert!(
                w[1] > w[0],
                "non-monotonic: {} -> {} (ts={ts:?})",
                w[0],
                w[1]
            );
            assert!(
                w[1] - w[0] < 1000,
                "forward PTS jump of {}ms between {} and {} would make ffmpeg -re sleep (ts={ts:?})",
                w[1] - w[0],
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn strips_sequence_headers_on_subsequent_chunks() {
        let mut norm = FlvStreamNormalizer::new();

        // First chunk has a sequence header (AVCPacketType=0x00) + a NALU.
        const SEQ_HEADER_VIDEO: [u8; 5] = [0x17, 0x00, 0x00, 0x00, 0x00];
        let chunk1 = build_flv(&[
            (9, 0, SEQ_HEADER_VIDEO.to_vec()),
            (9, 40, NALU_VIDEO.to_vec()),
        ]);
        let _ = norm.normalize(&chunk1);

        // Second chunk repeats the sequence header. Normalizer should strip it.
        let chunk2 = build_flv(&[
            (9, 60, SEQ_HEADER_VIDEO.to_vec()),
            (9, 80, NALU_VIDEO.to_vec()),
        ]);
        let out2 = norm.normalize(&chunk2);

        let ts = extract_timestamps(&out2, false);
        assert_eq!(
            ts.len(),
            1,
            "sequence header must be stripped, got ts={ts:?}"
        );
        // Chunk 1 was rebased with offset -40 (first_ts=40 → 0).
        // Chunk 2 targets last_output_ts+1 = 1; offset = 1 - 80 = -79.
        // Stripped seq hdr; remaining NALU tag lands at 1.
        assert_eq!(ts[0], 1);
    }

    #[test]
    fn passes_through_non_flv_data() {
        let mut norm = FlvStreamNormalizer::new();
        let not_flv = vec![0x00, 0x00, 0x01, 0xB3, 0x12, 0x34]; // MPEG-TS-ish
        let out = norm.normalize(&not_flv);
        assert_eq!(out, not_flv);
    }

    #[test]
    fn apply_offset_clamps_negative_result_to_zero() {
        // Defensive: if a future caller passes a negative offset big enough
        // to underflow ts, apply_offset must clamp to 0 (never wrap).
        assert_eq!(apply_offset(100, -500), 0);
        assert_eq!(apply_offset(0, -1), 0);
    }

    #[test]
    fn apply_offset_saturates_at_u32_max() {
        // FLV 4-byte extended timestamp is u32. Saturate on overflow instead of
        // wrapping, which would produce a backward DTS jump to 0.
        assert_eq!(apply_offset(u32::MAX - 5, 100), u32::MAX);
    }

    #[test]
    fn apply_offset_additive_identity() {
        assert_eq!(apply_offset(12_345, 0), 12_345);
        assert_eq!(apply_offset(0, 12_345), 12_345);
    }
}
