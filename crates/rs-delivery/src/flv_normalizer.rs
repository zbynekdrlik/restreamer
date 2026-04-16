/// FLV stream normalizer: strips duplicate FLV headers and sequence headers
/// from concatenated FLV chunks, producing a single continuous FLV stream
/// with monotonically non-decreasing tag timestamps.
///
/// When xiu's RTMP timestamp counter resets mid-stream (OBS reconnect,
/// server restart, session reset), the next chunk's tags start at ts=0
/// again. The FLV muxer on the output side (piped to ffmpeg → YouTube RTMP)
/// is strict about monotonic DTS — a backward jump produces
///     "Non-monotonic DTS ... Broken pipe ... Conversion failed!"
/// which kills the ffmpeg process and forces a restart.
///
/// This normalizer rewrites the 4-byte timestamp field of every non-sequence
/// tag so the output stream is always monotonic.
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
    /// First chunk: pass through as-is (has FLV header + sequence headers).
    /// Subsequent chunks: strip FLV header (9+4 bytes) and sequence header tags,
    /// keeping only data tags, and rebase their timestamps onto the previous
    /// chunk's last emitted timestamp so the combined stream is monotonic.
    pub fn normalize(&mut self, data: &[u8]) -> Vec<u8> {
        if data.len() < 13 || &data[0..3] != b"FLV" {
            return data.to_vec();
        }

        if !self.sent_header {
            self.sent_header = true;
            if let Some(last) = find_last_data_ts(data) {
                self.last_output_ts = last;
            }
            return data.to_vec();
        }

        let body_start = 9 + 4;
        let rebase_offset = compute_rebase_offset(data, body_start, self.last_output_ts);

        let mut offset = body_start;
        let mut result = Vec::with_capacity(data.len());

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
                let mut tag = data[offset..offset + tag_total].to_vec();
                let orig_ts = read_tag_timestamp(&tag);
                let new_ts = apply_offset(orig_ts, rebase_offset);
                write_tag_timestamp(&mut tag, new_ts);
                if new_ts > self.last_output_ts {
                    self.last_output_ts = new_ts;
                }
                result.extend_from_slice(&tag);
            }

            offset += tag_total;
        }

        result
    }
}

/// Scan an FLV chunk and return the timestamp of its last non-sequence tag.
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
/// so the first non-sequence tag lands at `last_output_ts + 1`. Returns 0
/// if the chunk's timestamps are already monotonic with the previous
/// chunk (no rebase needed).
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
            return (target - first_ts as i64).max(0);
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
    fn passes_through_first_chunk_unchanged() {
        let mut norm = FlvStreamNormalizer::new();
        let chunk = build_flv(&[(9, 1000, NALU_VIDEO.to_vec())]);
        let out = norm.normalize(&chunk);
        assert_eq!(out, chunk, "first chunk should pass through byte-for-byte");
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

        // The rebased tags must be strictly after chunk 1's last timestamp.
        assert!(
            ts.last().unwrap() > &1_457_000,
            "rebased tags should land after prior chunk's last ts; got {ts:?}"
        );
    }

    #[test]
    fn no_rebase_when_chunks_already_monotonic() {
        let mut norm = FlvStreamNormalizer::new();

        let chunk1 = build_flv(&[(9, 5000, NALU_VIDEO.to_vec())]);
        let _ = norm.normalize(&chunk1);

        // Chunk 2 continues the same session: ts grows naturally from 5020.
        let chunk2 = build_flv(&[
            (9, 5020, NALU_VIDEO.to_vec()),
            (9, 5040, NALU_VIDEO.to_vec()),
        ]);
        let out2 = norm.normalize(&chunk2);

        // Rebased output should preserve the original timestamps (no artificial shift).
        let ts = extract_timestamps(&out2, false);
        assert_eq!(
            ts,
            vec![5020, 5040],
            "monotonic-continuation chunks must not be shifted"
        );
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
        assert_eq!(ts[0], 80);
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
