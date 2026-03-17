/// MPEG-TS Timestamp Normalizer
///
/// Rewrites DTS/PTS timestamps in raw MPEG-TS binary data to ensure continuity
/// across chunk boundaries. Uses delta-based timing to preserve natural frame
/// spacing while capping jumps at chunk boundaries.
const TS_PACKET_SIZE: usize = 188;
const SYNC_BYTE: u8 = 0x47;

/// Maximum timestamp delta before capping (90kHz ticks = 1 second)
const MAX_DELTA: i64 = 90_000;

/// Default video frame duration at 90kHz (30fps equivalent)
const VIDEO_DEFAULT_DURATION: i64 = 3_000;

/// Default audio frame duration at 90kHz (AAC 48kHz, 1024 samples)
const AUDIO_DEFAULT_DURATION: i64 = 1_920;

/// Per-stream timestamp state
#[derive(Debug, Clone)]
struct StreamState {
    out_dts: i64,
    out_pts: i64,
    prev_orig_dts: Option<i64>,
    prev_orig_pts: Option<i64>,
    default_duration: i64,
}

impl StreamState {
    fn new(default_duration: i64) -> Self {
        Self {
            out_dts: 0,
            out_pts: 0,
            prev_orig_dts: None,
            prev_orig_pts: None,
            default_duration,
        }
    }
}

/// Normalizes MPEG-TS timestamps to ensure continuity across chunk boundaries.
pub struct TSTimestampNormalizer {
    video: StreamState,
    audio: StreamState,
}

impl TSTimestampNormalizer {
    pub fn new() -> Self {
        Self {
            video: StreamState::new(VIDEO_DEFAULT_DURATION),
            audio: StreamState::new(AUDIO_DEFAULT_DURATION),
        }
    }

    /// Parse a 33-bit PES timestamp from 5 bytes.
    fn parse_ts_timestamp(data: &[u8]) -> i64 {
        let b0 = data[0] as i64;
        let b1 = data[1] as i64;
        let b2 = data[2] as i64;
        let b3 = data[3] as i64;
        let b4 = data[4] as i64;

        let mut ts = ((b0 >> 1) & 0x07) << 30;
        ts |= b1 << 22;
        ts |= ((b2 >> 1) & 0x7F) << 15;
        ts |= b3 << 7;
        ts |= (b4 >> 1) & 0x7F;
        ts
    }

    /// Write a 33-bit PES timestamp into 5 bytes.
    fn write_ts_timestamp(data: &mut [u8], ts: i64, marker: u8) {
        data[0] = ((marker & 0x0F) << 4) | ((((ts >> 30) & 0x07) as u8) << 1) | 1;
        data[1] = ((ts >> 22) & 0xFF) as u8;
        data[2] = ((((ts >> 15) & 0x7F) as u8) << 1) | 1;
        data[3] = ((ts >> 7) & 0xFF) as u8;
        data[4] = (((ts & 0x7F) as u8) << 1) | 1;
    }

    /// Compute a safe delta between original timestamps, capping at MAX_DELTA.
    fn compute_delta(current: i64, prev: Option<i64>, default: i64) -> i64 {
        match prev {
            Some(p) => {
                let raw_delta = current - p;
                if raw_delta <= 0 || raw_delta > MAX_DELTA {
                    default
                } else {
                    raw_delta
                }
            }
            None => default,
        }
    }

    /// Normalize timestamps in raw MPEG-TS data.
    ///
    /// Scans for TS packets, finds PES headers, and rewrites DTS/PTS
    /// to be continuous from the normalizer's accumulated position.
    pub fn normalize(&mut self, chunk_data: &[u8]) -> Vec<u8> {
        let mut data = chunk_data.to_vec();
        let len = data.len();
        let mut pos = 0;

        while pos + TS_PACKET_SIZE <= len {
            if data[pos] != SYNC_BYTE {
                pos += 1;
                continue;
            }

            let packet_end = pos + TS_PACKET_SIZE;

            // Check PUSI (Payload Unit Start Indicator) - bit 6 of byte 1
            let pusi = (data[pos + 1] & 0x40) != 0;
            if !pusi {
                pos = packet_end;
                continue;
            }

            // Adaptation field control (bits 4-5 of byte 3)
            let afc = (data[pos + 3] >> 4) & 0x03;
            let payload_offset = match afc {
                // 01: payload only
                1 => pos + 4,
                // 11: adaptation + payload
                3 => {
                    let af_len = data[pos + 4] as usize;
                    pos + 5 + af_len
                }
                // 00 or 10: no payload
                _ => {
                    pos = packet_end;
                    continue;
                }
            };

            // Check we have enough room for PES header (minimum 9 bytes)
            if payload_offset + 9 > packet_end {
                pos = packet_end;
                continue;
            }

            // Check PES start code: 0x00 0x00 0x01
            if data[payload_offset] != 0x00
                || data[payload_offset + 1] != 0x00
                || data[payload_offset + 2] != 0x01
            {
                pos = packet_end;
                continue;
            }

            let stream_id = data[payload_offset + 3];

            // Determine stream type: video (0xE0-0xEF) or audio (0xC0-0xDF)
            let state = if (0xE0..=0xEF).contains(&stream_id) {
                &mut self.video
            } else if (0xC0..=0xDF).contains(&stream_id) {
                &mut self.audio
            } else {
                pos = packet_end;
                continue;
            };

            // pts_dts_flags: bits 6-7 of PES header byte 7
            let pts_dts_flags = (data[payload_offset + 7] >> 6) & 0x03;

            // PTS/DTS start at payload_offset + 9
            let ts_start = payload_offset + 9;

            match pts_dts_flags {
                // 0b10: PTS only (5 bytes)
                2 => {
                    if ts_start + 5 > packet_end {
                        pos = packet_end;
                        continue;
                    }
                    let orig_pts = Self::parse_ts_timestamp(&data[ts_start..ts_start + 5]);
                    let delta =
                        Self::compute_delta(orig_pts, state.prev_orig_pts, state.default_duration);
                    state.out_pts += delta;
                    state.out_dts = state.out_pts;
                    state.prev_orig_pts = Some(orig_pts);
                    state.prev_orig_dts = Some(orig_pts);
                    Self::write_ts_timestamp(&mut data[ts_start..ts_start + 5], state.out_pts, 2);
                }
                // 0b11: PTS + DTS (10 bytes)
                3 => {
                    if ts_start + 10 > packet_end {
                        pos = packet_end;
                        continue;
                    }
                    let orig_pts = Self::parse_ts_timestamp(&data[ts_start..ts_start + 5]);
                    let orig_dts = Self::parse_ts_timestamp(&data[ts_start + 5..ts_start + 10]);

                    let dts_delta =
                        Self::compute_delta(orig_dts, state.prev_orig_dts, state.default_duration);
                    state.out_dts += dts_delta;

                    let pts_delta =
                        Self::compute_delta(orig_pts, state.prev_orig_pts, state.default_duration);
                    state.out_pts += pts_delta;

                    // Ensure PTS >= DTS
                    if state.out_pts < state.out_dts {
                        state.out_pts = state.out_dts;
                    }

                    state.prev_orig_dts = Some(orig_dts);
                    state.prev_orig_pts = Some(orig_pts);

                    Self::write_ts_timestamp(&mut data[ts_start..ts_start + 5], state.out_pts, 3);
                    Self::write_ts_timestamp(
                        &mut data[ts_start + 5..ts_start + 10],
                        state.out_dts,
                        1,
                    );
                }
                _ => {}
            }

            pos = packet_end;
        }

        data
    }
}

impl Default for TSTimestampNormalizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal TS packet with a PES header containing PTS only.
    fn build_ts_packet_pts_only(stream_id: u8, pts: i64) -> [u8; TS_PACKET_SIZE] {
        let mut packet = [0xFF_u8; TS_PACKET_SIZE];
        packet[0] = SYNC_BYTE;
        // PUSI set, PID doesn't matter for our logic
        packet[1] = 0x40;
        packet[2] = 0x00;
        // AFC = 01 (payload only), continuity counter = 0
        packet[3] = 0x10;
        // PES start code: 00 00 01
        packet[4] = 0x00;
        packet[5] = 0x00;
        packet[6] = 0x01;
        // Stream ID
        packet[7] = stream_id;
        // PES packet length (0 = unbounded for video)
        packet[8] = 0x00;
        packet[9] = 0x00;
        // PES header flags: marker bits + no scrambling
        packet[10] = 0x80;
        // pts_dts_flags = 0b10 (PTS only)
        packet[11] = 0x80;
        // PES header data length
        packet[12] = 0x05;
        // Write PTS at offset 13
        TSTimestampNormalizer::write_ts_timestamp(&mut packet[13..18], pts, 2);
        packet
    }

    /// Build a TS packet with PTS + DTS.
    fn build_ts_packet_pts_dts(stream_id: u8, pts: i64, dts: i64) -> [u8; TS_PACKET_SIZE] {
        let mut packet = [0xFF_u8; TS_PACKET_SIZE];
        packet[0] = SYNC_BYTE;
        packet[1] = 0x40;
        packet[2] = 0x00;
        packet[3] = 0x10;
        packet[4] = 0x00;
        packet[5] = 0x00;
        packet[6] = 0x01;
        packet[7] = stream_id;
        packet[8] = 0x00;
        packet[9] = 0x00;
        packet[10] = 0x80;
        // pts_dts_flags = 0b11 (PTS + DTS)
        packet[11] = 0xC0;
        packet[12] = 0x0A;
        TSTimestampNormalizer::write_ts_timestamp(&mut packet[13..18], pts, 3);
        TSTimestampNormalizer::write_ts_timestamp(&mut packet[18..23], dts, 1);
        packet
    }

    #[test]
    fn parse_write_timestamp_roundtrip() {
        let timestamps = [0i64, 1, 90_000, 1_000_000, 0x1_FFFF_FFFF];
        for &ts in &timestamps {
            let mut buf = [0u8; 5];
            TSTimestampNormalizer::write_ts_timestamp(&mut buf, ts, 2);
            let parsed = TSTimestampNormalizer::parse_ts_timestamp(&buf);
            assert_eq!(parsed, ts, "Roundtrip failed for timestamp {ts}");
        }
    }

    #[test]
    fn normalize_single_video_packet_pts_only() {
        let mut normalizer = TSTimestampNormalizer::new();
        let packet = build_ts_packet_pts_only(0xE0, 90_000);
        let result = normalizer.normalize(&packet);

        // First packet: delta = default_duration (3000) since no previous
        let new_pts = TSTimestampNormalizer::parse_ts_timestamp(&result[13..18]);
        assert_eq!(new_pts, VIDEO_DEFAULT_DURATION);
    }

    #[test]
    fn normalize_sequential_video_packets() {
        let mut normalizer = TSTimestampNormalizer::new();

        // First packet at PTS=90000
        let p1 = build_ts_packet_pts_only(0xE0, 90_000);
        let r1 = normalizer.normalize(&p1);
        let pts1 = TSTimestampNormalizer::parse_ts_timestamp(&r1[13..18]);
        assert_eq!(pts1, VIDEO_DEFAULT_DURATION);

        // Second packet at PTS=93000 (delta = 3000, normal 30fps)
        let p2 = build_ts_packet_pts_only(0xE0, 93_000);
        let r2 = normalizer.normalize(&p2);
        let pts2 = TSTimestampNormalizer::parse_ts_timestamp(&r2[13..18]);
        assert_eq!(pts2, VIDEO_DEFAULT_DURATION * 2);

        // Third packet at PTS=96000
        let p3 = build_ts_packet_pts_only(0xE0, 96_000);
        let r3 = normalizer.normalize(&p3);
        let pts3 = TSTimestampNormalizer::parse_ts_timestamp(&r3[13..18]);
        assert_eq!(pts3, VIDEO_DEFAULT_DURATION * 3);
    }

    #[test]
    fn normalize_caps_large_discontinuity() {
        let mut normalizer = TSTimestampNormalizer::new();

        // First packet
        let p1 = build_ts_packet_pts_only(0xE0, 1_000_000);
        normalizer.normalize(&p1);

        // Large jump (> MAX_DELTA) — should be capped to default_duration
        let p2 = build_ts_packet_pts_only(0xE0, 2_000_000);
        let r2 = normalizer.normalize(&p2);
        let pts2 = TSTimestampNormalizer::parse_ts_timestamp(&r2[13..18]);
        assert_eq!(pts2, VIDEO_DEFAULT_DURATION * 2);
    }

    #[test]
    fn normalize_handles_negative_delta() {
        let mut normalizer = TSTimestampNormalizer::new();

        // First packet at high PTS
        let p1 = build_ts_packet_pts_only(0xE0, 500_000);
        normalizer.normalize(&p1);

        // Second packet at lower PTS (negative delta) — uses default
        let p2 = build_ts_packet_pts_only(0xE0, 100_000);
        let r2 = normalizer.normalize(&p2);
        let pts2 = TSTimestampNormalizer::parse_ts_timestamp(&r2[13..18]);
        assert_eq!(pts2, VIDEO_DEFAULT_DURATION * 2);
    }

    #[test]
    fn normalize_separate_audio_video_state() {
        let mut normalizer = TSTimestampNormalizer::new();

        // Video packet
        let vp = build_ts_packet_pts_only(0xE0, 90_000);
        let vr = normalizer.normalize(&vp);
        let v_pts = TSTimestampNormalizer::parse_ts_timestamp(&vr[13..18]);
        assert_eq!(v_pts, VIDEO_DEFAULT_DURATION);

        // Audio packet — should use its own state
        let ap = build_ts_packet_pts_only(0xC0, 90_000);
        let ar = normalizer.normalize(&ap);
        let a_pts = TSTimestampNormalizer::parse_ts_timestamp(&ar[13..18]);
        assert_eq!(a_pts, AUDIO_DEFAULT_DURATION);
    }

    #[test]
    fn normalize_pts_dts_packet() {
        let mut normalizer = TSTimestampNormalizer::new();

        let packet = build_ts_packet_pts_dts(0xE0, 93_000, 90_000);
        let result = normalizer.normalize(&packet);

        let new_pts = TSTimestampNormalizer::parse_ts_timestamp(&result[13..18]);
        let new_dts = TSTimestampNormalizer::parse_ts_timestamp(&result[18..23]);

        assert_eq!(new_pts, VIDEO_DEFAULT_DURATION);
        assert_eq!(new_dts, VIDEO_DEFAULT_DURATION);
        assert!(new_pts >= new_dts);
    }

    #[test]
    fn normalize_multiple_packets_in_one_chunk() {
        let mut normalizer = TSTimestampNormalizer::new();

        // Concatenate 3 packets
        let mut chunk = Vec::new();
        chunk.extend_from_slice(&build_ts_packet_pts_only(0xE0, 90_000));
        chunk.extend_from_slice(&build_ts_packet_pts_only(0xE0, 93_000));
        chunk.extend_from_slice(&build_ts_packet_pts_only(0xE0, 96_000));

        let result = normalizer.normalize(&chunk);
        assert_eq!(result.len(), TS_PACKET_SIZE * 3);

        let pts1 = TSTimestampNormalizer::parse_ts_timestamp(&result[13..18]);
        let pts2 = TSTimestampNormalizer::parse_ts_timestamp(
            &result[TS_PACKET_SIZE + 13..TS_PACKET_SIZE + 18],
        );
        let pts3 = TSTimestampNormalizer::parse_ts_timestamp(
            &result[TS_PACKET_SIZE * 2 + 13..TS_PACKET_SIZE * 2 + 18],
        );

        assert_eq!(pts1, VIDEO_DEFAULT_DURATION);
        assert_eq!(pts2, VIDEO_DEFAULT_DURATION * 2);
        assert_eq!(pts3, VIDEO_DEFAULT_DURATION * 3);
    }

    #[test]
    fn normalize_preserves_non_pes_packets() {
        let mut normalizer = TSTimestampNormalizer::new();

        // Packet without PUSI
        let mut packet = [0xFF_u8; TS_PACKET_SIZE];
        packet[0] = SYNC_BYTE;
        packet[1] = 0x00; // No PUSI
        packet[2] = 0x00;
        packet[3] = 0x10;

        let original = packet;
        let result = normalizer.normalize(&packet);
        assert_eq!(result, original.to_vec());
    }

    #[test]
    fn normalize_empty_input() {
        let mut normalizer = TSTimestampNormalizer::new();
        let result = normalizer.normalize(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn normalize_data_shorter_than_packet() {
        let mut normalizer = TSTimestampNormalizer::new();
        let result = normalizer.normalize(&[SYNC_BYTE, 0x00, 0x00]);
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn normalize_cross_chunk_continuity() {
        let mut normalizer = TSTimestampNormalizer::new();

        // Chunk 1: PTS 1000000..1003000
        let c1p1 = build_ts_packet_pts_only(0xE0, 1_000_000);
        let c1p2 = build_ts_packet_pts_only(0xE0, 1_003_000);
        let mut chunk1 = Vec::new();
        chunk1.extend_from_slice(&c1p1);
        chunk1.extend_from_slice(&c1p2);
        let r1 = normalizer.normalize(&chunk1);
        let last_pts_c1 = TSTimestampNormalizer::parse_ts_timestamp(
            &r1[TS_PACKET_SIZE + 13..TS_PACKET_SIZE + 18],
        );

        // Chunk 2: PTS jumps to 5000000 (big discontinuity from different session)
        let c2p1 = build_ts_packet_pts_only(0xE0, 5_000_000);
        let r2 = normalizer.normalize(&c2p1);
        let first_pts_c2 = TSTimestampNormalizer::parse_ts_timestamp(&r2[13..18]);

        // Should be continuous: last_pts_c1 + default_duration (capped jump)
        assert_eq!(first_pts_c2, last_pts_c1 + VIDEO_DEFAULT_DURATION);
    }

    #[test]
    fn compute_delta_normal() {
        assert_eq!(
            TSTimestampNormalizer::compute_delta(93_000, Some(90_000), 3_000),
            3_000
        );
    }

    #[test]
    fn compute_delta_no_previous() {
        assert_eq!(
            TSTimestampNormalizer::compute_delta(90_000, None, 3_000),
            3_000
        );
    }

    #[test]
    fn compute_delta_caps_large_jump() {
        assert_eq!(
            TSTimestampNormalizer::compute_delta(1_000_000, Some(100_000), 3_000),
            3_000
        );
    }

    #[test]
    fn compute_delta_caps_negative() {
        assert_eq!(
            TSTimestampNormalizer::compute_delta(100, Some(90_000), 3_000),
            3_000
        );
    }

    #[test]
    fn normalize_skips_non_av_stream_ids() {
        let mut normalizer = TSTimestampNormalizer::new();
        // Stream ID 0xBD (private stream 1) — should be ignored
        let packet = build_ts_packet_pts_only(0xBD, 90_000);
        let result = normalizer.normalize(&packet);
        // PTS should be unchanged
        let pts = TSTimestampNormalizer::parse_ts_timestamp(&result[13..18]);
        assert_eq!(pts, 90_000);
    }

    #[test]
    fn normalize_handles_adaptation_field() {
        let mut normalizer = TSTimestampNormalizer::new();

        let mut packet = [0xFF_u8; TS_PACKET_SIZE];
        packet[0] = SYNC_BYTE;
        packet[1] = 0x40; // PUSI
        packet[2] = 0x00;
        // AFC = 11 (adaptation + payload)
        packet[3] = 0x30;
        // Adaptation field length = 7
        packet[4] = 0x07;
        // Skip adaptation field (bytes 5-11), payload starts at 12
        let payload_start = 12;
        packet[payload_start] = 0x00;
        packet[payload_start + 1] = 0x00;
        packet[payload_start + 2] = 0x01;
        packet[payload_start + 3] = 0xE0; // Video
        packet[payload_start + 4] = 0x00;
        packet[payload_start + 5] = 0x00;
        packet[payload_start + 6] = 0x80;
        packet[payload_start + 7] = 0x80; // PTS only
        packet[payload_start + 8] = 0x05;
        TSTimestampNormalizer::write_ts_timestamp(
            &mut packet[payload_start + 9..payload_start + 14],
            90_000,
            2,
        );

        let result = normalizer.normalize(&packet);
        let new_pts = TSTimestampNormalizer::parse_ts_timestamp(
            &result[payload_start + 9..payload_start + 14],
        );
        assert_eq!(new_pts, VIDEO_DEFAULT_DURATION);
    }
}
