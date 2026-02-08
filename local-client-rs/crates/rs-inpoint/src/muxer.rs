use bytes::{BufMut, BytesMut};

/// Simple MPEG-TS muxer that wraps raw media data into transport stream packets.
///
/// MPEG-TS packets are always 188 bytes. This muxer creates a minimal TS container
/// suitable for the chunk upload pipeline. For full RTMP demuxing + H.264/AAC
/// remuxing, the xiu `rtmp`/`xmpegts` crates provide the complete solution.
///
/// Current implementation wraps raw bytes in TS packets with a null PID for
/// pass-through to the chunker. This ensures the chunk upload pipeline has
/// properly formatted binary chunks regardless of the RTMP parsing complexity.
pub struct TsMuxer {
    buffer: BytesMut,
    continuity_counter: u8,
}

/// MPEG-TS packet size.
const TS_PACKET_SIZE: usize = 188;
/// MPEG-TS sync byte.
const TS_SYNC_BYTE: u8 = 0x47;
/// PID for our data stream (arbitrary, non-reserved).
const DATA_PID: u16 = 0x0100;

impl TsMuxer {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(TS_PACKET_SIZE * 8),
            continuity_counter: 0,
        }
    }

    /// Write raw media data, producing complete TS packets.
    ///
    /// Returns any complete TS packets produced.
    pub fn write(&mut self, data: &[u8]) -> Vec<u8> {
        self.buffer.extend_from_slice(data);
        let mut output = Vec::new();

        // Payload capacity per TS packet (188 - 4 header bytes)
        let payload_size = TS_PACKET_SIZE - 4;

        while self.buffer.len() >= payload_size {
            let payload = self.buffer.split_to(payload_size);
            let packet = self.build_ts_packet(&payload, true);
            output.extend_from_slice(&packet);
        }

        output
    }

    /// Flush remaining data as a padded TS packet.
    pub fn flush(&mut self) -> Vec<u8> {
        if self.buffer.is_empty() {
            return Vec::new();
        }

        let payload_size = TS_PACKET_SIZE - 4;
        let mut payload = self.buffer.split_to(self.buffer.len()).to_vec();
        // Pad with 0xFF (stuffing bytes)
        payload.resize(payload_size, 0xFF);
        self.build_ts_packet(&payload, true)
    }

    fn build_ts_packet(&mut self, payload: &[u8], payload_unit_start: bool) -> Vec<u8> {
        let mut packet = BytesMut::with_capacity(TS_PACKET_SIZE);

        // Sync byte
        packet.put_u8(TS_SYNC_BYTE);

        // Transport error (0) | Payload unit start | Transport priority (0) | PID (13 bits)
        let byte1 = if payload_unit_start { 0x40 } else { 0x00 } | ((DATA_PID >> 8) as u8 & 0x1F);
        packet.put_u8(byte1);
        packet.put_u8((DATA_PID & 0xFF) as u8);

        // Scrambling (00) | Adaptation field (01 = payload only) | Continuity counter
        let byte3 = 0x10 | (self.continuity_counter & 0x0F);
        packet.put_u8(byte3);
        self.continuity_counter = (self.continuity_counter + 1) & 0x0F;

        // Payload
        packet.extend_from_slice(payload);

        packet.to_vec()
    }

    /// Reset the muxer state.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.continuity_counter = 0;
    }
}

impl Default for TsMuxer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ts_packet_is_188_bytes() {
        let mut muxer = TsMuxer::new();
        let data = vec![0xAA; 200];
        let output = muxer.write(&data);
        assert_eq!(output.len(), TS_PACKET_SIZE);
        assert_eq!(output[0], TS_SYNC_BYTE);
    }

    #[test]
    fn multiple_packets() {
        let mut muxer = TsMuxer::new();
        let data = vec![0xBB; 500];
        let output = muxer.write(&data);
        // 500 bytes -> 184 bytes per packet payload -> 2 full packets (368 bytes used)
        assert_eq!(output.len(), TS_PACKET_SIZE * 2);
        // Verify sync bytes
        assert_eq!(output[0], TS_SYNC_BYTE);
        assert_eq!(output[TS_PACKET_SIZE], TS_SYNC_BYTE);
    }

    #[test]
    fn flush_pads_remaining() {
        let mut muxer = TsMuxer::new();
        let data = vec![0xCC; 50];
        let output = muxer.write(&data);
        assert!(output.is_empty()); // not enough for a full packet

        let flushed = muxer.flush();
        assert_eq!(flushed.len(), TS_PACKET_SIZE);
        assert_eq!(flushed[0], TS_SYNC_BYTE);
    }

    #[test]
    fn continuity_counter_wraps() {
        let mut muxer = TsMuxer::new();
        for i in 0..20 {
            let data = vec![0xDD; 184]; // exactly one packet payload
            let output = muxer.write(&data);
            assert_eq!(output.len(), TS_PACKET_SIZE);
            let cc = output[3] & 0x0F;
            assert_eq!(cc, (i % 16) as u8);
        }
    }

    #[test]
    fn reset_clears_state() {
        let mut muxer = TsMuxer::new();
        muxer.write(&[0xEE; 100]);
        muxer.reset();

        let flushed = muxer.flush();
        assert!(flushed.is_empty());
    }
}
