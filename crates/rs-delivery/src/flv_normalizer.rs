/// FLV stream normalizer: strips duplicate FLV headers and sequence headers
/// from concatenated FLV chunks, producing a single continuous FLV stream.
pub struct FlvStreamNormalizer {
    pub sent_header: bool,
}

impl Default for FlvStreamNormalizer {
    fn default() -> Self {
        Self::new()
    }
}

impl FlvStreamNormalizer {
    pub fn new() -> Self {
        Self { sent_header: false }
    }

    /// Normalize an FLV chunk for continuous streaming.
    /// First chunk: pass through as-is (has FLV header + sequence headers).
    /// Subsequent chunks: strip FLV header (9+4 bytes) and sequence header tags,
    /// keeping only data tags.
    pub fn normalize(&mut self, data: &[u8]) -> Vec<u8> {
        // Not valid FLV -- pass through raw
        if data.len() < 13 || &data[0..3] != b"FLV" {
            return data.to_vec();
        }

        if !self.sent_header {
            // First chunk: send everything as-is
            self.sent_header = true;
            return data.to_vec();
        }

        // Subsequent chunks: skip FLV header and sequence header tags
        let mut offset = 9 + 4; // Skip FLV header + first prev_tag_size
        let mut result = Vec::with_capacity(data.len());

        while offset + 11 <= data.len() {
            let tag_type = data[offset];
            if tag_type != 8 && tag_type != 9 && tag_type != 18 {
                break;
            }

            let data_size = ((data[offset + 1] as u32) << 16)
                | ((data[offset + 2] as u32) << 8)
                | (data[offset + 3] as u32);

            let tag_total = 11 + data_size as usize + 4; // header + body + prev_tag_size

            if offset + tag_total > data.len() {
                break;
            }

            // Check if this is a sequence header (skip it -- already sent in first chunk)
            let is_seq_header = (tag_type == 9 || tag_type == 8)
                && offset + 12 < data.len()
                && data[offset + 12] == 0x00;

            if !is_seq_header {
                // Copy data tag as-is (with absolute timestamps from xiu)
                result.extend_from_slice(&data[offset..offset + tag_total]);
            }

            offset += tag_total;
        }

        result
    }
}
