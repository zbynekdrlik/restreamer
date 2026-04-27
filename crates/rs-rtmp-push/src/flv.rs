//! Hand-rolled FLV tag iterator. The format is stable
//! (Adobe Flash Video File Format Specification v10) so a 80-LOC reader
//! avoids coupling to xflv's API surface.

use crate::PushError;

#[derive(Debug)]
pub struct FlvTag<'a> {
    pub tag_type: u8, // 8 = audio, 9 = video, 18 = script
    pub timestamp_ms: u32,
    pub body: &'a [u8],
}

/// Iterate FLV tags from a self-contained FLV file (header + tags + previous-size markers).
///
/// On malformed input, sets the iterator's error and stops.
pub struct FlvTagIter<'a> {
    bytes: &'a [u8],
    offset: usize,
    error: Option<PushError>,
}

impl<'a> FlvTagIter<'a> {
    /// Construct an iterator. Validates the 9-byte FLV header.
    pub fn new(bytes: &'a [u8]) -> Result<Self, PushError> {
        if bytes.len() < 9 + 4 {
            return Err(PushError::MalformedInput {
                offset: 0,
                reason: format!("FLV must be at least 13 bytes, got {}", bytes.len()),
            });
        }
        if &bytes[0..3] != b"FLV" {
            return Err(PushError::MalformedInput {
                offset: 0,
                reason: format!("expected 'FLV' signature, got {:?}", &bytes[0..3]),
            });
        }
        Ok(Self {
            bytes,
            offset: 9 + 4,
            error: None,
        })
    }

    pub fn into_error(self) -> Option<PushError> {
        self.error
    }
}

impl<'a> Iterator for FlvTagIter<'a> {
    type Item = FlvTag<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.error.is_some() || self.offset + 11 > self.bytes.len() {
            return None;
        }

        let tag_type = self.bytes[self.offset];
        let data_size = ((self.bytes[self.offset + 1] as usize) << 16)
            | ((self.bytes[self.offset + 2] as usize) << 8)
            | (self.bytes[self.offset + 3] as usize);
        let ts_low = ((self.bytes[self.offset + 4] as u32) << 16)
            | ((self.bytes[self.offset + 5] as u32) << 8)
            | (self.bytes[self.offset + 6] as u32);
        let ts_high = self.bytes[self.offset + 7] as u32;
        let timestamp_ms = (ts_high << 24) | ts_low;

        let body_start = self.offset + 11;
        let body_end = body_start + data_size;
        if body_end > self.bytes.len() {
            self.error = Some(PushError::MalformedInput {
                offset: self.offset,
                reason: format!(
                    "tag declares {} body bytes but only {} remain",
                    data_size,
                    self.bytes.len() - body_start
                ),
            });
            return None;
        }

        let body = &self.bytes[body_start..body_end];
        self.offset = body_end + 4;

        Some(FlvTag {
            tag_type,
            timestamp_ms,
            body,
        })
    }
}

pub const FLV_TAG_AUDIO: u8 = 8;
pub const FLV_TAG_VIDEO: u8 = 9;
pub const FLV_TAG_SCRIPT: u8 = 18;

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_flv_header() -> Vec<u8> {
        // FLV signature (3) + version (1) + flags (1) + data_offset (4) = 9 bytes
        // followed by PreviousTagSize0 (4 bytes = 0)
        let mut v = vec![b'F', b'L', b'V', 0x01, 0x05, 0x00, 0x00, 0x00, 0x09];
        v.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // PreviousTagSize0
        v
    }

    fn make_tag(tag_type: u8, timestamp_ms: u32, body: &[u8]) -> Vec<u8> {
        let data_size = body.len() as u32;
        let mut tag = Vec::new();
        tag.push(tag_type);
        tag.push(((data_size >> 16) & 0xff) as u8);
        tag.push(((data_size >> 8) & 0xff) as u8);
        tag.push((data_size & 0xff) as u8);
        // timestamp: low 24 bits then high 8 bits
        tag.push(((timestamp_ms >> 16) & 0xff) as u8);
        tag.push(((timestamp_ms >> 8) & 0xff) as u8);
        tag.push((timestamp_ms & 0xff) as u8);
        tag.push(((timestamp_ms >> 24) & 0xff) as u8);
        // stream id (3 bytes, always 0)
        tag.extend_from_slice(&[0x00, 0x00, 0x00]);
        tag.extend_from_slice(body);
        // PreviousTagSize = 11 + data_size
        let pts = 11 + data_size;
        tag.push(((pts >> 24) & 0xff) as u8);
        tag.push(((pts >> 16) & 0xff) as u8);
        tag.push(((pts >> 8) & 0xff) as u8);
        tag.push((pts & 0xff) as u8);
        tag
    }

    #[test]
    fn rejects_too_short() {
        assert!(FlvTagIter::new(b"FLV").is_err());
    }

    #[test]
    fn rejects_bad_signature() {
        let mut data = make_flv_header();
        data[0] = b'X';
        assert!(FlvTagIter::new(&data).is_err());
    }

    #[test]
    fn empty_flv_yields_no_tags() {
        let data = make_flv_header();
        let iter = FlvTagIter::new(&data).unwrap();
        assert_eq!(iter.count(), 0);
    }

    #[test]
    fn single_audio_tag() {
        let body = b"\xaf\x00hello";
        let mut data = make_flv_header();
        data.extend_from_slice(&make_tag(FLV_TAG_AUDIO, 42, body));
        let tags: Vec<_> = FlvTagIter::new(&data).unwrap().collect();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0].tag_type, FLV_TAG_AUDIO);
        assert_eq!(tags[0].timestamp_ms, 42);
        assert_eq!(tags[0].body, body);
    }

    #[test]
    fn timestamp_extended_bits() {
        // timestamp_ms that uses the high byte (> 0x00FFFFFF = 16777215)
        let ts = 0x01_00_00_00u32; // 16777216
        let mut data = make_flv_header();
        data.extend_from_slice(&make_tag(FLV_TAG_VIDEO, ts, b"\x17\x00"));
        let tags: Vec<_> = FlvTagIter::new(&data).unwrap().collect();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0].timestamp_ms, ts);
    }

    #[test]
    fn multiple_tags_in_order() {
        let mut data = make_flv_header();
        data.extend_from_slice(&make_tag(FLV_TAG_AUDIO, 0, b"a"));
        data.extend_from_slice(&make_tag(FLV_TAG_VIDEO, 33, b"v"));
        data.extend_from_slice(&make_tag(FLV_TAG_SCRIPT, 0, b"s"));
        let tags: Vec<_> = FlvTagIter::new(&data).unwrap().collect();
        assert_eq!(tags.len(), 3);
        assert_eq!(tags[0].tag_type, FLV_TAG_AUDIO);
        assert_eq!(tags[1].tag_type, FLV_TAG_VIDEO);
        assert_eq!(tags[2].tag_type, FLV_TAG_SCRIPT);
    }

    #[test]
    fn stops_on_truncated_tag() {
        let mut data = make_flv_header();
        // Write a tag header that claims 100 bytes body but we truncate early.
        data.extend_from_slice(&[
            FLV_TAG_AUDIO,
            0x00,
            0x00,
            100, // data_size = 100
            0x00,
            0x00,
            0x00,
            0x00, // timestamp
            0x00,
            0x00,
            0x00, // stream_id
            // only 5 body bytes, not 100
            0x01,
            0x02,
            0x03,
            0x04,
            0x05,
        ]);
        let mut iter = FlvTagIter::new(&data).unwrap();
        assert!(iter.next().is_none()); // stops
        assert!(iter.into_error().is_some()); // error recorded
    }
}
