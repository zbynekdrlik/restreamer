//! Embedded default rescue FLV blob.
//!
//! Loaded at compile time from `assets/default_rescue.flv`. Pushed via
//! `rs_rtmp_push` whenever an endpoint enters rescue mode without a custom
//! operator-uploaded video. Always present, always works.
//!
//! Regenerate the asset with `cargo run --bin gen_rescue_flv`.

pub const DEFAULT_RESCUE_FLV: &[u8] = include_bytes!("../assets/default_rescue.flv");

#[cfg(test)]
mod tests {
    use super::*;

    /// R4: blob must be non-trivial and parse as FLV.
    #[test]
    fn default_rescue_flv_blob_integrity() {
        assert!(
            DEFAULT_RESCUE_FLV.len() > 50_000,
            "Default rescue blob too small: {} bytes (asset should be ~50-100KB)",
            DEFAULT_RESCUE_FLV.len()
        );
        assert!(
            DEFAULT_RESCUE_FLV.starts_with(b"FLV"),
            "Default rescue blob missing FLV magic prefix"
        );
        // FLV header: 'F' 'L' 'V' version flags datalen[4]
        // FLV version byte at offset 3, should be 1.
        assert_eq!(
            DEFAULT_RESCUE_FLV[3], 0x01,
            "Default rescue FLV version != 1"
        );
    }
}
