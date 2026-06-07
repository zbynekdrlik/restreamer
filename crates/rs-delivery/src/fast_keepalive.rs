//! Fast-endpoint keepalive: hold the EXISTING rtmp session alive during a
//! short producer gap by re-pushing the last delivered chunk (freeze), then
//! the default rescue loop after `FAST_KEEPALIVE_RESCUE_AFTER_SECS`. Re-using
//! the same session means the RTMP connection is never closed by starvation
//! — only a real socket error reconnects. `push_flv_bytes` re-anchors
//! timestamps across the repeated blob internally.
#![allow(dead_code)]
use std::sync::Arc;

/// Wait this long for a real chunk before starting keepalive frames. Far
/// below the 8s full-stall rescue threshold so the trickle regime (chunks
/// arriving late but often) is covered, not just total outages.
pub const FAST_KEEPALIVE_TRIGGER_SECS: u64 = 2;
/// After this much continuous gap, switch the keepalive content from the
/// frozen last chunk to the default rescue loop.
pub const FAST_KEEPALIVE_RESCUE_AFTER_SECS: u64 = 10;

/// Which content the keepalive is currently pushing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeepaliveMode {
    Freeze,
    Rescue,
}

/// Pure decision: given how long the gap has lasted, what to push.
pub fn keepalive_mode(gap_secs: u64, have_freeze: bool) -> KeepaliveMode {
    if have_freeze && gap_secs < FAST_KEEPALIVE_RESCUE_AFTER_SECS {
        KeepaliveMode::Freeze
    } else {
        KeepaliveMode::Rescue
    }
}

/// Select the FLV bytes to push for a keepalive tick.
pub fn keepalive_bytes<'a>(
    mode: KeepaliveMode,
    last_chunk: &'a Option<Arc<Vec<u8>>>,
) -> std::borrow::Cow<'a, [u8]> {
    match mode {
        KeepaliveMode::Freeze => match last_chunk {
            Some(b) => std::borrow::Cow::Borrowed(b.as_slice()),
            None => std::borrow::Cow::Borrowed(crate::rescue_default::DEFAULT_RESCUE_FLV),
        },
        KeepaliveMode::Rescue => {
            std::borrow::Cow::Borrowed(crate::rescue_default::DEFAULT_RESCUE_FLV)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn freeze_then_rescue_by_gap() {
        assert_eq!(keepalive_mode(0, true), KeepaliveMode::Freeze);
        assert_eq!(keepalive_mode(9, true), KeepaliveMode::Freeze);
        assert_eq!(keepalive_mode(10, true), KeepaliveMode::Rescue);
        assert_eq!(keepalive_mode(0, false), KeepaliveMode::Rescue);
    }

    #[test]
    fn bytes_fall_back_to_default_when_no_freeze() {
        let none: Option<Arc<Vec<u8>>> = None;
        let b = keepalive_bytes(KeepaliveMode::Freeze, &none);
        assert_eq!(&*b, crate::rescue_default::DEFAULT_RESCUE_FLV);
    }

    #[test]
    fn freeze_uses_last_chunk_bytes() {
        let last = Some(Arc::new(vec![1u8, 2, 3]));
        let b = keepalive_bytes(KeepaliveMode::Freeze, &last);
        assert_eq!(&*b, &[1u8, 2, 3]);
    }
}
