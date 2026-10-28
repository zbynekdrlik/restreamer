//! #424: the wire timestamps one `push_flv_bytes` call put on the wire.
//!
//! The VPS logs one line per pushed chunk and ingest (primary / YouTube
//! backup) with this span, so a repeat or a timestamp step around a publish
//! switch can be read from the log: which chunk went out when, at which
//! outgoing timestamps, on which ingest.

/// First and last outgoing (wire) media timestamp of one push, in ms.
///
/// `first_ts_ms` is the first media tag SENT (send order), `last_ts_ms` the
/// highest one sent, across both tracks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireSpan {
    pub first_ts_ms: u64,
    pub last_ts_ms: u64,
}

impl WireSpan {
    /// Extend `span` with one media tag sent at wire ts `ts`.
    pub fn note(span: &mut Option<WireSpan>, ts: u64) {
        match span {
            Some(s) => s.last_ts_ms = s.last_ts_ms.max(ts),
            None => {
                *span = Some(WireSpan {
                    first_ts_ms: ts,
                    last_ts_ms: ts,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_tag_opens_the_span_and_the_highest_closes_it() {
        let mut span = None;
        WireSpan::note(&mut span, 1000);
        assert_eq!(
            span,
            Some(WireSpan {
                first_ts_ms: 1000,
                last_ts_ms: 1000
            })
        );
        WireSpan::note(&mut span, 1040);
        // An interleaved audio tag slightly behind the video keeps the max.
        WireSpan::note(&mut span, 1021);
        assert_eq!(
            span,
            Some(WireSpan {
                first_ts_ms: 1000,
                last_ts_ms: 1040
            })
        );
    }

    #[test]
    fn the_first_sent_tag_stays_first_even_if_a_later_one_is_lower() {
        let mut span = None;
        WireSpan::note(&mut span, 500);
        WireSpan::note(&mut span, 480);
        assert_eq!(
            span,
            Some(WireSpan {
                first_ts_ms: 500,
                last_ts_ms: 500
            })
        );
    }
}
