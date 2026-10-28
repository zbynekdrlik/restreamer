//! #424: one VPS log line per pushed chunk and ingest.
//!
//! The go-live repeat (camera-box av-gate) could be neither confirmed nor
//! ruled out from the VPS side: nothing logged which chunk each ingest
//! (primary / YouTube backup) pushed when, at which outgoing timestamps. Every
//! successful chunk push now logs, under the `chunk_push` target:
//!
//! `chunk_push alias=<a> ingest=<primary|backup> chunk_id=<id|-> first_ts=<ms>
//! last_ts=<ms> start_unix_ms=<ms> end_unix_ms=<ms>`
//!
//! `first_ts` / `last_ts` are the WIRE timestamps (`rs_rtmp_push::WireSpan`),
//! `chunk_id=-` marks a rescue / warmup clip segment (no S3 chunk). The two
//! ingests have their own wire mappings, so the same chunk id shows both
//! mappings side by side.

use rs_rtmp_push::WireSpan;

/// Which ingest pushed the chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ingest {
    Primary,
    Backup,
}

impl Ingest {
    fn label(self) -> &'static str {
        match self {
            Ingest::Primary => "primary",
            Ingest::Backup => "backup",
        }
    }
}

/// The log line of one push, or `None` when the push put no media on the wire
/// (a failed push, an empty payload).
pub(crate) fn chunk_push_line(
    alias: &str,
    ingest: Ingest,
    chunk_id: Option<i64>,
    span: Option<WireSpan>,
    start_unix_ms: i64,
    end_unix_ms: i64,
) -> Option<String> {
    let span = span?;
    let chunk = chunk_id.map_or_else(|| "-".to_string(), |id| id.to_string());
    Some(format!(
        "chunk_push alias={alias} ingest={} chunk_id={chunk} first_ts={} last_ts={} start_unix_ms={start_unix_ms} end_unix_ms={end_unix_ms}",
        ingest.label(),
        span.first_ts_ms,
        span.last_ts_ms,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPAN: WireSpan = WireSpan {
        first_ts_ms: 41_000,
        last_ts_ms: 41_980,
    };

    #[test]
    fn a_primary_chunk_line_carries_id_wire_span_and_wall_times() {
        assert_eq!(
            chunk_push_line(
                "e2e rtmp",
                Ingest::Primary,
                Some(29),
                Some(SPAN),
                1_793_139_500_000,
                1_793_139_500_990,
            )
            .as_deref(),
            Some(
                "chunk_push alias=e2e rtmp ingest=primary chunk_id=29 first_ts=41000 last_ts=41980 start_unix_ms=1793139500000 end_unix_ms=1793139500990"
            )
        );
    }

    #[test]
    fn a_backup_clip_segment_has_no_chunk_id() {
        assert_eq!(
            chunk_push_line("yt", Ingest::Backup, None, Some(SPAN), 5, 7).as_deref(),
            Some(
                "chunk_push alias=yt ingest=backup chunk_id=- first_ts=41000 last_ts=41980 start_unix_ms=5 end_unix_ms=7"
            )
        );
    }

    #[test]
    fn a_push_with_nothing_on_the_wire_logs_nothing() {
        assert_eq!(
            chunk_push_line("yt", Ingest::Primary, Some(3), None, 5, 7),
            None
        );
    }
}
