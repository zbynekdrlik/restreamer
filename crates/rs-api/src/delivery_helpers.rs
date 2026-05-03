//! Small pure helpers used by the delivery orchestrator.
//!
//! Kept in a separate file so `delivery.rs` stays under the 1000-line file-size gate.

use std::path::PathBuf;

use rs_core::models::{EndpointConfig, PusherKind};

/// Build the per-endpoint JSON object embedded in the `/api/init` payload sent
/// to the rs-delivery VPS. The `pusher` field MUST be included so the VPS
/// honors the per-endpoint backend selection (#103) — without it, the VPS-side
/// `EndpointConfig` deserializer falls back to `PusherKind::Ffmpeg` via
/// `#[serde(default)]` and silently runs ffmpeg even when the operator
/// requested the rust pusher.
pub(crate) fn build_endpoint_init_entry(
    ep: &EndpointConfig,
    chunk_format: &str,
    start_chunk_id: i64,
) -> serde_json::Value {
    serde_json::json!({
        "alias": ep.alias,
        "service_type": ep.service_type,
        "stream_key": ep.stream_key,
        "is_fast": ep.is_fast,
        "chunk_format": chunk_format,
        "start_chunk_id": start_chunk_id,
        "pusher": ep.pusher,
    })
}

/// Returns the wire string a `PusherKind` serializes to. Used by audit and
/// dashboard payloads that want a stable lowercase tag.
#[allow(dead_code)]
pub(crate) fn pusher_wire_tag(p: PusherKind) -> &'static str {
    match p {
        PusherKind::Ffmpeg => "ffmpeg",
        PusherKind::Rust => "rust",
    }
}

/// Returns true if the DB-side status represents a live delivery instance
/// that we can talk to over HTTP.
pub(crate) fn is_delivery_active(status: &str) -> bool {
    matches!(
        status,
        "booting" | "initializing" | "delivering" | "running"
    )
}

/// Wider predicate than `is_delivery_active`: returns true when the row
/// represents a VPS that is either currently serving traffic OR being
/// spawned. `start_delivery` uses this to decide whether to short-circuit
/// (reuse existing row) vs. mark the row deleted and spawn fresh.
///
/// `creating` is included because an in-flight spawn shouldn't be raced
/// by a second `start_delivery` call. Anything else (`stopping`,
/// `failed`, `stopped`, `deleted`, unknown future statuses) is stale and
/// safe to clean up.
pub(crate) fn is_delivery_or_spawning(status: &str) -> bool {
    status == "creating" || is_delivery_active(status)
}

/// Build the filename for a disk-persisted VPS log capture. Uses a
/// timestamp prefix so files sort chronologically in a directory listing.
pub(crate) fn delivery_log_filename(
    instance_id: i64,
    event_id: Option<i64>,
    unix_secs: u64,
) -> String {
    let evt = event_id
        .map(|e| e.to_string())
        .unwrap_or_else(|| "_".to_string());
    format!("{unix_secs}-evt{evt}-inst{instance_id}.log")
}

/// Pure helper: write `log_text` to `{dir}/{filename}`, creating `dir`
/// if missing. Returns the full path on success so tests can assert the
/// content landed where expected.
pub(crate) fn write_delivery_log_to_dir(
    dir: &std::path::Path,
    filename: &str,
    log_text: &str,
) -> std::io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(filename);
    std::fs::write(&path, log_text)?;
    Ok(path)
}

/// Persist VPS log text to disk as a companion to the DB row. Failure is
/// logged but not propagated — the DB row is the source of truth; this is
/// a resilience layer.
pub(crate) fn persist_delivery_log_to_disk(
    instance_id: i64,
    event_id: Option<i64>,
    log_text: &str,
) {
    let dir = rs_core::config::Config::delivery_log_dir();
    let unix_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let filename = delivery_log_filename(instance_id, event_id, unix_secs);

    match write_delivery_log_to_dir(&dir, &filename, log_text) {
        Ok(path) => {
            tracing::info!(
                path = %path.display(),
                bytes = log_text.len(),
                "VPS logs persisted to disk"
            );
        }
        Err(e) => {
            tracing::warn!(
                dir = %dir.display(),
                filename,
                "VPS log disk write failed: {e}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_delivery_active_live_states() {
        assert!(is_delivery_active("booting"));
        assert!(is_delivery_active("initializing"));
        assert!(is_delivery_active("delivering"));
        assert!(is_delivery_active("running"));
    }

    #[test]
    fn is_delivery_active_dead_states() {
        assert!(!is_delivery_active("creating"));
        assert!(!is_delivery_active("stopping"));
        assert!(!is_delivery_active("deleted"));
        assert!(!is_delivery_active("failed"));
        assert!(!is_delivery_active(""));
    }

    #[test]
    fn is_delivery_or_spawning_includes_creating_plus_active() {
        // Reuse-existing-row states: must short-circuit start_delivery.
        for s in [
            "creating",
            "booting",
            "initializing",
            "delivering",
            "running",
        ] {
            assert!(
                is_delivery_or_spawning(s),
                "{s} must short-circuit start_delivery"
            );
        }
    }

    #[test]
    fn is_delivery_or_spawning_excludes_stale_states() {
        // Stale states: must fall through to spawn fresh. Includes the
        // bug states from #165 (`failed`, `stopped`, `stopping`) plus
        // `deleted` and any unknown future status.
        for s in [
            "stopping",
            "failed",
            "stopped",
            "deleted",
            "unknown_future_status",
            "",
        ] {
            assert!(
                !is_delivery_or_spawning(s),
                "{s} must NOT short-circuit start_delivery (would block fresh spawn)"
            );
        }
    }

    #[test]
    fn delivery_log_filename_with_event() {
        assert_eq!(
            delivery_log_filename(42, Some(9279), 1_744_632_900),
            "1744632900-evt9279-inst42.log"
        );
    }

    #[test]
    fn delivery_log_filename_without_event() {
        assert_eq!(
            delivery_log_filename(7, None, 1_000_000_000),
            "1000000000-evt_-inst7.log"
        );
    }

    #[test]
    fn write_delivery_log_creates_missing_dir_and_file() {
        let tmp = std::env::temp_dir()
            .join(format!("restreamer-log-test-{}", std::process::id()))
            .join("nested");
        let _ = std::fs::remove_dir_all(tmp.parent().unwrap());

        let path = write_delivery_log_to_dir(&tmp, "probe.log", "hello\nworld").expect("write ok");
        assert_eq!(path, tmp.join("probe.log"));
        let read_back = std::fs::read_to_string(&path).expect("read ok");
        assert_eq!(read_back, "hello\nworld");

        std::fs::remove_dir_all(tmp.parent().unwrap()).ok();
    }

    fn make_endpoint(alias: &str, pusher: PusherKind) -> EndpointConfig {
        EndpointConfig {
            id: 1,
            alias: alias.to_string(),
            service_type: "youtube_hls".to_string(),
            stream_key: "key-xyz".to_string(),
            enabled: true,
            position_last: 0,
            delivered_bytes: 0,
            is_fast: false,
            pusher,
            created_at: "2026-04-27T00:00:00Z".to_string(),
            updated_at: "2026-04-27T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn init_entry_includes_pusher_rust() {
        // Regression for #103: VPS init payload MUST include the per-endpoint
        // pusher field. Without it, the VPS-side EndpointConfig deserializer
        // falls back to PusherKind::Ffmpeg via #[serde(default)] and the
        // operator's "pusher='rust'" choice is silently lost.
        let ep = make_endpoint("e2e rtmp", PusherKind::Rust);
        let v = build_endpoint_init_entry(&ep, "flv", 42);
        assert_eq!(
            v["pusher"], "rust",
            "pusher field must be present and 'rust'"
        );
        assert_eq!(v["alias"], "e2e rtmp");
        assert_eq!(v["start_chunk_id"], 42);
        assert_eq!(v["chunk_format"], "flv");
    }

    #[test]
    fn init_entry_includes_pusher_ffmpeg() {
        let ep = make_endpoint("FB-Zbynek", PusherKind::Ffmpeg);
        let v = build_endpoint_init_entry(&ep, "flv", 7);
        assert_eq!(
            v["pusher"], "ffmpeg",
            "pusher field must be present and 'ffmpeg'"
        );
    }

    #[test]
    fn init_entry_pusher_field_is_never_missing() {
        // Belt-and-braces: assert the JSON key exists for both variants. A
        // missing key (rather than wrong value) is the exact failure mode the
        // VPS silently absorbs via #[serde(default)].
        for p in [PusherKind::Ffmpeg, PusherKind::Rust] {
            let ep = make_endpoint("any", p);
            let v = build_endpoint_init_entry(&ep, "flv", 0);
            let obj = v.as_object().expect("init entry must be a JSON object");
            assert!(
                obj.contains_key("pusher"),
                "init entry missing 'pusher' field for {p:?}"
            );
        }
    }

    #[test]
    fn pusher_wire_tag_matches_serde_rename() {
        assert_eq!(pusher_wire_tag(PusherKind::Ffmpeg), "ffmpeg");
        assert_eq!(pusher_wire_tag(PusherKind::Rust), "rust");
    }
}
