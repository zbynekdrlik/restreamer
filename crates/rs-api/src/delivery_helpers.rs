//! Small pure helpers used by the delivery orchestrator.
//!
//! Kept in a separate file so `delivery.rs` stays under the 1000-line file-size gate.

use std::path::PathBuf;

/// Returns true if the DB-side status represents a live delivery instance
/// that we can talk to over HTTP.
pub(crate) fn is_delivery_active(status: &str) -> bool {
    matches!(
        status,
        "booting" | "initializing" | "delivering" | "running"
    )
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
}
