//! Tests for `refused_subscribes.rs` (#404).

use super::*;
use rs_core::log_buffer::LogBuffer;
use rs_core::log_capture::LogCaptureLayer;
use tracing_subscriber::prelude::*;

/// Logs one `log` record through the tracing-log bridge, exactly as xiu's
/// `log::error!` reaches our subscriber, under a stack of the refusal layer
/// plus a capture layer. Returns what the capture layer saw.
fn bridge(refused: &RefusedSubscribes, target: &str, level: log::Level, message: &str) -> usize {
    let buffer = LogBuffer::new(100);
    let subscriber = tracing_subscriber::registry()
        .with(refused.clone())
        .with(LogCaptureLayer::new(buffer.clone()));
    tracing::subscriber::with_default(subscriber, || {
        // One statement: the `format_args!` temporary lives until its end.
        tracing_log::format_trace(
            &log::Record::builder()
                .target(target)
                .level(level)
                .args(format_args!("{message}"))
                .build(),
        )
        .expect("bridged log record");
    });
    buffer.recent("", 100).len()
}

const REFUSAL: &str = "event_loop Subscribe error: no app or stream name";

#[test]
fn the_no_publisher_refusal_is_dropped_and_counted() {
    let refused = RefusedSubscribes::new();
    assert_eq!(
        bridge(&refused, "streamhub", log::Level::Error, REFUSAL),
        0,
        "the refusal must not reach any layer"
    );
    assert_eq!(
        refused.take_summary().as_deref(),
        Some(
            "1 RTMP subscribes refused since the last report: nothing publishes the stream (#404)"
        )
    );
}

#[test]
fn other_streamhub_errors_still_pass() {
    let refused = RefusedSubscribes::new();
    assert_eq!(
        bridge(
            &refused,
            "streamhub",
            log::Level::Error,
            "event_loop Subscribe error: The receiver dropped."
        ),
        1
    );
    assert_eq!(refused.take_summary(), None, "nothing was refused");
}

#[test]
fn the_same_message_from_another_target_passes() {
    let refused = RefusedSubscribes::new();
    assert_eq!(bridge(&refused, "rtmp", log::Level::Error, REFUSAL), 1);
    assert_eq!(refused.take_summary(), None);
}

#[test]
fn the_same_message_below_error_passes() {
    let refused = RefusedSubscribes::new();
    assert_eq!(bridge(&refused, "streamhub", log::Level::Warn, REFUSAL), 1);
    assert_eq!(refused.take_summary(), None);
}

#[test]
fn a_native_tracing_event_with_the_streamhub_target_is_dropped_too() {
    let refused = RefusedSubscribes::new();
    let buffer = LogBuffer::new(100);
    let subscriber = tracing_subscriber::registry()
        .with(refused.clone())
        .with(LogCaptureLayer::new(buffer.clone()));
    tracing::subscriber::with_default(subscriber, || {
        tracing::error!(target: "streamhub", "event_loop Subscribe error: no app or stream name");
        tracing::info!(target: "rs_inpoint::rtmp_server", "still logged");
    });
    let seen = buffer.recent("", 100);
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].message, "still logged");
}

#[test]
fn the_summary_counts_since_the_last_report_and_resets() {
    let refused = RefusedSubscribes::new();
    assert_eq!(refused.take_summary(), None, "no refusal yet");
    for _ in 0..3 {
        bridge(&refused, "streamhub", log::Level::Error, REFUSAL);
    }
    assert_eq!(
        refused.take_summary().as_deref(),
        Some(
            "3 RTMP subscribes refused since the last report: nothing publishes the stream (#404)"
        )
    );
    assert_eq!(refused.take_summary(), None, "the report resets the count");
}

#[test]
fn the_summary_thread_reports_on_its_interval() {
    let refused = RefusedSubscribes::new();
    bridge(&refused, "streamhub", log::Level::Error, REFUSAL);
    bridge(&refused, "streamhub", log::Level::Error, REFUSAL);
    let (tx, rx) = std::sync::mpsc::channel();
    let handle = refused
        .spawn_summary(Duration::from_millis(10), move |line| {
            tx.send(line).expect("test receiver alive");
            false
        })
        .expect("summary thread starts");
    let line = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the summary thread reports the counted refusals");
    assert_eq!(
        line,
        "2 RTMP subscribes refused since the last report: nothing publishes the stream (#404)"
    );
    handle
        .join()
        .expect("the thread ends when emit returns false");
}
