//! `rust_rescue_push` — pure-rust rescue push loop.
//!
//! Replaces the legacy `rescue::run_rescue_loop` (which spawns an `ffmpeg`
//! process to push a looping rescue video over RTMP). This module instead
//! pushes pre-encoded FLV bytes — typically the `DEFAULT_RESCUE_FLV` blob
//! produced by `rescue_default`, or a custom S3-fetched FLV — through
//! `rs_rtmp_push::RtmpPusher::push_flv_bytes` until either the buffer
//! genuinely refills (producer-active AND fresh chunks queued for
//! `RESCUE_REFILL_TARGET_SECS` continuous seconds — #289) or a stop signal
//! arrives.
//!
//! ## Pacing
//!
//! `RtmpPusher::push_flv_bytes` paces internally via `CATCHUP_FACTOR_PCT`
//! (120 → max 1.2× realtime) — each call returns after the FLV blob's
//! media duration of wall time (modulo catch-up). The loop here therefore
//! does **not** add an external `tokio::time::sleep` to throttle: doing
//! so would oversleep on top of internal pacing and the pusher would
//! send dead air. Drive the loop with `push_flv_bytes` as the awaitable.
//!
//! ## Connection
//!
//! `RtmpPusher::new` is synchronous and takes `(url, PusherConfig)`; the
//! first `push_flv_bytes` call lazy-connects via `Session::connect`.
//! After an error the next call reconnects automatically (the `session`
//! Option is cleared internally). No external reconnect-bookkeeping
//! needed here — the loop only adds a small backoff on the error path
//! so we don't hot-spin on persistent connect failures (e.g. while the
//! upstream RTMP endpoint is down).
//!
//! ## Exit conditions
//!
//! Returns `true` when a stop signal arrives (caller should exit the
//! whole endpoint task), or `false` when `producer_active` has stayed
//! `true` AND `highest_sent_chunk_id` has advanced (genuinely fresh chunks
//! queued) for `RESCUE_REFILL_TARGET_SECS` continuous wall-seconds — proving
//! OBS is back and the cache window is really refilling, not merely that the
//! producer_active flag flapped true over a stalled producer (#289).

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
// `tokio::time::Instant` (NOT `std::time::Instant`) so the refill counter's
// `last_check.elapsed()` shares the loop's `tokio::time::sleep` / pusher
// time source: identical to the real clock in prod, but it honors
// `tokio::time::advance()` under `#[tokio::test(start_paused = true)]`. With
// `std::time::Instant` the paused-time test `rescue_push_resumes_normal_when_
// producer_recovers` reads the real wall clock (advances only microseconds),
// so `continuous_active_ms` never reaches `RESCUE_REFILL_TARGET_SECS` (120)
// and the loop never exits. Production behaviour is unchanged.
use tokio::time::Instant;

use rs_ffmpeg::ServiceType;
use rs_rtmp_push::{PusherConfig, RtmpPusher};

use crate::buffer_state::BufferState;
use crate::endpoint_rtmp_url::build_rtmp_url;
use crate::endpoint_stats::Stats;
// Canonical home for the refill-target constant is `rescue.rs` — that's
// the legacy public name referenced across the crate. Task 6 (the
// run_rescue_loop GREEN commit) folded the legacy ffmpeg rescue loop's
// body to delegate here, but the constant stays in `rescue` so existing
// `crate::rescue::RESCUE_REFILL_TARGET_SECS` call sites keep working
// without churn.
use crate::rescue::RESCUE_REFILL_TARGET_SECS;

/// Backoff applied after a `push_flv_bytes` error before the next attempt.
/// Avoids tight error loops when the upstream RTMP endpoint is unreachable.
/// The pusher itself lazy-reconnects on the next call after an error.
const ERROR_BACKOFF: Duration = Duration::from_millis(500);

/// Selects whether `rust_rescue_push` owns the stats fields it would
/// otherwise overwrite each tick. See review finding #2 (warmup race).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RescuePushMode {
    /// Outage rescue (cache-drain or recv-None defensive). This caller
    /// owns `stats.delivery_mode` + `stats.rescue_eta_secs` and the
    /// pusher updates them every iteration.
    Outage,
    /// Warmup background push. The warmup probe loop in `run_warmup_loop`
    /// owns `stats.delivery_mode = "warmup"` + `rescue_eta_secs`; this
    /// mode keeps the pusher inert for stats so the two writers don't
    /// race and make the dashboard flicker between "warmup" and
    /// "rescue"/"recovering".
    Warmup,
}

/// Loop a pre-encoded FLV blob through `RtmpPusher` until stop or refill.
///
/// Returns `true` if stop signal received, `false` once the producer has been
/// active AND queuing genuinely fresh chunks for `RESCUE_REFILL_TARGET_SECS`
/// continuous wall-seconds (#289).
///
/// When `mode == RescuePushMode::Outage`, `stats.delivery_mode` is updated
/// each tick:
/// - `"rescue"` while the producer is stalled OR flapping active without
///   queuing fresh chunks (a bare flag flap — #289)
/// - `"recovering"` while the producer is active AND fresh chunks are being
///   queued but the refill window has not yet completed
///
/// and `stats.rescue_eta_secs` is updated each tick with seconds remaining
/// until refill (saturating to 0).
///
/// When `mode == RescuePushMode::Warmup` the pusher does NOT touch stats —
/// the warmup probe loop owns those fields.
#[allow(clippy::too_many_arguments)]
pub async fn rust_rescue_push(
    alias: &str,
    service_type: ServiceType,
    stream_key: &str,
    flv_bytes: Arc<Vec<u8>>,
    buffer_state: Arc<BufferState>,
    stats: Stats,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    mode: RescuePushMode,
) -> bool {
    // Production path: dial the real RTMP server. The byte-pushing loop is
    // factored into `rust_rescue_push_with_pusher` so tests can inject a
    // recording `Pushable` and prove the rescue clip bytes are actually
    // pushed without standing up a real RTMP server (#239). This wrapper is
    // the ONLY place that constructs the concrete `RtmpPusher`, so the
    // production behaviour is byte-identical to before the extraction.
    let url = build_rtmp_url(service_type, stream_key);
    tracing::info!(
        alias,
        url = %url,
        flv_len = flv_bytes.len(),
        "rust_rescue_push: starting rust rescue loop"
    );
    let pusher = RtmpPusher::new(url, PusherConfig::default());
    rust_rescue_push_with_pusher(pusher, alias, flv_bytes, buffer_state, stats, stop_rx, mode).await
}

/// Inner rescue push loop, generic over the `Pushable` so tests can inject a
/// recording mock. `pusher` is already constructed (and, for the real path,
/// not yet connected — the first `push_flv_bytes` lazy-connects). Returns the
/// same contract as `rust_rescue_push`: `true` on stop, `false` once the
/// producer has been active AND queuing genuinely fresh chunks for
/// `RESCUE_REFILL_TARGET_SECS` continuous wall-seconds (refill complete — #289).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn rust_rescue_push_with_pusher<P: crate::pushable::Pushable>(
    mut pusher: P,
    alias: &str,
    flv_bytes: Arc<Vec<u8>>,
    buffer_state: Arc<BufferState>,
    stats: Stats,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    mode: RescuePushMode,
) -> bool {
    let mut continuous_active_ms: u64 = 0;
    let mut last_check = Instant::now();
    // #289: the high-water mark of chunks the producer had SENT into the
    // prefetch channel at the start of the current continuous-active window.
    // Outage rescue must exit only on GENUINE resumed live delivery, never on
    // a bare `producer_active` flag flip. During a sustained/trickle outage the
    // #237 producer-respawn churn (and a stale-tail re-fetch) can hold
    // `producer_active` true for the whole refill window WITHOUT any fresh
    // content: the respawn resumes PAST the live edge, finds nothing, and
    // `highest_sent_chunk_id` (a `fetch_max`, capped at the pre-outage edge)
    // never advances. Keying the exit on the flag alone therefore dropped
    // rescue back to "normal" onto a dark/stuttering stream (issue #289).
    // Requiring `highest_sent_chunk_id` to have ADVANCED during the active
    // window proves genuinely new chunks were queued — the cache is really
    // refilling — before we resume normal delivery.
    let mut active_window_start_sent = buffer_state.highest_sent_chunk_id.load(Ordering::Relaxed);

    loop {
        tokio::select! {
            res = pusher.push_flv_bytes(&flv_bytes) => {
                let push_ok = res.is_ok();
                if let Err(e) = res {
                    tracing::warn!(alias, "rust_rescue_push: push error: {e}; backing off");
                    // Backoff but observe stop signal so shutdown latency
                    // stays bounded by the awaited future, not 500ms.
                    tokio::select! {
                        _ = tokio::time::sleep(ERROR_BACKOFF) => {}
                        _ = stop_rx.changed() => {
                            if *stop_rx.borrow() { return true; }
                        }
                    }
                }

                // After each pace-paced push (or backoff after error), update
                // the refill bookkeeping. Counting elapsed wall time here
                // rather than the FLV's media duration keeps the exit
                // condition aligned with the legacy ffmpeg rescue loop
                // (which polled at 5-second wall-clock intervals).
                // Count elapsed in MILLISECONDS: pushes can be faster than 1s
                // (the real pusher paces ~1x but a short clip / fast segment can
                // return sub-second), and `as_secs()` would truncate each such
                // push to 0 — so the counter would never grow and recovery would
                // never complete, leaving the endpoint stuck in rescue forever.
                let elapsed_ms = last_check.elapsed().as_millis() as u64;
                last_check = Instant::now();
                let active = buffer_state.producer_active.load(Ordering::Relaxed);
                let sent = buffer_state.highest_sent_chunk_id.load(Ordering::Relaxed);
                if active {
                    continuous_active_ms =
                        continuous_active_ms.saturating_add(elapsed_ms);
                } else {
                    // Producer stalled again: reset BOTH the refill timer and
                    // the fresh-chunk baseline, so the NEXT active window is
                    // measured from the current (frozen) high-water mark.
                    continuous_active_ms = 0;
                    active_window_start_sent = sent;
                }
                // #289: fresh chunks the producer has queued since the current
                // continuous-active window began. > 0 iff genuinely new content
                // appeared past the pre-window live edge (real recovery); stays
                // 0 while `producer_active` merely flaps true over a stalled
                // producer (respawn churn / stale-tail re-fetch).
                let fresh_chunks = sent.saturating_sub(active_window_start_sent);
                let refilling = active && fresh_chunks > 0;
                let eta = RESCUE_REFILL_TARGET_SECS
                    .saturating_sub(continuous_active_ms / 1000);

                // Review finding #2: only the Outage caller owns these
                // stats fields. Warmup's probe loop is the canonical
                // writer during warmup; writing here too races and
                // makes the dashboard flicker between "warmup" and
                // "rescue"/"recovering".
                if matches!(mode, RescuePushMode::Outage) {
                    let mut s = stats.lock().await;
                    // #289: "recovering" only when REAL fresh delivery is
                    // resuming (producer active AND fresh chunks queued);
                    // otherwise stay "rescue". Both are banner-worthy
                    // (#263/#288), so the calm outage UI never drops
                    // mid-outage — but the dashboard no longer claims
                    // "recovering" on a bare `producer_active` flag flap.
                    s.delivery_mode = if refilling {
                        "recovering".to_string()
                    } else {
                        "rescue".to_string()
                    };
                    s.rescue_eta_secs = Some(eta);
                    // #284/#238: a rescue-clip push IS a successful push —
                    // last_push_ok_age_ms keeps advancing during an outage,
                    // which is exactly what the crash-exhaustion E2E gate
                    // asserts ("rescue is live", not just "rescue flagged").
                    if push_ok {
                        s.last_push_ok_unix_ms =
                            Some(crate::endpoint_stats::unix_ms_now());
                    }
                }

                // #289: exit rescue ONLY on a full continuous-active window
                // WITH genuinely fresh delivery — never on `producer_active`
                // alone. A stuck flag with no fresh chunks (fresh_chunks == 0)
                // keeps rescue latched so the stream never resumes dark.
                if continuous_active_ms >= RESCUE_REFILL_TARGET_SECS.saturating_mul(1000)
                    && fresh_chunks > 0
                {
                    tracing::info!(
                        alias,
                        continuous_active_ms,
                        fresh_chunks,
                        "rust_rescue_push: producer active + fresh chunks flowing, exiting rescue"
                    );
                    return false;
                }
            }
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    tracing::info!(alias, "rust_rescue_push: stop signal received");
                    return true;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoint_stats::EndpointStats;
    use std::sync::Arc;
    use tokio::sync::{Mutex, watch};

    /// Verify the loop exits within a short timeout when the stop signal is
    /// already set before the function is called. We deliberately do NOT
    /// try to test the real RTMP push path here — that requires a test
    /// server. We only verify the cancellation contract.
    ///
    /// Pre-sending `true` on the watch channel means `stop_rx.changed()`
    /// resolves immediately on first await (watch tracks unseen values),
    /// so the `tokio::select!` exits before `push_flv_bytes` ever finishes
    /// its TCP connect attempt.
    #[tokio::test]
    async fn stop_signal_exits_immediately() {
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let buffer_state = Arc::new(BufferState::default());
        let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
        // Minimal valid-looking FLV header bytes (FLV signature + version +
        // flags + header length). Content doesn't matter — the test never
        // actually pushes because stop_rx wins the select.
        let flv_bytes = Arc::new(vec![b'F', b'L', b'V', 0x01, 0x05, 0, 0, 0, 9, 0, 0, 0, 0]);

        // Send stop BEFORE calling so the loop short-circuits on first poll.
        stop_tx.send(true).expect("stop_tx send");

        // Use a bogus stream_key against TestFile — no upstream needed.
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            rust_rescue_push(
                "test-alias",
                ServiceType::TestFile,
                "test-key",
                flv_bytes,
                buffer_state,
                stats,
                &mut stop_rx,
                RescuePushMode::Outage,
            ),
        )
        .await;

        assert!(
            result.is_ok(),
            "rust_rescue_push must exit within 5s on stop signal"
        );
        assert!(
            result.unwrap(),
            "rust_rescue_push must return true for stop signal"
        );
    }
}
