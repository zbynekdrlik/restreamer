//! #84 — long-stream warning helpers.
//!
//! A single delivery running much longer than `delivery.long_stream_warn_secs`
//! (default 2.5 h) usually means the operator left the stream on after the
//! event finished. This module holds the PURE, clock-injectable pieces shared
//! by the delivery health monitor (rs-api) so the once-per-delivery arming and
//! the elapsed-time parsing are unit-testable in isolation, without tokio or a
//! live DB:
//!
//! - [`elapsed_secs`] — parse a delivery instance's `created_at` and return the
//!   whole seconds elapsed at a caller-supplied `now` (the "fake clock").
//! - [`LongStreamWarner`] — once-per-delivery arming. A fresh instance is
//!   created per delivery-monitor loop (which lives exactly one delivery), so
//!   it re-arms automatically when a delivery stops and a new one starts.
//! - [`is_long_running`] — the async banner-flag helper, called by BOTH
//!   `get_status` paths (the HTTP handler and the Tauri IPC command).

use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::SqlitePool;

use crate::models::StreamingEvent;

/// Whole seconds elapsed between a delivery instance's `created_at` and `now`.
///
/// `created_at` is written by SQLite as `datetime('now')`, i.e.
/// `"YYYY-MM-DD HH:MM:SS"` in UTC with no zone suffix; an RFC 3339 value is
/// also accepted defensively (a future writer). Returns `None` for an
/// unparseable timestamp OR a `created_at` in the future relative to `now`
/// (clock skew — a just-inserted row yields `0`, not `None`) — callers treat
/// `None` as "not long-running", never as an error.
pub fn elapsed_secs(created_at: &str, now: DateTime<Utc>) -> Option<u64> {
    let created = parse_utc(created_at)?;
    let secs = (now - created).num_seconds();
    if secs < 0 { None } else { Some(secs as u64) }
}

/// Parse either the SQLite `datetime('now')` format or RFC 3339 into UTC.
fn parse_utc(s: &str) -> Option<DateTime<Utc>> {
    let s = s.trim();
    // Primary: SQLite `datetime('now')` -> "YYYY-MM-DD HH:MM:SS" (UTC, no zone).
    if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc));
    }
    // Defensive: an RFC 3339 timestamp (e.g. a future emitter).
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    None
}

/// Once-per-delivery arming for the long-stream warning.
///
/// Live for exactly one delivery (the delivery-monitor loop owns it and the
/// loop lives one delivery), so a fresh warner per delivery gives the desired
/// "fire ONCE per event, re-arm on stop" behaviour for free.
#[derive(Debug, Default)]
pub struct LongStreamWarner {
    fired: bool,
}

impl LongStreamWarner {
    pub fn new() -> Self {
        Self { fired: false }
    }

    /// Returns `true` EXACTLY ONCE — the first observation where the delivery
    /// has run at least `threshold_secs`. Returns `false` on every observation
    /// before the crossing and on every observation after it has fired. A
    /// `threshold_secs` of `0` disables the warning entirely (never fires).
    pub fn observe(&mut self, elapsed_secs: u64, threshold_secs: u64) -> bool {
        if threshold_secs == 0 || self.fired {
            return false;
        }
        if elapsed_secs >= threshold_secs {
            self.fired = true;
            true
        } else {
            false
        }
    }

    /// Whether the warning has already fired for this delivery.
    pub fn has_fired(&self) -> bool {
        self.fired
    }
}

/// Banner-flag helper: is the CURRENT delivery running longer than the
/// operator's threshold right now? Reflects live state (auto-clears the moment
/// delivery stops), so both `get_status` paths compute the dashboard's
/// `long_stream_warning` flag through this one function.
///
/// Returns `false` (banner hidden) when `threshold_secs == 0`, no event is
/// delivering, or the delivery instance / its timestamp cannot be read.
pub async fn is_long_running(
    pool: &SqlitePool,
    event: Option<&StreamingEvent>,
    threshold_secs: u64,
    now: DateTime<Utc>,
) -> bool {
    if threshold_secs == 0 {
        return false;
    }
    let Some(evt) = event else {
        return false;
    };
    if !evt.delivering_activated {
        return false;
    }
    match crate::db::get_delivery_instance_by_event(pool, evt.id).await {
        Ok(Some(inst)) => elapsed_secs(&inst.created_at, now).is_some_and(|e| e >= threshold_secs),
        _ => false,
    }
}

/// [`is_long_running`] against the real wall clock — the form the live
/// handlers use so callers need no `chrono` dependency of their own.
pub async fn is_long_running_now(
    pool: &SqlitePool,
    event: Option<&StreamingEvent>,
    threshold_secs: u64,
) -> bool {
    is_long_running(pool, event, threshold_secs, Utc::now()).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(s: &str) -> DateTime<Utc> {
        // Fake clock helper: build a fixed UTC instant from the SQLite format.
        let ndt = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap();
        Utc.from_utc_datetime(&ndt)
    }

    #[test]
    fn elapsed_secs_parses_sqlite_format() {
        let now = at("2026-09-02 12:00:00");
        // 2.5 h earlier.
        assert_eq!(elapsed_secs("2026-09-02 09:30:00", now), Some(9000));
        assert_eq!(elapsed_secs("2026-09-02 12:00:00", now), Some(0));
        assert_eq!(elapsed_secs("2026-09-02 11:59:01", now), Some(59));
    }

    #[test]
    fn elapsed_secs_parses_rfc3339() {
        let now = at("2026-09-02 12:00:00");
        assert_eq!(elapsed_secs("2026-09-02T09:30:00Z", now), Some(9000));
    }

    #[test]
    fn elapsed_secs_none_for_future_or_garbage() {
        let now = at("2026-09-02 12:00:00");
        // created_at in the future (clock skew / just inserted) -> None.
        assert_eq!(elapsed_secs("2026-09-02 12:00:05", now), None);
        assert_eq!(elapsed_secs("not-a-timestamp", now), None);
        assert_eq!(elapsed_secs("", now), None);
    }

    #[test]
    fn warner_fires_exactly_once_at_the_crossing() {
        let mut w = LongStreamWarner::new();
        let threshold = 9000;
        // Simulated fake-clock progression of elapsed seconds.
        assert!(!w.observe(0, threshold), "no warn at start");
        assert!(!w.observe(8999, threshold), "no warn one second before");
        assert!(!w.has_fired());
        assert!(w.observe(9000, threshold), "warns exactly at the threshold");
        assert!(w.has_fired());
        // Every later observation is silent — once per delivery.
        assert!(!w.observe(9001, threshold));
        assert!(!w.observe(20000, threshold));
    }

    #[test]
    fn warner_fires_once_when_first_seen_already_over() {
        // The loop may first observe an already-long delivery (e.g. after an
        // app restart mid-event). It must still fire exactly once.
        let mut w = LongStreamWarner::new();
        assert!(w.observe(12345, 9000));
        assert!(!w.observe(12346, 9000));
    }

    #[test]
    fn warner_disabled_by_zero_threshold() {
        let mut w = LongStreamWarner::new();
        assert!(!w.observe(1_000_000, 0), "threshold 0 disables the warning");
        assert!(!w.has_fired());
    }

    // --- is_long_running (the banner-flag helper both get_status paths call) ---

    fn evt(id: i64, delivering: bool) -> StreamingEvent {
        StreamingEvent {
            id,
            name: "e".to_string(),
            received_bytes: 0,
            receiving_activated: true,
            delivering_activated: delivering,
            cache_delay_secs: None,
            created_from: None,
            rescue_video_url: None,
        }
    }

    async fn setup_pool() -> sqlx::SqlitePool {
        let pool = crate::db::create_memory_pool().await.unwrap();
        crate::db::run_migrations(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn is_long_running_covers_every_branch() {
        let pool = setup_pool().await;
        let now = at("2026-01-01 03:00:00");

        // No event -> false.
        assert!(!is_long_running(&pool, None, 9000, now).await);

        let event_id = crate::db::create_streaming_event(&pool, "evt")
            .await
            .unwrap();

        // Delivering NOT activated -> false, even with an old instance present.
        let inst = crate::db::create_delivery_instance(
            &pool,
            1,
            "d",
            "1.2.3.4",
            "cx23",
            Some(event_id),
            "tok",
        )
        .await
        .unwrap();
        sqlx::query(
            "UPDATE delivery_instances SET created_at = '2026-01-01 00:00:00' WHERE id = ?1",
        )
        .bind(inst)
        .execute(&pool)
        .await
        .unwrap();
        assert!(!is_long_running(&pool, Some(&evt(event_id, false)), 9000, now).await);

        // Delivering + instance created 3h (10800s) ago, threshold 9000 -> true.
        assert!(is_long_running(&pool, Some(&evt(event_id, true)), 9000, now).await);

        // Threshold 0 disables it even when long-running.
        assert!(!is_long_running(&pool, Some(&evt(event_id, true)), 0, now).await);

        // Not yet over the threshold (only 1h elapsed at this `now`) -> false.
        let early = at("2026-01-01 01:00:00");
        assert!(!is_long_running(&pool, Some(&evt(event_id, true)), 9000, early).await);
    }

    #[tokio::test]
    async fn is_long_running_false_when_no_instance() {
        let pool = setup_pool().await;
        let event_id = crate::db::create_streaming_event(&pool, "evt")
            .await
            .unwrap();
        // Delivering flag on, but no delivery instance row exists yet.
        let now = at("2026-01-01 03:00:00");
        assert!(!is_long_running(&pool, Some(&evt(event_id, true)), 9000, now).await);
    }
}
