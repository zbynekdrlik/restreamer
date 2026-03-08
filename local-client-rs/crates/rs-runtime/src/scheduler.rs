use std::time::Duration;

use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tracing::{debug, error, info};

use rs_core::db;
use rs_core::models::WsEvent;

/// Periodically checks the `scheduled_streams` table and triggers
/// event activation when a schedule's `next_run_at` has passed.
pub struct Scheduler {
    pool: SqlitePool,
    ws_tx: broadcast::Sender<WsEvent>,
    interval: Duration,
}

impl Scheduler {
    pub fn new(pool: SqlitePool, ws_tx: broadcast::Sender<WsEvent>) -> Self {
        Self {
            pool,
            ws_tx,
            interval: Duration::from_secs(60),
        }
    }

    /// Run the scheduler until shutdown.
    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) {
        info!("Scheduler started (interval: {:?})", self.interval);

        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Scheduler shutting down");
                    break;
                }
                _ = tokio::time::sleep(self.interval) => {
                    self.check_schedules().await;
                }
            }
        }
    }

    async fn check_schedules(&self) {
        let now = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        let due = match db::get_due_scheduled_streams(&self.pool, &now).await {
            Ok(schedules) => schedules,
            Err(e) => {
                error!("Failed to query due schedules: {e}");
                return;
            }
        };

        for schedule in due {
            info!(
                "Schedule {} triggered for event {}",
                schedule.id, schedule.event_id
            );

            // Activate receiving on the event
            if let Err(e) = db::set_receiving_activated(&self.pool, schedule.event_id, true).await {
                error!(
                    "Failed to activate event {} for schedule {}: {e}",
                    schedule.event_id, schedule.id
                );
                continue;
            }

            // Compute next_run_at based on repeat_interval
            let (next_run, still_enabled) = match schedule.repeat_interval.as_deref() {
                Some("weekly") => {
                    // Advance by 7 days from the current next_run_at
                    let next = advance_by_days(schedule.next_run_at.as_deref(), 7);
                    (next, true)
                }
                Some("daily") => {
                    let next = advance_by_days(schedule.next_run_at.as_deref(), 1);
                    (next, true)
                }
                _ => {
                    // One-shot schedule: disable after execution
                    (None, false)
                }
            };

            if let Err(e) = db::mark_scheduled_stream_run(
                &self.pool,
                schedule.id,
                &now,
                next_run.as_deref(),
                still_enabled,
            )
            .await
            {
                error!("Failed to mark schedule {} as run: {e}", schedule.id);
            }

            // Broadcast the trigger event
            if let Err(e) = self.ws_tx.send(WsEvent::ScheduleTriggered {
                schedule_id: schedule.id,
                event_id: schedule.event_id,
            }) {
                debug!("No WS subscribers for ScheduleTriggered: {e}");
            }
        }
    }
}

/// Advance a datetime string by the given number of days.
/// Returns None if the input is None or unparseable.
fn advance_by_days(datetime_str: Option<&str>, days: i64) -> Option<String> {
    let s = datetime_str?;
    let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok()?;
    let advanced = dt + chrono::Duration::days(days);
    Some(advanced.format("%Y-%m-%d %H:%M:%S").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_core::db;

    async fn setup_db() -> SqlitePool {
        let pool = db::create_pool(std::path::Path::new(":memory:"))
            .await
            .unwrap();
        db::run_migrations(&pool).await.unwrap();
        db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
        pool
    }

    #[tokio::test]
    async fn scheduler_shuts_down_cleanly() {
        let pool = setup_db().await;
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);

        let scheduler = Scheduler {
            pool,
            ws_tx,
            interval: Duration::from_millis(50),
        };

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let handle = tokio::spawn(async move { scheduler.run(shutdown_rx).await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = shutdown_tx.send(());

        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("scheduler timed out")
            .expect("scheduler panicked");
    }

    #[tokio::test]
    async fn check_schedules_with_no_due_does_nothing() {
        let pool = setup_db().await;
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);

        let scheduler = Scheduler {
            pool,
            ws_tx,
            interval: Duration::from_millis(50),
        };

        // Should not panic or error with empty table
        scheduler.check_schedules().await;
    }

    #[tokio::test]
    async fn check_schedules_triggers_due_schedule() {
        let pool = setup_db().await;
        let (ws_tx, mut ws_rx) = broadcast::channel::<WsEvent>(16);

        // Create event and schedule
        let event_id =
            db::upsert_streaming_event(&pool, "sched-evt", Some("Scheduled"), "127.0.0.1")
                .await
                .unwrap();

        // Disable receiving first so we can verify it gets activated
        db::set_receiving_activated(&pool, event_id, false)
            .await
            .unwrap();

        // Create a schedule that's already due (start_time in the past)
        db::create_scheduled_stream(&pool, event_id, "2020-01-01 00:00:00", None)
            .await
            .unwrap();

        let scheduler = Scheduler {
            pool: pool.clone(),
            ws_tx,
            interval: Duration::from_millis(50),
        };

        scheduler.check_schedules().await;

        // Verify receiving was activated
        let event = db::get_streaming_event(&pool).await.unwrap().unwrap();
        assert!(
            event.receiving_activated,
            "Receiving should be activated by scheduler"
        );

        // Verify WS event was sent
        let ws_event = ws_rx.try_recv().unwrap();
        match ws_event {
            WsEvent::ScheduleTriggered {
                schedule_id,
                event_id: eid,
            } => {
                assert!(schedule_id > 0);
                assert_eq!(eid, event_id);
            }
            other => panic!("Expected ScheduleTriggered, got: {other:?}"),
        }
    }

    #[test]
    fn advance_by_days_weekly() {
        let result = advance_by_days(Some("2026-03-01 10:00:00"), 7);
        assert_eq!(result, Some("2026-03-08 10:00:00".to_string()));
    }

    #[test]
    fn advance_by_days_daily() {
        let result = advance_by_days(Some("2026-03-01 10:00:00"), 1);
        assert_eq!(result, Some("2026-03-02 10:00:00".to_string()));
    }

    #[test]
    fn advance_by_days_none() {
        assert_eq!(advance_by_days(None, 7), None);
    }

    #[test]
    fn advance_by_days_invalid() {
        assert_eq!(advance_by_days(Some("not-a-date"), 7), None);
    }
}
