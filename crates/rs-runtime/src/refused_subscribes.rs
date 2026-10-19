//! #404: drop xiu's ERROR for a subscribe to a stream nobody publishes.
//!
//! streamhub 0.2.4 logs a subscribe to an unpublished stream as
//! `log::error!("event_loop Subscribe error: no app or stream name")`. Two
//! sources hit it while OBS is not publishing: camera-box's headphone monitor,
//! which plays `live/stream-pp` every ~3 s on purpose, and the inpoint's own
//! probe subscribes (`local-chunker`). On stream-pp that line came 7203 times
//! in 6 h and buried the real errors. This layer drops exactly that event (every other streamhub
//! error keeps ERROR) and counts it; the summary thread reports the count at
//! INFO, so the fact stays visible.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// The `log` target xiu's streamhub crate logs under.
const STREAMHUB_TARGET: &str = "streamhub";
/// The metadata target tracing-log gives every bridged `log` record.
const LOG_BRIDGE_TARGET: &str = "log";
/// streamhub's message for a subscribe to a stream nobody publishes.
const REFUSAL_MESSAGE: &str = "event_loop Subscribe error: no app or stream name";

/// How often the refusal count is logged.
const SUMMARY_INTERVAL: Duration = Duration::from_secs(60);

/// A global filter layer: drops streamhub's "no publisher" refusal and
/// counts it. Clones share the count.
#[derive(Clone, Default)]
pub struct RefusedSubscribes {
    refused: Arc<AtomicU64>,
}

impl RefusedSubscribes {
    /// The summary line for the refusals counted since the last call, or
    /// `None` when there were none. Resets the count.
    pub fn take_summary(&self) -> Option<String> {
        let refused = self.refused.swap(0, Ordering::Relaxed);
        (refused > 0).then(|| {
            format!(
                "{refused} RTMP subscribes refused since the last report: \
                 the stream had no publisher (#404)"
            )
        })
    }

    /// Starts the thread that logs the summary at INFO every
    /// `SUMMARY_INTERVAL` (60 s).
    pub fn start_summary(&self) -> std::io::Result<JoinHandle<()>> {
        self.spawn_summary(SUMMARY_INTERVAL, |line| {
            tracing::info!("{line}");
            true
        })
    }

    /// Every `every`, passes the summary (when there is one) to `emit`, on
    /// its own thread. The thread ends when `emit` returns false.
    fn spawn_summary(
        &self,
        every: Duration,
        mut emit: impl FnMut(String) -> bool + Send + 'static,
    ) -> std::io::Result<JoinHandle<()>> {
        let this = self.clone();
        std::thread::Builder::new()
            .name("refused-subscribes".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(every);
                    if let Some(line) = this.take_summary()
                        && !emit(line)
                    {
                        return;
                    }
                }
            })
    }
}

impl<S: Subscriber> Layer<S> for RefusedSubscribes {
    /// Returning false here drops the event for the WHOLE stack (stdout, the
    /// log file, the dashboard capture), not just for this layer.
    fn event_enabled(&self, event: &Event<'_>, _ctx: Context<'_, S>) -> bool {
        let meta_target = event.metadata().target();
        if *event.metadata().level() != Level::ERROR
            || (meta_target != LOG_BRIDGE_TARGET && meta_target != STREAMHUB_TARGET)
        {
            return true;
        }
        let mut fields = RefusalFields::default();
        event.record(&mut fields);
        // A `log` record arrives with the metadata target "log" and its real
        // target in the `log.target` field (tracing-log).
        let target = fields.log_target.as_deref().unwrap_or(meta_target);
        if target == STREAMHUB_TARGET && fields.message == REFUSAL_MESSAGE {
            self.refused.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        true
    }
}

/// The two fields of an event the refusal check reads.
#[derive(Default)]
struct RefusalFields {
    message: String,
    log_target: Option<String>,
}

impl Visit for RefusalFields {
    fn record_str(&mut self, field: &Field, value: &str) {
        match field.name() {
            "message" => self.message = value.to_owned(),
            "log.target" => self.log_target = Some(value.to_owned()),
            _ => {}
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

#[cfg(test)]
#[path = "refused_subscribes_tests.rs"]
mod tests;
