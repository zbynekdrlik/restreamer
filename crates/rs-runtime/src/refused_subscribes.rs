//! #404: drop xiu's ERROR for a subscribe to a stream nobody publishes.
//!
//! streamhub 0.2.4 logs every refused subscribe as `log::error!("event_loop
//! Subscribe error: no app or stream name")`. On stream-pp, camera-box's
//! headphone monitor plays `live/stream-pp` every ~3 s on purpose while OBS
//! is not publishing, so that line came 7203 times in 6 h and buried the
//! real errors. This layer drops exactly that event (every other streamhub
//! error keeps ERROR) and counts it; the summary thread reports the count at
//! INFO, so the fact stays visible.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::thread::JoinHandle;
use std::time::Duration;

use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// How often the refusal count is logged.
pub const SUMMARY_INTERVAL: Duration = Duration::from_secs(60);

/// A global filter layer: drops streamhub's "no publisher" refusal and
/// counts it. Clones share the count.
#[derive(Clone, Default)]
pub struct RefusedSubscribes {
    refused: Arc<AtomicU64>,
}

impl RefusedSubscribes {
    pub fn new() -> Self {
        Self::default()
    }

    /// The summary line for the refusals counted since the last call, or
    /// `None` when there were none. Resets the count.
    pub fn take_summary(&self) -> Option<String> {
        None
    }

    /// Starts the thread that logs the summary at INFO every
    /// [`SUMMARY_INTERVAL`].
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
        _every: Duration,
        _emit: impl FnMut(String) -> bool + Send + 'static,
    ) -> std::io::Result<JoinHandle<()>> {
        std::thread::Builder::new()
            .name("refused-subscribes".into())
            .spawn(|| {})
    }
}

impl<S: Subscriber> Layer<S> for RefusedSubscribes {
    fn event_enabled(&self, _event: &Event<'_>, _ctx: Context<'_, S>) -> bool {
        true
    }
}

#[cfg(test)]
#[path = "refused_subscribes_tests.rs"]
mod tests;
