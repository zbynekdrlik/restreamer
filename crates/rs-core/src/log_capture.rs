use crate::log_buffer::{LogBuffer, LogEntry};
use tracing::Subscriber;
use tracing::field::{Field, Visit};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;

/// Tracing layer that captures log events into a shared LogBuffer.
pub struct LogCaptureLayer {
    buffer: LogBuffer,
}

impl LogCaptureLayer {
    pub fn new(buffer: LogBuffer) -> Self {
        Self { buffer }
    }
}

impl<S: Subscriber> Layer<S> for LogCaptureLayer {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        // Skip DEBUG/TRACE from noisy HTTP crates to prevent buffer overflow
        let level = *event.metadata().level();
        let target = event.metadata().target();
        if level > tracing::Level::INFO
            && (target.starts_with("hyper")
                || target.starts_with("h2")
                || target.starts_with("rustls")
                || target.starts_with("reqwest"))
        {
            return;
        }

        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);

        self.buffer.push(LogEntry {
            level: level.to_string(),
            target: target.to_string(),
            message: visitor.message,
        });
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::prelude::*;

    #[test]
    fn captures_log_events() {
        let buffer = LogBuffer::new(100);
        let layer = LogCaptureLayer::new(buffer.clone());

        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(target: "rs_inpoint::rtmp", "server started");
            tracing::warn!(target: "rs_endpoint::s3", "upload retry");
        });

        let inpoint = buffer.recent("rs_inpoint", 10);
        assert_eq!(inpoint.len(), 1);
        assert!(inpoint[0].message.contains("server started"));

        let endpoint = buffer.recent("rs_endpoint", 10);
        assert_eq!(endpoint.len(), 1);
        assert!(endpoint[0].message.contains("upload retry"));
    }

    #[test]
    fn filters_hyper_trace_events() {
        let buffer = LogBuffer::new(100);
        let layer = LogCaptureLayer::new(buffer.clone());
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, || {
            tracing::trace!(target: "hyper::proto::h1", "received bytes");
            tracing::debug!(target: "h2::codec", "frame decoded");
            tracing::info!(target: "rs_delivery::endpoint_task", "ffmpeg started");
        });
        let all = buffer.recent("", 100);
        assert_eq!(all.len(), 1, "Should only capture INFO, not hyper TRACE/DEBUG");
        assert!(all[0].message.contains("ffmpeg started"));
    }
}
