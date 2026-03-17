use rs_core::log_buffer::{LogBuffer, LogEntry};
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
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);

        self.buffer.push(LogEntry {
            level: event.metadata().level().to_string(),
            target: event.metadata().target().to_string(),
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
}
