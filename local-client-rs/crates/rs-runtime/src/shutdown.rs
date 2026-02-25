use tokio::sync::broadcast;
use tracing::{debug, info};

/// Centralized shutdown coordination.
///
/// All service components subscribe to the shutdown signal. When `trigger()`
/// is called (from signal handler or Windows SCM), all components stop gracefully.
pub struct ShutdownCoordinator {
    tx: broadcast::Sender<()>,
}

impl ShutdownCoordinator {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1);
        Self { tx }
    }

    /// Get a receiver that will fire when shutdown is triggered.
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.tx.subscribe()
    }

    /// Trigger shutdown of all components.
    pub fn trigger(&self) {
        info!("Shutdown signal sent");
        match self.tx.send(()) {
            Ok(n) => debug!("Shutdown signal delivered to {n} receivers"),
            Err(_) => debug!("Shutdown signal sent but no active receivers"),
        }
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_notifies_subscribers() {
        let coordinator = ShutdownCoordinator::new();
        let mut rx1 = coordinator.subscribe();
        let mut rx2 = coordinator.subscribe();

        coordinator.trigger();

        assert!(rx1.recv().await.is_ok());
        assert!(rx2.recv().await.is_ok());
    }

    #[tokio::test]
    async fn multiple_triggers_do_not_panic() {
        let coordinator = ShutdownCoordinator::new();
        // Subscribe so the channel has a receiver
        let _rx = coordinator.subscribe();

        // Multiple triggers should not panic
        coordinator.trigger();
        coordinator.trigger();
    }
}
