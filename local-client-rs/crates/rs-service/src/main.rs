mod poller;
mod service;
mod shutdown;

use anyhow::Context;
use tracing_subscriber::EnvFilter;

use rs_core::config::Config;
use service::ServiceRunner;

fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Load config
    let config_path = std::env::args()
        .nth(1)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(Config::default_path);

    let config = if config_path.exists() {
        Config::load(&config_path).context("failed to load config")?
    } else {
        tracing::warn!(
            "Config file not found at {}, using defaults",
            config_path.display()
        );
        Config::default()
    };

    config
        .validate()
        .map_err(|e| anyhow::anyhow!("Invalid config: {e}"))?;

    // Run the service
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async { ServiceRunner::new(config).run().await })
}
