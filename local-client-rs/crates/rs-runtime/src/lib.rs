//! Restreamer service runtime - embeddable service core for Tauri and standalone use.
//!
//! This crate provides the core service orchestration that can be embedded in:
//! - The standalone `restreamer-service` binary (Windows Service / console mode)
//! - The unified Tauri application with embedded service

mod log_capture;
mod orchestrator;
mod scheduler;
mod shutdown;

pub use log_capture::LogCaptureLayer;
pub use orchestrator::ServiceCore;
pub use shutdown::ShutdownCoordinator;
