//! Restreamer service runtime - embeddable service core for Tauri and standalone use.
//!
//! This crate provides the core service orchestration that can be embedded in:
//! - The standalone `restreamer-service` binary (Windows Service / console mode)
//! - The unified Tauri application with embedded service

pub mod daily_log;
pub mod ingest_priority;
pub mod ingest_runtime;
mod inpoint_service;
mod log_capture;
mod orchestrator;
pub mod rtmp_bind;
mod shutdown;
pub mod stall_detector;

pub use log_capture::LogCaptureLayer;
pub use orchestrator::ServiceCore;
pub use shutdown::ShutdownCoordinator;
