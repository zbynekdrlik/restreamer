//! Room sound after a PipeWire restart (#420).
//!
//! The room Chrome plays through PulseAudio (pipewire-pulse on the stream PCs).
//! Its audio service connects once; when pipewire-pulse restarts (camera-box
//! installing the operator-headphone setup, 23.10.2026) that connection is gone
//! and the service never makes a new one: every page plays into nothing
//! (`pa_operation is nullptr` in chrome.stderr.log) until the service restarts.
//! Chrome starts a new audio service on the next sound after the old one ends,
//! so ending a service older than pipewire-pulse brings room sound back.

use std::path::Path;

/// One process, as read from `/proc`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proc {
    pub pid: u32,
    /// The NUL-separated command line, joined with spaces.
    pub cmdline: String,
    /// Start time in clock ticks since boot (`/proc/<pid>/stat` field 22).
    pub start: u64,
}

/// The start time from a `/proc/<pid>/stat` line. The command name (field 2)
/// sits in parentheses and may itself contain spaces or `)`.
pub fn stat_start(stat: &str) -> Option<u64> {
    let _ = stat;
    None
}

/// The audio services of the room Chrome (`user_data`) that started before
/// the newest pipewire-pulse: they hold a dead PulseAudio connection.
pub fn stale_audio_services(procs: &[Proc], user_data: &Path) -> Vec<u32> {
    let _ = (procs, user_data);
    Vec::new()
}

#[cfg(test)]
#[path = "audio_service_tests.rs"]
mod tests;
