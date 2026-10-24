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
    // Fields after the name start at field 3 (state); field 22 is the 20th of them.
    let (_, rest) = stat.rsplit_once(')')?;
    rest.split_whitespace().nth(19)?.parse().ok()
}

/// The audio services of the room Chrome (`user_data`) that started before
/// the newest pipewire-pulse: they hold a dead PulseAudio connection.
pub fn stale_audio_services(procs: &[Proc], user_data: &Path) -> Vec<u32> {
    let Some(pulse) = procs
        .iter()
        .filter(|p| {
            p.cmdline.split(' ').next().is_some_and(|exe| {
                Path::new(exe).file_name() == Some(std::ffi::OsStr::new("pipewire-pulse"))
            })
        })
        .map(|p| p.start)
        .max()
    else {
        return Vec::new();
    };
    let ours = format!("--user-data-dir={}", user_data.display());
    procs
        .iter()
        .filter(|p| {
            let has = |arg: &str| p.cmdline.split(' ').any(|a| a == arg);
            has("--utility-sub-type=audio.mojom.AudioService") && has(&ours) && p.start < pulse
        })
        .map(|p| p.pid)
        .collect()
}

/// Every process of this user readable in `/proc`.
pub fn read_procs() -> Vec<Proc> {
    let Ok(dir) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    dir.flatten()
        .filter_map(|e| {
            let pid: u32 = e.file_name().to_str()?.parse().ok()?;
            let raw = std::fs::read(e.path().join("cmdline")).ok()?;
            let cmdline = String::from_utf8_lossy(&raw)
                .split('\0')
                .filter(|a| !a.is_empty())
                .collect::<Vec<_>>()
                .join(" ");
            let start = stat_start(&std::fs::read_to_string(e.path().join("stat")).ok()?)?;
            Some(Proc {
                pid,
                cmdline,
                start,
            })
        })
        .collect()
}

/// End the room Chrome's audio services that predate pipewire-pulse; Chrome
/// starts a fresh one, connected to the running sound server, on the next
/// sound. `procs` is [`read_procs`]. Returns the pids it ended.
pub fn restart_stale(procs: &[Proc], user_data: &Path) -> Vec<u32> {
    stale_audio_services(procs, user_data)
        .into_iter()
        .filter(|pid| {
            tracing::info!(
                "control rooms: Chrome's audio service (pid {pid}) is older than pipewire-pulse, \
                 so its sound goes nowhere; ending it, Chrome starts a new one on the next sound (#420)"
            );
            match std::process::Command::new("kill").arg(pid.to_string()).status() {
                Ok(s) => {
                    if !s.success() {
                        tracing::warn!("control rooms: kill {pid} (audio service) exited {s}");
                    }
                    s.success()
                }
                Err(e) => {
                    tracing::warn!("control rooms: kill {pid} (audio service): {e}");
                    false
                }
            }
        })
        .collect()
}

#[cfg(test)]
#[path = "audio_service_tests.rs"]
mod tests;
