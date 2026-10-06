//! Process + system resource snapshot for the #367 process-stall detector.
//!
//! Sampled ON the detector's own OS thread — at detector start, periodically
//! as a pre-stall baseline, when a stall is detected and when it ends. It must
//! therefore never touch the tokio runtime or the `log` crate: plain syscalls
//! only, and every failure is carried IN the snapshot (`errors`) instead of
//! being logged.
//!
//! Windows (the production box, stream.lan) gets the exact numbers the
//! 2026-10-01 freeze investigation lacked — process working set, private
//! bytes, page faults, handle count, the process's own pool quota, and the
//! system-wide `GetPerformanceInfo` view (commit, physical available, kernel
//! paged/nonpaged pool, system handle count). camera-box traced a stream-OBS
//! leak of ~47 handles/s; the kernel-pool + system-handle figures are what
//! prove or refute that it starves this process. Other platforms fall back to
//! `sysinfo` (memory only) so Linux CI compiles and exercises the same path.

use serde_json::{Value, json};

/// One point-in-time resource reading. Every field is `Option` because each
/// platform exposes a different subset; `None` serializes as JSON `null`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResourceSnapshot {
    /// Process working set (Windows) / resident set (other platforms), bytes.
    pub working_set_bytes: Option<u64>,
    /// Process private (committed) bytes.
    pub private_bytes: Option<u64>,
    /// Cumulative process page-fault count.
    pub page_fault_count: Option<u64>,
    /// Open handles held by THIS process.
    pub handle_count: Option<u64>,
    /// Paged-pool quota charged to this process, bytes.
    pub process_paged_pool_bytes: Option<u64>,
    /// Nonpaged-pool quota charged to this process, bytes.
    pub process_nonpaged_pool_bytes: Option<u64>,
    /// System commit charge, bytes.
    pub commit_total_bytes: Option<u64>,
    /// System commit limit, bytes.
    pub commit_limit_bytes: Option<u64>,
    /// Physical memory installed, bytes.
    pub physical_total_bytes: Option<u64>,
    /// Physical memory available, bytes.
    pub physical_available_bytes: Option<u64>,
    /// Kernel paged pool (system-wide), bytes.
    pub kernel_paged_pool_bytes: Option<u64>,
    /// Kernel nonpaged pool (system-wide), bytes.
    pub kernel_nonpaged_pool_bytes: Option<u64>,
    /// Open handles across the whole system.
    pub system_handle_count: Option<u64>,
    /// Processes running on the system.
    pub system_process_count: Option<u64>,
    /// Threads running on the system.
    pub system_thread_count: Option<u64>,
    /// Which probe failed, e.g. `GetPerformanceInfo: Access is denied.`.
    pub errors: Vec<String>,
}

impl ResourceSnapshot {
    /// Stable JSON shape used in `stall.log` records and the `ProcessStall`
    /// audit detail.
    pub fn to_json(&self) -> Value {
        json!({
            "working_set_bytes": self.working_set_bytes,
            "private_bytes": self.private_bytes,
            "page_fault_count": self.page_fault_count,
            "handle_count": self.handle_count,
            "process_paged_pool_bytes": self.process_paged_pool_bytes,
            "process_nonpaged_pool_bytes": self.process_nonpaged_pool_bytes,
            "commit_total_bytes": self.commit_total_bytes,
            "commit_limit_bytes": self.commit_limit_bytes,
            "physical_total_bytes": self.physical_total_bytes,
            "physical_available_bytes": self.physical_available_bytes,
            "kernel_paged_pool_bytes": self.kernel_paged_pool_bytes,
            "kernel_nonpaged_pool_bytes": self.kernel_nonpaged_pool_bytes,
            "system_handle_count": self.system_handle_count,
            "system_process_count": self.system_process_count,
            "system_thread_count": self.system_thread_count,
            "errors": self.errors,
        })
    }
}

/// Raw `PROCESS_MEMORY_COUNTERS_EX` values, all bytes except the fault count.
/// A plain struct so the mapping into a snapshot is testable on every platform.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProcessCounters {
    pub working_set: usize,
    pub private_usage: usize,
    pub page_faults: u32,
    pub paged_pool_quota: usize,
    pub nonpaged_pool_quota: usize,
}

/// Raw `PERFORMANCE_INFORMATION` values. The memory figures are in PAGES of
/// `page_size` bytes; the counts are plain counts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PerformancePages {
    pub commit_total: usize,
    pub commit_limit: usize,
    pub physical_total: usize,
    pub physical_available: usize,
    pub kernel_paged: usize,
    pub kernel_nonpaged: usize,
    pub page_size: usize,
    pub handle_count: u32,
    pub process_count: u32,
    pub thread_count: u32,
}

impl ResourceSnapshot {
    pub fn set_process_counters(&mut self, c: ProcessCounters) {
        self.working_set_bytes = Some(c.working_set as u64);
        self.private_bytes = Some(c.private_usage as u64);
        self.page_fault_count = Some(u64::from(c.page_faults));
        self.process_paged_pool_bytes = Some(c.paged_pool_quota as u64);
        self.process_nonpaged_pool_bytes = Some(c.nonpaged_pool_quota as u64);
    }

    /// Scales the page-denominated memory figures to bytes.
    pub fn set_performance(&mut self, p: PerformancePages) {
        let page = p.page_size as u64;
        self.commit_total_bytes = Some(p.commit_total as u64 * page);
        self.commit_limit_bytes = Some(p.commit_limit as u64 * page);
        self.physical_total_bytes = Some(p.physical_total as u64 * page);
        self.physical_available_bytes = Some(p.physical_available as u64 * page);
        self.kernel_paged_pool_bytes = Some(p.kernel_paged as u64 * page);
        self.kernel_nonpaged_pool_bytes = Some(p.kernel_nonpaged as u64 * page);
        self.system_handle_count = Some(u64::from(p.handle_count));
        self.system_process_count = Some(u64::from(p.process_count));
        self.system_thread_count = Some(u64::from(p.thread_count));
    }
}

/// Take a snapshot of this process and the system. Never panics, never logs.
pub fn sample() -> ResourceSnapshot {
    platform::sample()
}

// The Windows FFI lives in its own file: it only compiles for Windows, so the
// ubuntu mutation job excludes it (ci.yml), and the windows-latest Test job
// runs it for real (`resource_sample_reads_real_process_and_system_memory`).
// All arithmetic stays in the cross-platform helpers above.
#[cfg(windows)]
#[path = "stall_resources_windows.rs"]
mod platform;

#[cfg(not(windows))]
mod platform {
    use super::ResourceSnapshot;
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};

    pub(super) fn sample() -> ResourceSnapshot {
        let mut s = ResourceSnapshot::default();
        let mut sys = System::new();

        sys.refresh_memory();
        s.physical_total_bytes = Some(sys.total_memory());
        s.physical_available_bytes = Some(sys.available_memory());

        match sysinfo::get_current_pid() {
            Ok(pid) => {
                sys.refresh_processes_specifics(
                    ProcessesToUpdate::Some(&[pid]),
                    false,
                    ProcessRefreshKind::nothing().with_memory(),
                );
                match sys.process(pid) {
                    Some(p) => s.working_set_bytes = Some(p.memory()),
                    None => s.errors.push(format!("sysinfo: process {pid} not found")),
                }
            }
            Err(e) => s.errors.push(format!("sysinfo get_current_pid: {e}")),
        }

        s
    }
}
