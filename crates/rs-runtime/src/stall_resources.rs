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

/// Take a snapshot of this process and the system. Never panics, never logs.
pub fn sample() -> ResourceSnapshot {
    platform::sample()
}

#[cfg(windows)]
mod platform {
    use super::ResourceSnapshot;
    use windows_sys::Win32::System::ProcessStatus::{
        GetPerformanceInfo, GetProcessMemoryInfo, PERFORMANCE_INFORMATION, PROCESS_MEMORY_COUNTERS,
        PROCESS_MEMORY_COUNTERS_EX,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessHandleCount};

    fn os_error(api: &str) -> String {
        format!("{api}: {}", std::io::Error::last_os_error())
    }

    pub(super) fn sample() -> ResourceSnapshot {
        let mut s = ResourceSnapshot::default();

        // SAFETY: `GetCurrentProcess` takes no arguments and returns a
        // pseudo-handle for this process that is always valid and never needs
        // closing.
        let process = unsafe { GetCurrentProcess() };

        let mut pmc = PROCESS_MEMORY_COUNTERS_EX {
            cb: size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32,
            ..Default::default()
        };
        // SAFETY: `pmc` is a properly aligned, writable PROCESS_MEMORY_COUNTERS_EX
        // whose `cb` states its real size; the API accepts the EX layout through
        // the base-struct pointer when `cb` says so (documented psapi contract).
        let ok = unsafe {
            GetProcessMemoryInfo(
                process,
                (&mut pmc as *mut PROCESS_MEMORY_COUNTERS_EX).cast::<PROCESS_MEMORY_COUNTERS>(),
                pmc.cb,
            )
        };
        if ok != 0 {
            s.working_set_bytes = Some(pmc.WorkingSetSize as u64);
            s.private_bytes = Some(pmc.PrivateUsage as u64);
            s.page_fault_count = Some(u64::from(pmc.PageFaultCount));
            s.process_paged_pool_bytes = Some(pmc.QuotaPagedPoolUsage as u64);
            s.process_nonpaged_pool_bytes = Some(pmc.QuotaNonPagedPoolUsage as u64);
        } else {
            s.errors.push(os_error("GetProcessMemoryInfo"));
        }

        let mut handles: u32 = 0;
        // SAFETY: `process` is the current-process pseudo-handle and `handles`
        // is a valid, writable u32 out-pointer.
        if unsafe { GetProcessHandleCount(process, &mut handles) } != 0 {
            s.handle_count = Some(u64::from(handles));
        } else {
            s.errors.push(os_error("GetProcessHandleCount"));
        }

        let mut perf = PERFORMANCE_INFORMATION {
            cb: size_of::<PERFORMANCE_INFORMATION>() as u32,
            ..Default::default()
        };
        // SAFETY: `perf` is a valid, writable PERFORMANCE_INFORMATION and `cb`
        // states its real size.
        if unsafe { GetPerformanceInfo(&mut perf, perf.cb) } != 0 {
            // Memory figures are reported in PAGES; scale by the page size.
            let page = perf.PageSize as u64;
            s.commit_total_bytes = Some(perf.CommitTotal as u64 * page);
            s.commit_limit_bytes = Some(perf.CommitLimit as u64 * page);
            s.physical_total_bytes = Some(perf.PhysicalTotal as u64 * page);
            s.physical_available_bytes = Some(perf.PhysicalAvailable as u64 * page);
            s.kernel_paged_pool_bytes = Some(perf.KernelPaged as u64 * page);
            s.kernel_nonpaged_pool_bytes = Some(perf.KernelNonpaged as u64 * page);
            s.system_handle_count = Some(u64::from(perf.HandleCount));
            s.system_process_count = Some(u64::from(perf.ProcessCount));
            s.system_thread_count = Some(u64::from(perf.ThreadCount));
        } else {
            s.errors.push(os_error("GetPerformanceInfo"));
        }

        s
    }
}

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
