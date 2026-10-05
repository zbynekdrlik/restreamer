//! Windows FFI half of the #367 stall-detector resource snapshot.
//!
//! Only the three psapi/kernel32 calls live here, plus the copy of their raw
//! fields into the cross-platform `ProcessCounters` / `PerformancePages`
//! structs. All arithmetic (pages × page size) is in `stall_resources.rs`,
//! where it is unit-tested on every platform. This file is compiled and run by
//! the windows-latest Test job and excluded from the ubuntu mutation job, which
//! cannot compile it.

use super::{PerformancePages, ProcessCounters, ResourceSnapshot};
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
        s.set_process_counters(ProcessCounters {
            working_set: pmc.WorkingSetSize,
            private_usage: pmc.PrivateUsage,
            page_faults: pmc.PageFaultCount,
            paged_pool_quota: pmc.QuotaPagedPoolUsage,
            nonpaged_pool_quota: pmc.QuotaNonPagedPoolUsage,
        });
    } else {
        s.errors.push(os_error("GetProcessMemoryInfo"));
    }

    let mut handles: u32 = 0;
    // SAFETY: `process` is the current-process pseudo-handle and `handles` is
    // a valid, writable u32 out-pointer.
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
        s.set_performance(PerformancePages {
            commit_total: perf.CommitTotal,
            commit_limit: perf.CommitLimit,
            physical_total: perf.PhysicalTotal,
            physical_available: perf.PhysicalAvailable,
            kernel_paged: perf.KernelPaged,
            kernel_nonpaged: perf.KernelNonpaged,
            page_size: perf.PageSize,
            handle_count: perf.HandleCount,
            process_count: perf.ProcessCount,
            thread_count: perf.ThreadCount,
        });
    } else {
        s.errors.push(os_error("GetPerformanceInfo"));
    }

    s
}
