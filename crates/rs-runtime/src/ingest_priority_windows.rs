//! Windows FFI half of the #368 ingest priorities.
//!
//! Only the raw kernel32 / ntdll calls live here. Every decision (which
//! class, which memory and I/O priority, the EcoQoS state, the thread level,
//! what to log) is in `ingest_priority.rs`, where it is unit-tested on every
//! platform. This file is compiled and run by the windows-latest Test job and
//! excluded from the ubuntu mutation job, which cannot compile it.

use std::ffi::c_void;

use super::{
    OsCall, PROCESS_IO_PRIORITY_CLASS, PowerThrottling, PriorityOs, ProcessLevel, SystemPriorityOs,
};
use windows_sys::Win32::Foundation::{HANDLE, NTSTATUS};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, GetCurrentThread, GetPriorityClass, GetProcessInformation,
    GetThreadPriority, MEMORY_PRIORITY_INFORMATION, PROCESS_POWER_THROTTLING_STATE,
    ProcessMemoryPriority, ProcessPowerThrottling, SetPriorityClass, SetProcessInformation,
    SetThreadPriority,
};

// The I/O priority has no documented Win32 call: only the native
// `ProcessIoPriority` information class reaches it. windows-sys 0.61 has
// `NtQueryInformationProcess` (behind a WDK feature) but no
// `NtSetInformationProcess`, so both are declared here. `raw-dylib` needs no
// import library.
#[link(name = "ntdll", kind = "raw-dylib")]
unsafe extern "system" {
    fn NtQueryInformationProcess(
        process: HANDLE,
        class: i32,
        information: *mut c_void,
        length: u32,
        return_length: *mut u32,
    ) -> NTSTATUS;
    fn NtSetInformationProcess(
        process: HANDLE,
        class: i32,
        information: *const c_void,
        length: u32,
    ) -> NTSTATUS;
}

/// `GetThreadPriority`'s error return (`THREAD_PRIORITY_ERROR_RETURN`, which
/// windows-sys files under a feature this crate does not enable).
const THREAD_PRIORITY_ERROR_RETURN: i32 = 0x7FFF_FFFF;

fn os_error<T>(api: &str) -> OsCall<T> {
    OsCall::Failed(format!("{api}: {}", std::io::Error::last_os_error()))
}

/// A native call's status: `NT_SUCCESS` is a non-negative `NTSTATUS`.
fn nt_status<T>(api: &str, status: NTSTATUS, done: T) -> OsCall<T> {
    if status >= 0 {
        OsCall::Done(done)
    } else {
        OsCall::Failed(format!("{api}: NTSTATUS {:#010x}", status as u32))
    }
}

fn memory_priority() -> OsCall<u32> {
    let mut info = MEMORY_PRIORITY_INFORMATION { MemoryPriority: 0 };
    // SAFETY: the current-process pseudo-handle, and `info` is a valid,
    // writable MEMORY_PRIORITY_INFORMATION whose real size is passed, as
    // ProcessMemoryPriority requires.
    let ok = unsafe {
        GetProcessInformation(
            GetCurrentProcess(),
            ProcessMemoryPriority,
            (&mut info as *mut MEMORY_PRIORITY_INFORMATION).cast(),
            size_of::<MEMORY_PRIORITY_INFORMATION>() as u32,
        )
    };
    if ok != 0 {
        OsCall::Done(info.MemoryPriority)
    } else {
        os_error("GetProcessInformation(ProcessMemoryPriority)")
    }
}

fn set_memory_priority(value: u32) -> OsCall<()> {
    let info = MEMORY_PRIORITY_INFORMATION {
        MemoryPriority: value,
    };
    // SAFETY: as in `memory_priority`; `info` outlives the call.
    let ok = unsafe {
        SetProcessInformation(
            GetCurrentProcess(),
            ProcessMemoryPriority,
            (&info as *const MEMORY_PRIORITY_INFORMATION).cast(),
            size_of::<MEMORY_PRIORITY_INFORMATION>() as u32,
        )
    };
    if ok != 0 {
        OsCall::Done(())
    } else {
        os_error("SetProcessInformation(ProcessMemoryPriority)")
    }
}

fn io_priority() -> OsCall<u32> {
    let mut hint: u32 = 0;
    let mut written: u32 = 0;
    // SAFETY: the current-process pseudo-handle; `hint` is a writable u32,
    // the size ProcessIoPriority uses (an IO_PRIORITY_HINT enum), and
    // `written` a writable u32.
    let status = unsafe {
        NtQueryInformationProcess(
            GetCurrentProcess(),
            PROCESS_IO_PRIORITY_CLASS,
            (&mut hint as *mut u32).cast(),
            size_of::<u32>() as u32,
            &mut written,
        )
    };
    nt_status("NtQueryInformationProcess(ProcessIoPriority)", status, hint)
}

fn set_io_priority(value: u32) -> OsCall<()> {
    // SAFETY: as in `io_priority`; `value` outlives the call.
    let status = unsafe {
        NtSetInformationProcess(
            GetCurrentProcess(),
            PROCESS_IO_PRIORITY_CLASS,
            (&value as *const u32).cast(),
            size_of::<u32>() as u32,
        )
    };
    nt_status("NtSetInformationProcess(ProcessIoPriority)", status, ())
}

impl PriorityOs for SystemPriorityOs {
    fn priority_class(&self) -> OsCall<u32> {
        // SAFETY: `GetCurrentProcess` returns a pseudo-handle for this process
        // that is always valid and never needs closing.
        let class = unsafe { GetPriorityClass(GetCurrentProcess()) };
        if class == 0 {
            os_error("GetPriorityClass")
        } else {
            OsCall::Done(class)
        }
    }

    fn set_priority_class(&self, class: u32) -> OsCall<()> {
        // SAFETY: the current-process pseudo-handle and a plain class value.
        if unsafe { SetPriorityClass(GetCurrentProcess(), class) } != 0 {
            OsCall::Done(())
        } else {
            os_error("SetPriorityClass")
        }
    }

    fn process_level(&self, which: ProcessLevel) -> OsCall<u32> {
        match which {
            ProcessLevel::Memory => memory_priority(),
            ProcessLevel::Io => io_priority(),
        }
    }

    fn set_process_level(&self, which: ProcessLevel, value: u32) -> OsCall<()> {
        match which {
            ProcessLevel::Memory => set_memory_priority(value),
            ProcessLevel::Io => set_io_priority(value),
        }
    }

    fn set_power_throttling(&self, state: PowerThrottling) -> OsCall<()> {
        let raw = PROCESS_POWER_THROTTLING_STATE {
            Version: state.version,
            ControlMask: state.control_mask,
            StateMask: state.state_mask,
        };
        // SAFETY: `raw` is a valid, properly aligned
        // PROCESS_POWER_THROTTLING_STATE that outlives the call, and the size
        // passed is its real size, as ProcessPowerThrottling requires.
        let ok = unsafe {
            SetProcessInformation(
                GetCurrentProcess(),
                ProcessPowerThrottling,
                (&raw as *const PROCESS_POWER_THROTTLING_STATE).cast(),
                size_of::<PROCESS_POWER_THROTTLING_STATE>() as u32,
            )
        };
        if ok != 0 {
            OsCall::Done(())
        } else {
            os_error("SetProcessInformation(ProcessPowerThrottling)")
        }
    }

    fn set_current_thread_priority(&self, priority: i32) -> OsCall<()> {
        // SAFETY: `GetCurrentThread` returns a pseudo-handle for the calling
        // thread that is always valid and never needs closing.
        if unsafe { SetThreadPriority(GetCurrentThread(), priority) } != 0 {
            OsCall::Done(())
        } else {
            os_error("SetThreadPriority")
        }
    }

    fn current_thread_priority(&self) -> OsCall<i32> {
        // SAFETY: the calling thread's pseudo-handle.
        let priority = unsafe { GetThreadPriority(GetCurrentThread()) };
        if priority == THREAD_PRIORITY_ERROR_RETURN {
            os_error("GetThreadPriority")
        } else {
            OsCall::Done(priority)
        }
    }
}
