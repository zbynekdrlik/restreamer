//! Windows FFI half of the #368 ingest priorities.
//!
//! Only the raw kernel32 calls live here. Every decision (which class, the
//! EcoQoS state, the thread level, what to log) is in `ingest_priority.rs`,
//! where it is unit-tested on every platform. This file is compiled and run by
//! the windows-latest Test job and excluded from the ubuntu mutation job,
//! which cannot compile it.

use super::{OsCall, PowerThrottling, PriorityOs, SystemPriorityOs};
use windows_sys::Win32::System::Threading::{
    GetCurrentProcess, GetCurrentThread, GetPriorityClass, GetThreadPriority,
    PROCESS_POWER_THROTTLING_STATE, ProcessPowerThrottling, SetPriorityClass,
    SetProcessInformation, SetThreadPriority,
};

/// `GetThreadPriority`'s error return (`THREAD_PRIORITY_ERROR_RETURN`, which
/// windows-sys files under a feature this crate does not enable).
const THREAD_PRIORITY_ERROR_RETURN: i32 = 0x7FFF_FFFF;

fn os_error<T>(api: &str) -> OsCall<T> {
    OsCall::Failed(format!("{api}: {}", std::io::Error::last_os_error()))
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
