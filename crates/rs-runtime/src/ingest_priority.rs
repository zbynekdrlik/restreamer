//! Windows scheduling priorities for the RTMP ingest (#368).
//!
//! Restreamer ran at BelowNormal: the `RestreamerGUI` scheduled task used
//! Task Scheduler's default priority 7. On stream.lan every Normal-or-higher
//! thread (RealTime dantesync/ptp, High ProcessGovernor, AboveNormal
//! webview/browser pages) then preempted the ingest, and OBS dropped frames.
//!
//! - The process runs at Normal. `install.ps1` and the CI deploy register the
//!   task with `-Priority 4`; [`apply_process_priority`] raises a process
//!   that still starts below Normal (a task registered before #368).
//! - The process is opted out of EcoQoS power throttling.
//! - The one ingest thread runs at `THREAD_PRIORITY_HIGHEST`. Its work is tiny
//!   (socket read, parse, hand-off), so it cannot starve stream OBS.
//! - The process is NEVER raised above Normal: stream OBS runs BelowNormal
//!   (camera-box's domain), and restreamer must not compete with its encoder.
//!
//! The OS calls sit behind [`PriorityOs`]; `ingest_priority_windows.rs` holds
//! only the Windows FFI. Every decision is here, unit-tested on all platforms.

/// `GetPriorityClass` / `SetPriorityClass` values (the same numbers as
/// windows-sys, kept here so the decisions compile and are tested everywhere).
pub const IDLE_PRIORITY_CLASS: u32 = 0x0000_0040;
pub const BELOW_NORMAL_PRIORITY_CLASS: u32 = 0x0000_4000;
pub const NORMAL_PRIORITY_CLASS: u32 = 0x0000_0020;
pub const ABOVE_NORMAL_PRIORITY_CLASS: u32 = 0x0000_8000;
pub const HIGH_PRIORITY_CLASS: u32 = 0x0000_0080;
pub const REALTIME_PRIORITY_CLASS: u32 = 0x0000_0100;
/// `SetThreadPriority` level of the ingest thread.
pub const THREAD_PRIORITY_HIGHEST: i32 = 2;
/// `PROCESS_POWER_THROTTLING_STATE` constants.
pub const PROCESS_POWER_THROTTLING_CURRENT_VERSION: u32 = 1;
pub const PROCESS_POWER_THROTTLING_EXECUTION_SPEED: u32 = 1;
/// Memory priorities (`MEMORY_PRIORITY_INFORMATION.MemoryPriority`). A
/// process the Task Scheduler starts at task priority 7 gets LOW.
pub const MEMORY_PRIORITY_VERY_LOW: u32 = 1;
pub const MEMORY_PRIORITY_LOW: u32 = 2;
pub const MEMORY_PRIORITY_MEDIUM: u32 = 3;
pub const MEMORY_PRIORITY_BELOW_NORMAL: u32 = 4;
pub const MEMORY_PRIORITY_NORMAL: u32 = 5;
/// I/O priority hints (`IO_PRIORITY_HINT`). Task priority 7 gives LOW.
pub const IO_PRIORITY_VERY_LOW: u32 = 0;
pub const IO_PRIORITY_LOW: u32 = 1;
pub const IO_PRIORITY_NORMAL: u32 = 2;
pub const IO_PRIORITY_HIGH: u32 = 3;
pub const IO_PRIORITY_CRITICAL: u32 = 4;
/// `PROCESSINFOCLASS::ProcessIoPriority`, for `NtQueryInformationProcess` /
/// `NtSetInformationProcess` (windows-sys has no `NtSetInformationProcess`).
pub const PROCESS_IO_PRIORITY_CLASS: i32 = 33;

/// The two process priorities a level number describes, next to the CPU
/// class. Task Scheduler priority 7 lowers both (#368
/// issuecomment-6012351712), and a box upgraded without re-registering the
/// task keeps them low.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessLevel {
    /// The memory priority: how early this process's pages are trimmed
    /// under memory pressure.
    Memory,
    /// The I/O priority: the chunk writes and the database.
    Io,
}

/// The priority classes in scheduling order, lowest first (the raw values
/// are not ordered).
const CLASS_ORDER: [(u32, &str); 6] = [
    (IDLE_PRIORITY_CLASS, "idle"),
    (BELOW_NORMAL_PRIORITY_CLASS, "below_normal"),
    (NORMAL_PRIORITY_CLASS, "normal"),
    (ABOVE_NORMAL_PRIORITY_CLASS, "above_normal"),
    (HIGH_PRIORITY_CLASS, "high"),
    (REALTIME_PRIORITY_CLASS, "realtime"),
];

/// Position of `class` in scheduling order; `None` for an unknown value.
pub fn class_rank(class: u32) -> Option<usize> {
    CLASS_ORDER.iter().position(|(c, _)| *c == class)
}

/// Readable name of a priority class.
pub fn class_name(class: u32) -> &'static str {
    CLASS_ORDER
        .iter()
        .find(|(c, _)| *c == class)
        .map_or("unknown", |(_, name)| name)
}

/// What to do with the process priority class read at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassAction {
    /// Below Normal (Idle, BelowNormal): raise to Normal.
    RaiseToNormal,
    /// Normal or above, or an unknown value: leave it. Restreamer never
    /// raises itself above Normal, and never lowers what an operator set.
    Keep,
}

pub fn class_action(current: u32) -> ClassAction {
    match (class_rank(current), class_rank(NORMAL_PRIORITY_CLASS)) {
        (Some(rank), Some(normal)) if rank < normal => ClassAction::RaiseToNormal,
        _ => ClassAction::Keep,
    }
}

/// A `PROCESS_POWER_THROTTLING_STATE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PowerThrottling {
    pub version: u32,
    pub control_mask: u32,
    pub state_mask: u32,
}

/// EcoQoS off: take control of the execution-speed policy (its control bit
/// set) and switch throttling OFF (its state bit clear).
pub fn ecoqos_off() -> PowerThrottling {
    PowerThrottling {
        version: PROCESS_POWER_THROTTLING_CURRENT_VERSION,
        control_mask: PROCESS_POWER_THROTTLING_EXECUTION_SPEED,
        state_mask: 0,
    }
}

/// Outcome of one OS call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OsCall<T> {
    Done(T),
    /// The platform has no such call (everything but Windows).
    Unsupported,
    Failed(String),
}

impl<T> OsCall<T> {
    fn failed(&self) -> bool {
        matches!(self, Self::Failed(_))
    }

    /// The outcome for a log line; `done` renders a success.
    fn text(&self, done: impl FnOnce(&T) -> String) -> String {
        match self {
            Self::Done(v) => done(v),
            Self::Unsupported => "unsupported on this platform".to_string(),
            Self::Failed(e) => format!("FAILED ({e})"),
        }
    }
}

fn ok<T>(_: &T) -> String {
    "ok".to_string()
}

/// The OS priority calls. [`SystemPriorityOs`] is the real one; tests use a
/// fake that records the calls.
pub trait PriorityOs {
    fn priority_class(&self) -> OsCall<u32>;
    fn set_priority_class(&self, class: u32) -> OsCall<()>;
    /// The process's memory or I/O priority.
    fn process_level(&self, which: ProcessLevel) -> OsCall<u32>;
    fn set_process_level(&self, which: ProcessLevel, value: u32) -> OsCall<()>;
    fn set_power_throttling(&self, state: PowerThrottling) -> OsCall<()>;
    fn set_current_thread_priority(&self, priority: i32) -> OsCall<()>;
    fn current_thread_priority(&self) -> OsCall<i32>;
}

/// The real OS: the Windows calls in `ingest_priority_windows.rs`; every
/// other platform has none.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemPriorityOs;

#[cfg(windows)]
#[path = "ingest_priority_windows.rs"]
mod os_windows;

#[cfg(not(windows))]
impl PriorityOs for SystemPriorityOs {
    fn priority_class(&self) -> OsCall<u32> {
        OsCall::Unsupported
    }
    fn set_priority_class(&self, _class: u32) -> OsCall<()> {
        OsCall::Unsupported
    }
    fn process_level(&self, _which: ProcessLevel) -> OsCall<u32> {
        OsCall::Unsupported
    }
    fn set_process_level(&self, _which: ProcessLevel, _value: u32) -> OsCall<()> {
        OsCall::Unsupported
    }
    fn set_power_throttling(&self, _state: PowerThrottling) -> OsCall<()> {
        OsCall::Unsupported
    }
    fn set_current_thread_priority(&self, _priority: i32) -> OsCall<()> {
        OsCall::Unsupported
    }
    fn current_thread_priority(&self) -> OsCall<i32> {
        OsCall::Unsupported
    }
}

/// What [`apply_process_priority`] found and did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessPriorityReport {
    pub class_before: OsCall<u32>,
    pub action: ClassAction,
    /// The `SetPriorityClass(NORMAL)` outcome; `None` when the class was kept.
    pub raise: Option<OsCall<()>>,
    pub ecoqos_off: OsCall<()>,
}

/// Process-wide, once at startup: raise a below-Normal process to Normal
/// and switch EcoQoS throttling off.
pub fn apply_process_priority(os: &impl PriorityOs) -> ProcessPriorityReport {
    let class_before = os.priority_class();
    let action = match &class_before {
        OsCall::Done(class) => class_action(*class),
        OsCall::Unsupported | OsCall::Failed(_) => ClassAction::Keep,
    };
    let raise = (action == ClassAction::RaiseToNormal)
        .then(|| os.set_priority_class(NORMAL_PRIORITY_CLASS));
    let ecoqos_off = os.set_power_throttling(ecoqos_off());
    ProcessPriorityReport {
        class_before,
        action,
        raise,
        ecoqos_off,
    }
}

impl ProcessPriorityReport {
    /// One log line.
    pub fn summary(&self) -> String {
        let raise = match &self.raise {
            Some(raise) => format!("raise to normal: {}", raise.text(ok)),
            None => "class kept".to_string(),
        };
        format!(
            "process priority class {}; {raise}; EcoQoS throttling off: {}",
            self.class_before
                .text(|class| class_name(*class).to_string()),
            self.ecoqos_off.text(ok),
        )
    }

    /// `Warn` when a Windows call failed, `Info` otherwise.
    pub fn level(&self) -> log::Level {
        let failed = self.class_before.failed()
            || self.raise.as_ref().is_some_and(OsCall::failed)
            || self.ecoqos_off.failed();
        if failed {
            log::Level::Warn
        } else {
            log::Level::Info
        }
    }
}

/// What [`raise_ingest_thread`] did to the calling thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadPriorityReport {
    pub set: OsCall<()>,
    /// The priority read back after the set.
    pub now: OsCall<i32>,
}

/// Run on the ingest thread itself: raise it to `THREAD_PRIORITY_HIGHEST`
/// and read the result back.
pub fn raise_ingest_thread(os: &impl PriorityOs) -> ThreadPriorityReport {
    ThreadPriorityReport {
        set: os.set_current_thread_priority(THREAD_PRIORITY_HIGHEST),
        now: os.current_thread_priority(),
    }
}

impl ThreadPriorityReport {
    /// One log line.
    pub fn summary(&self) -> String {
        format!(
            "thread priority highest: {}; thread priority now {}",
            self.set.text(ok),
            self.now.text(|p| p.to_string()),
        )
    }

    /// `Warn` when the raise failed or did not stick, `Info` otherwise.
    pub fn level(&self) -> log::Level {
        let not_highest = matches!(self.now, OsCall::Done(p) if p != THREAD_PRIORITY_HIGHEST);
        if self.set.failed() || self.now.failed() || not_highest {
            log::Level::Warn
        } else {
            log::Level::Info
        }
    }
}

#[cfg(test)]
#[path = "ingest_priority_tests.rs"]
mod tests;
