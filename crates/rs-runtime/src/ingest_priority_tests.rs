//! #368 design test (iii): the priority decisions, against a fake OS.

use super::*;

use std::sync::Mutex;

/// A fake Windows: answers from its fields and records every call. The
/// memory and I/O priorities are state: a successful set changes what the
/// next read returns, as on Windows.
struct FakeOs {
    class: OsCall<u32>,
    set_class: OsCall<()>,
    memory: Mutex<OsCall<u32>>,
    set_memory: OsCall<()>,
    io: Mutex<OsCall<u32>>,
    set_io: OsCall<()>,
    throttling: OsCall<()>,
    set_thread: OsCall<()>,
    thread_now: OsCall<i32>,
    calls: Mutex<Vec<String>>,
}

impl FakeOs {
    /// A process at `class`, with Normal memory and I/O priority.
    fn at(class: u32) -> Self {
        Self {
            class: OsCall::Done(class),
            set_class: OsCall::Done(()),
            memory: Mutex::new(OsCall::Done(MEMORY_PRIORITY_NORMAL)),
            set_memory: OsCall::Done(()),
            io: Mutex::new(OsCall::Done(IO_PRIORITY_NORMAL)),
            set_io: OsCall::Done(()),
            throttling: OsCall::Done(()),
            set_thread: OsCall::Done(()),
            thread_now: OsCall::Done(THREAD_PRIORITY_HIGHEST),
            calls: Mutex::new(Vec::new()),
        }
    }

    /// What a `RestreamerGUI` task still at Task Scheduler priority 7
    /// starts the app with (stream.lan, #368 issuecomment-6012351712):
    /// BelowNormal class, memory priority LOW, I/O priority Low.
    fn task_priority_7() -> Self {
        Self {
            memory: Mutex::new(OsCall::Done(MEMORY_PRIORITY_LOW)),
            io: Mutex::new(OsCall::Done(IO_PRIORITY_LOW)),
            ..Self::at(BELOW_NORMAL_PRIORITY_CLASS)
        }
    }

    /// A process at Normal class with these memory and I/O priorities.
    fn levels(memory: u32, io: u32) -> Self {
        Self {
            memory: Mutex::new(OsCall::Done(memory)),
            io: Mutex::new(OsCall::Done(io)),
            ..Self::at(NORMAL_PRIORITY_CLASS)
        }
    }

    /// The cell and the set outcome of one process level, and its name in
    /// the call log.
    fn level(&self, which: ProcessLevel) -> (&Mutex<OsCall<u32>>, &OsCall<()>, &'static str) {
        match which {
            ProcessLevel::Memory => (&self.memory, &self.set_memory, "memory"),
            ProcessLevel::Io => (&self.io, &self.set_io, "io"),
        }
    }

    fn record(&self, call: String) {
        self.calls.lock().unwrap().push(call);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl PriorityOs for FakeOs {
    fn priority_class(&self) -> OsCall<u32> {
        self.record("get_class".into());
        self.class.clone()
    }
    fn set_priority_class(&self, class: u32) -> OsCall<()> {
        self.record(format!("set_class {class:#x}"));
        self.set_class.clone()
    }
    fn process_level(&self, which: ProcessLevel) -> OsCall<u32> {
        let (cell, _, name) = self.level(which);
        self.record(format!("get_{name}"));
        cell.lock().unwrap().clone()
    }
    fn set_process_level(&self, which: ProcessLevel, value: u32) -> OsCall<()> {
        let (cell, outcome, name) = self.level(which);
        self.record(format!("set_{name} {value}"));
        if *outcome == OsCall::Done(()) {
            *cell.lock().unwrap() = OsCall::Done(value);
        }
        outcome.clone()
    }
    fn set_power_throttling(&self, state: PowerThrottling) -> OsCall<()> {
        self.record(format!(
            "throttling v{} control {:#x} state {:#x}",
            state.version, state.control_mask, state.state_mask
        ));
        self.throttling.clone()
    }
    fn set_current_thread_priority(&self, priority: i32) -> OsCall<()> {
        self.record(format!("set_thread {priority}"));
        self.set_thread.clone()
    }
    fn current_thread_priority(&self) -> OsCall<i32> {
        self.record("get_thread".into());
        self.thread_now.clone()
    }
}

#[test]
fn classes_rank_in_scheduling_order() {
    let ranks: Vec<_> = [
        IDLE_PRIORITY_CLASS,
        BELOW_NORMAL_PRIORITY_CLASS,
        NORMAL_PRIORITY_CLASS,
        ABOVE_NORMAL_PRIORITY_CLASS,
        HIGH_PRIORITY_CLASS,
        REALTIME_PRIORITY_CLASS,
    ]
    .into_iter()
    .map(class_rank)
    .collect();
    assert_eq!(ranks, (0..6).map(Some).collect::<Vec<_>>());
    assert_eq!(class_rank(0x1234), None);
    assert_eq!(class_name(BELOW_NORMAL_PRIORITY_CLASS), "below_normal");
    assert_eq!(class_name(NORMAL_PRIORITY_CLASS), "normal");
    assert_eq!(class_name(REALTIME_PRIORITY_CLASS), "realtime");
    assert_eq!(class_name(0x1234), "unknown");
}

/// The constants are the Windows values (`windows-sys` 0.61).
#[test]
fn constants_are_the_windows_values() {
    assert_eq!(IDLE_PRIORITY_CLASS, 64);
    assert_eq!(BELOW_NORMAL_PRIORITY_CLASS, 16_384);
    assert_eq!(NORMAL_PRIORITY_CLASS, 32);
    assert_eq!(ABOVE_NORMAL_PRIORITY_CLASS, 32_768);
    assert_eq!(HIGH_PRIORITY_CLASS, 128);
    assert_eq!(REALTIME_PRIORITY_CLASS, 256);
    assert_eq!(THREAD_PRIORITY_HIGHEST, 2);
}

/// The memory and I/O priority values (`MEMORY_PRIORITY_*`,
/// `IO_PRIORITY_HINT`, `ProcessIoPriority`), lowest first.
#[test]
fn level_constants_are_the_windows_values() {
    assert_eq!(
        [
            MEMORY_PRIORITY_LOWEST,
            MEMORY_PRIORITY_VERY_LOW,
            MEMORY_PRIORITY_LOW,
            MEMORY_PRIORITY_MEDIUM,
            MEMORY_PRIORITY_BELOW_NORMAL,
            MEMORY_PRIORITY_NORMAL,
        ],
        [0, 1, 2, 3, 4, 5]
    );
    assert_eq!(
        [
            IO_PRIORITY_VERY_LOW,
            IO_PRIORITY_LOW,
            IO_PRIORITY_NORMAL,
            IO_PRIORITY_HIGH,
            IO_PRIORITY_CRITICAL,
        ],
        [0, 1, 2, 3, 4]
    );
    assert_eq!(PROCESS_IO_PRIORITY_CLASS, 33);
}

/// The memory priorities are the windows-sys ones.
#[cfg(windows)]
#[test]
fn memory_constants_match_windows_sys() {
    use windows_sys::Win32::System::Threading as w;
    assert_eq!(MEMORY_PRIORITY_VERY_LOW, w::MEMORY_PRIORITY_VERY_LOW);
    assert_eq!(MEMORY_PRIORITY_LOW, w::MEMORY_PRIORITY_LOW);
    assert_eq!(MEMORY_PRIORITY_MEDIUM, w::MEMORY_PRIORITY_MEDIUM);
    assert_eq!(
        MEMORY_PRIORITY_BELOW_NORMAL,
        w::MEMORY_PRIORITY_BELOW_NORMAL
    );
    assert_eq!(MEMORY_PRIORITY_NORMAL, w::MEMORY_PRIORITY_NORMAL);
}

/// Below Normal is raised to Normal; Normal and above are never touched, so
/// restreamer never competes above Normal with stream OBS.
#[test]
fn only_a_class_below_normal_is_raised() {
    assert_eq!(
        class_action(IDLE_PRIORITY_CLASS),
        ClassAction::RaiseToNormal
    );
    assert_eq!(
        class_action(BELOW_NORMAL_PRIORITY_CLASS),
        ClassAction::RaiseToNormal
    );
    for kept in [
        NORMAL_PRIORITY_CLASS,
        ABOVE_NORMAL_PRIORITY_CLASS,
        HIGH_PRIORITY_CLASS,
        REALTIME_PRIORITY_CLASS,
        0x1234,
    ] {
        assert_eq!(class_action(kept), ClassAction::Keep, "{kept:#x}");
    }
}

#[test]
fn ecoqos_off_controls_execution_speed_and_clears_it() {
    assert_eq!(
        ecoqos_off(),
        PowerThrottling {
            version: 1,
            control_mask: 1,
            state_mask: 0,
        }
    );
}

/// The Sunday box: the task started the app BelowNormal.
#[test]
fn a_below_normal_process_is_raised_to_normal_and_ecoqos_is_switched_off() {
    let os = FakeOs::at(BELOW_NORMAL_PRIORITY_CLASS);
    let report = apply_process_priority(&os);
    assert_eq!(
        os.calls(),
        [
            "get_class",
            "set_class 0x20",
            "get_memory",
            "get_io",
            "throttling v1 control 0x1 state 0x0"
        ]
    );
    assert_eq!(report.action, ClassAction::RaiseToNormal);
    assert_eq!(report.raise, Some(OsCall::Done(())));
    assert_eq!(
        report.summary(),
        "process priority class below_normal; raise to normal: ok; \
         memory priority normal (5), kept; I/O priority normal (2), kept; \
         EcoQoS throttling off: ok"
    );
    assert_eq!(report.level(), log::Level::Info);
}

#[test]
fn a_normal_or_higher_process_keeps_its_class() {
    for class in [NORMAL_PRIORITY_CLASS, HIGH_PRIORITY_CLASS] {
        let os = FakeOs::at(class);
        let report = apply_process_priority(&os);
        assert_eq!(
            os.calls(),
            [
                "get_class",
                "get_memory",
                "get_io",
                "throttling v1 control 0x1 state 0x0"
            ],
            "{class:#x} must never be changed"
        );
        assert_eq!(report.raise, None);
        assert_eq!(report.level(), log::Level::Info);
    }
    let report = apply_process_priority(&FakeOs::at(NORMAL_PRIORITY_CLASS));
    assert_eq!(
        report.summary(),
        "process priority class normal; class kept; memory priority normal (5), kept; \
         I/O priority normal (2), kept; EcoQoS throttling off: ok"
    );
}

/// An unreadable class is left alone; EcoQoS is still switched off. Any
/// failed call makes the line a warning.
#[test]
fn failed_calls_keep_the_class_and_warn() {
    let os = FakeOs {
        class: OsCall::Failed("denied".into()),
        ..FakeOs::at(0)
    };
    let report = apply_process_priority(&os);
    assert_eq!(
        os.calls(),
        [
            "get_class",
            "get_memory",
            "get_io",
            "throttling v1 control 0x1 state 0x0"
        ]
    );
    assert_eq!(report.action, ClassAction::Keep);
    assert_eq!(
        report.summary(),
        "process priority class FAILED (denied); class kept; \
         memory priority normal (5), kept; I/O priority normal (2), kept; \
         EcoQoS throttling off: ok"
    );
    assert_eq!(report.level(), log::Level::Warn);

    let raise_failed = apply_process_priority(&FakeOs {
        set_class: OsCall::Failed("no".into()),
        ..FakeOs::at(IDLE_PRIORITY_CLASS)
    });
    assert_eq!(raise_failed.level(), log::Level::Warn);
    assert_eq!(
        raise_failed.summary(),
        "process priority class idle; raise to normal: FAILED (no); \
         memory priority normal (5), kept; I/O priority normal (2), kept; \
         EcoQoS throttling off: ok"
    );

    let ecoqos_failed = apply_process_priority(&FakeOs {
        throttling: OsCall::Failed("old windows".into()),
        ..FakeOs::at(NORMAL_PRIORITY_CLASS)
    });
    assert_eq!(ecoqos_failed.level(), log::Level::Warn);
}

/// Off Windows nothing is supported, and that is no warning.
#[test]
fn an_unsupported_platform_is_reported_at_info() {
    let os = FakeOs {
        class: OsCall::Unsupported,
        memory: Mutex::new(OsCall::Unsupported),
        io: Mutex::new(OsCall::Unsupported),
        throttling: OsCall::Unsupported,
        ..FakeOs::at(0)
    };
    let report = apply_process_priority(&os);
    assert_eq!(report.action, ClassAction::Keep);
    assert_eq!(
        os.calls(),
        [
            "get_class",
            "get_memory",
            "get_io",
            "throttling v1 control 0x1 state 0x0"
        ]
    );
    assert_eq!(
        report.summary(),
        "process priority class unsupported on this platform; class kept; \
         memory priority unsupported on this platform, kept; \
         I/O priority unsupported on this platform, kept; \
         EcoQoS throttling off: unsupported on this platform"
    );
    assert_eq!(report.level(), log::Level::Info);
}

/// ROZHODNUTÉ issuecomment-6013075985 item 1. A box whose task still has
/// Task Scheduler priority 7 (streampp, an in-app-updated install) starts
/// the app with memory priority LOW and I/O priority Low as well as a
/// BelowNormal class. Startup restores all three to Normal, reads each level
/// back and logs before and after.
#[test]
fn a_task_priority_7_process_gets_normal_memory_and_io_priority() {
    let os = FakeOs::task_priority_7();
    let report = apply_process_priority(&os);
    assert_eq!(
        os.calls(),
        [
            "get_class",
            "set_class 0x20",
            "get_memory",
            "set_memory 5",
            "get_memory",
            "get_io",
            "set_io 2",
            "get_io",
            "throttling v1 control 0x1 state 0x0"
        ]
    );
    assert_eq!(
        report.summary(),
        "process priority class below_normal; raise to normal: ok; \
         memory priority low (2), raise to normal: ok, now normal (5); \
         I/O priority low (1), raise to normal: ok, now normal (2); \
         EcoQoS throttling off: ok"
    );
    assert_eq!(report.level(), log::Level::Info);
}

/// Each level is raised only from a KNOWN value below Normal, and only to
/// Normal: never lowered (I/O High or Critical stays), never above Normal,
/// and an unknown value is left alone.
#[test]
fn memory_and_io_are_raised_only_from_a_known_level_below_normal() {
    let raised = |memory: u32, io: u32| {
        let os = FakeOs::levels(memory, io);
        apply_process_priority(&os);
        let calls = os.calls();
        (
            calls.iter().any(|c| c == "set_memory 5"),
            calls.iter().any(|c| c == "set_io 2"),
            calls.iter().filter(|c| c.starts_with("set_")).count(),
        )
    };
    for memory in [
        MEMORY_PRIORITY_LOWEST,
        MEMORY_PRIORITY_VERY_LOW,
        MEMORY_PRIORITY_LOW,
        MEMORY_PRIORITY_MEDIUM,
        MEMORY_PRIORITY_BELOW_NORMAL,
    ] {
        assert_eq!(
            raised(memory, IO_PRIORITY_NORMAL),
            (true, false, 1),
            "memory {memory}"
        );
    }
    for memory in [MEMORY_PRIORITY_NORMAL, 6, 99] {
        assert_eq!(
            raised(memory, IO_PRIORITY_NORMAL),
            (false, false, 0),
            "memory {memory}"
        );
    }
    for io in [IO_PRIORITY_VERY_LOW, IO_PRIORITY_LOW] {
        assert_eq!(
            raised(MEMORY_PRIORITY_NORMAL, io),
            (false, true, 1),
            "io {io}"
        );
    }
    for io in [
        IO_PRIORITY_NORMAL,
        IO_PRIORITY_HIGH,
        IO_PRIORITY_CRITICAL,
        5,
        99,
    ] {
        assert_eq!(
            raised(MEMORY_PRIORITY_NORMAL, io),
            (false, false, 0),
            "io {io}"
        );
    }
}

/// Kept levels name their value; a level that cannot be read is left alone
/// and makes the line a warning.
#[test]
fn kept_and_unreadable_levels_are_named_in_the_log_line() {
    let report = apply_process_priority(&FakeOs::levels(6, IO_PRIORITY_HIGH));
    assert_eq!(
        report.summary(),
        "process priority class normal; class kept; memory priority unknown (6), kept; \
         I/O priority high (3), kept; EcoQoS throttling off: ok"
    );
    assert_eq!(report.level(), log::Level::Info);

    let os = FakeOs {
        io: Mutex::new(OsCall::Failed("gone".into())),
        ..FakeOs::task_priority_7()
    };
    let report = apply_process_priority(&os);
    assert!(
        report
            .summary()
            .contains("; I/O priority FAILED (gone), kept; "),
        "{}",
        report.summary()
    );
    assert!(!os.calls().iter().any(|c| c.starts_with("set_io")));
    assert_eq!(report.level(), log::Level::Warn);
}

/// A raise Windows refuses is logged with the value read back after it, and
/// makes the line a warning.
#[test]
fn a_refused_level_raise_warns() {
    let os = FakeOs {
        set_memory: OsCall::Failed("denied".into()),
        ..FakeOs::task_priority_7()
    };
    let report = apply_process_priority(&os);
    assert!(
        report
            .summary()
            .contains("; memory priority low (2), raise to normal: FAILED (denied), now low (2); "),
        "{}",
        report.summary()
    );
    assert_eq!(report.level(), log::Level::Warn);

    let io_refused = apply_process_priority(&FakeOs {
        set_io: OsCall::Failed("denied".into()),
        ..FakeOs::levels(MEMORY_PRIORITY_NORMAL, IO_PRIORITY_VERY_LOW)
    });
    assert!(
        io_refused.summary().contains(
            "; I/O priority very_low (0), raise to normal: FAILED (denied), now very_low (0); "
        ),
        "{}",
        io_refused.summary()
    );
    assert_eq!(io_refused.level(), log::Level::Warn);
}

#[test]
fn process_levels_name_their_values_and_their_normal() {
    assert_eq!(ProcessLevel::Memory.label(), "memory priority");
    assert_eq!(ProcessLevel::Io.label(), "I/O priority");
    assert_eq!(ProcessLevel::Memory.normal(), MEMORY_PRIORITY_NORMAL);
    assert_eq!(ProcessLevel::Io.normal(), IO_PRIORITY_NORMAL);
    let memory: Vec<_> = (0..=6)
        .map(|v| ProcessLevel::Memory.value_name(v))
        .collect();
    assert_eq!(
        memory,
        [
            "lowest",
            "very_low",
            "low",
            "medium",
            "below_normal",
            "normal",
            "unknown"
        ]
    );
    let io: Vec<_> = (0..=5).map(|v| ProcessLevel::Io.value_name(v)).collect();
    assert_eq!(
        io,
        ["very_low", "low", "normal", "high", "critical", "unknown"]
    );
}

/// The raise decision at its boundaries, for both levels.
#[test]
fn a_level_is_raised_only_from_a_known_value_below_normal() {
    use ClassAction::{Keep, RaiseToNormal};
    let memory: Vec<_> = (0..=6).map(|v| ProcessLevel::Memory.action(v)).collect();
    assert_eq!(
        memory,
        [
            RaiseToNormal,
            RaiseToNormal,
            RaiseToNormal,
            RaiseToNormal,
            RaiseToNormal,
            Keep,
            Keep
        ]
    );
    let io: Vec<_> = (0..=5).map(|v| ProcessLevel::Io.action(v)).collect();
    assert_eq!(io, [RaiseToNormal, RaiseToNormal, Keep, Keep, Keep, Keep]);
}

/// A raise must reach Normal: a refused set, or a set that did not stick
/// (read back below Normal), makes the line a warning, even when the other
/// half looks fine.
#[test]
fn a_level_raise_that_fails_or_does_not_stick_is_a_failure() {
    let raised = |set: OsCall<()>, now: OsCall<u32>| LevelReport {
        level: ProcessLevel::Memory,
        before: OsCall::Done(MEMORY_PRIORITY_LOW),
        action: ClassAction::RaiseToNormal,
        raise: Some((set, now)),
    };
    assert!(!raised(OsCall::Done(()), OsCall::Done(MEMORY_PRIORITY_NORMAL)).failed());
    assert!(raised(OsCall::Done(()), OsCall::Done(MEMORY_PRIORITY_LOW)).failed());
    assert!(raised(OsCall::Done(()), OsCall::Failed("gone".into())).failed());
    assert!(
        raised(
            OsCall::Failed("denied".into()),
            OsCall::Done(MEMORY_PRIORITY_NORMAL)
        )
        .failed()
    );
    let not_stuck = raised(OsCall::Done(()), OsCall::Done(MEMORY_PRIORITY_MEDIUM));
    assert_eq!(
        not_stuck.summary(),
        "memory priority low (2), raise to normal: ok, now medium (3)"
    );

    let kept = LevelReport {
        level: ProcessLevel::Io,
        before: OsCall::Done(IO_PRIORITY_NORMAL),
        action: ClassAction::Keep,
        raise: None,
    };
    assert!(!kept.failed());
    assert_eq!(kept.summary(), "I/O priority normal (2), kept");
    let unreadable = LevelReport {
        before: OsCall::Failed("gone".into()),
        ..kept
    };
    assert!(unreadable.failed());
}

/// A process level that did not stick makes the whole startup line a
/// warning.
#[test]
fn a_level_that_does_not_stick_makes_the_startup_line_a_warning() {
    let os = FakeOs::task_priority_7();
    let mut report = apply_process_priority(&os);
    assert_eq!(report.level(), log::Level::Info);
    report.io.raise = Some((OsCall::Done(()), OsCall::Done(IO_PRIORITY_LOW)));
    assert_eq!(report.level(), log::Level::Warn);
}

#[test]
fn the_ingest_thread_is_raised_to_highest_and_read_back() {
    let os = FakeOs::at(NORMAL_PRIORITY_CLASS);
    let report = raise_ingest_thread(&os);
    assert_eq!(os.calls(), ["set_thread 2", "get_thread"]);
    assert_eq!(
        report.summary(),
        "thread priority highest: ok; thread priority now 2"
    );
    assert_eq!(report.level(), log::Level::Info);
}

#[test]
fn a_thread_raise_that_fails_or_does_not_stick_warns() {
    let failed = raise_ingest_thread(&FakeOs {
        set_thread: OsCall::Failed("denied".into()),
        thread_now: OsCall::Done(0),
        ..FakeOs::at(0)
    });
    assert_eq!(failed.level(), log::Level::Warn);
    assert_eq!(
        failed.summary(),
        "thread priority highest: FAILED (denied); thread priority now 0"
    );

    let not_stuck = raise_ingest_thread(&FakeOs {
        thread_now: OsCall::Done(1),
        ..FakeOs::at(0)
    });
    assert_eq!(not_stuck.level(), log::Level::Warn);

    let unreadable = raise_ingest_thread(&FakeOs {
        thread_now: OsCall::Failed("gone".into()),
        ..FakeOs::at(0)
    });
    assert_eq!(unreadable.level(), log::Level::Warn);

    let unsupported = raise_ingest_thread(&FakeOs {
        set_thread: OsCall::Unsupported,
        thread_now: OsCall::Unsupported,
        ..FakeOs::at(0)
    });
    assert_eq!(unsupported.level(), log::Level::Info);
}

/// The real OS off Windows: every call is unsupported.
#[cfg(not(windows))]
#[test]
fn system_os_off_windows_supports_nothing() {
    let os = SystemPriorityOs;
    assert_eq!(os.priority_class(), OsCall::Unsupported);
    assert_eq!(
        os.set_priority_class(NORMAL_PRIORITY_CLASS),
        OsCall::Unsupported
    );
    assert_eq!(os.set_power_throttling(ecoqos_off()), OsCall::Unsupported);
    assert_eq!(os.set_current_thread_priority(2), OsCall::Unsupported);
    assert_eq!(os.current_thread_priority(), OsCall::Unsupported);
    for level in [ProcessLevel::Memory, ProcessLevel::Io] {
        assert_eq!(os.process_level(level), OsCall::Unsupported);
        assert_eq!(os.set_process_level(level, 5), OsCall::Unsupported);
    }
}

/// The real OS on Windows (the windows-latest Test job): the calls work, a
/// fresh thread can be raised to HIGHEST, and the process class is known.
#[cfg(windows)]
#[test]
fn system_os_on_windows_raises_a_thread_and_reads_the_class() {
    let report = std::thread::spawn(|| raise_ingest_thread(&SystemPriorityOs))
        .join()
        .expect("thread");
    assert_eq!(report.set, OsCall::Done(()));
    assert_eq!(report.now, OsCall::Done(THREAD_PRIORITY_HIGHEST));
    match SystemPriorityOs.priority_class() {
        OsCall::Done(class) => assert!(class_rank(class).is_some(), "{class:#x}"),
        other => panic!("GetPriorityClass: {other:?}"),
    }
    assert_eq!(
        SystemPriorityOs.set_power_throttling(ecoqos_off()),
        OsCall::Done(())
    );
}

/// The real OS on Windows (the windows-latest Test job): the memory and I/O
/// priorities read back, and setting them to Normal (which a process may
/// always do for itself) sticks.
#[cfg(windows)]
#[test]
fn system_os_on_windows_reads_and_sets_memory_and_io_priority() {
    for (level, normal) in [
        (ProcessLevel::Memory, MEMORY_PRIORITY_NORMAL),
        (ProcessLevel::Io, IO_PRIORITY_NORMAL),
    ] {
        match SystemPriorityOs.process_level(level) {
            OsCall::Done(_) => {}
            other => panic!("{level:?}: {other:?}"),
        }
        assert_eq!(
            SystemPriorityOs.set_process_level(level, normal),
            OsCall::Done(()),
            "{level:?}"
        );
        assert_eq!(
            SystemPriorityOs.process_level(level),
            OsCall::Done(normal),
            "{level:?}"
        );
    }
}
