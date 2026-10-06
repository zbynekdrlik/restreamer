//! #368 design test (ii), runtime half: the dedicated ingest runtime runs its
//! tasks on its own thread, raises that thread's priority there, and leaves
//! no thread behind on shutdown or drop.

use super::*;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::ingest_priority::{OsCall, PowerThrottling, THREAD_PRIORITY_HIGHEST};

/// Await `fut` from the test thread on a throwaway runtime.
fn wait<T>(fut: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime")
        .block_on(fut)
}

fn current_thread_name() -> Option<String> {
    std::thread::current().name().map(str::to_owned)
}

/// Records which thread asked for the thread priority.
#[derive(Default)]
struct ThreadOs {
    raised_on: Arc<Mutex<Vec<Option<String>>>>,
}

impl PriorityOs for ThreadOs {
    fn priority_class(&self) -> OsCall<u32> {
        OsCall::Unsupported
    }
    fn set_priority_class(&self, _class: u32) -> OsCall<()> {
        OsCall::Unsupported
    }
    fn set_power_throttling(&self, _state: PowerThrottling) -> OsCall<()> {
        OsCall::Unsupported
    }
    fn set_current_thread_priority(&self, priority: i32) -> OsCall<()> {
        assert_eq!(priority, THREAD_PRIORITY_HIGHEST);
        self.raised_on.lock().unwrap().push(current_thread_name());
        OsCall::Done(())
    }
    fn current_thread_priority(&self) -> OsCall<i32> {
        OsCall::Done(THREAD_PRIORITY_HIGHEST)
    }
}

#[test]
fn tasks_and_blocking_io_run_on_the_ingest_runtime_threads() {
    let mut rt = IngestRuntime::start().expect("ingest runtime");
    let task_thread = wait(rt.handle().spawn(async { current_thread_name() })).unwrap();
    assert_eq!(task_thread.as_deref(), Some(INGEST_THREAD_NAME));
    let io_thread = wait(rt.handle().spawn_blocking(current_thread_name)).unwrap();
    assert_eq!(io_thread.as_deref(), Some(INGEST_BLOCKING_THREAD_NAME));
    rt.shutdown();
}

#[test]
fn the_priority_is_raised_on_the_ingest_thread_itself() {
    let os = ThreadOs::default();
    let raised_on = Arc::clone(&os.raised_on);
    let mut rt = IngestRuntime::start_with(os).expect("ingest runtime");
    assert_eq!(
        *raised_on.lock().unwrap(),
        [Some(INGEST_THREAD_NAME.to_string())],
        "the raise must run once, on the ingest thread (not the caller's)"
    );
    assert_eq!(
        rt.thread_priority(),
        &ThreadPriorityReport {
            set: OsCall::Done(()),
            now: OsCall::Done(THREAD_PRIORITY_HIGHEST),
        }
    );
    rt.shutdown();
}

/// `shutdown` stops the runtime and waits for its thread: a blocking task
/// still running is finished by the time it returns, and the runtime takes
/// no more work.
#[test]
fn shutdown_stops_the_runtime_and_joins_its_thread() {
    let mut rt = IngestRuntime::start().expect("ingest runtime");
    assert!(rt.is_running());
    let handle = rt.handle().clone();
    let written = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicBool::new(false));
    {
        let (written, started) = (Arc::clone(&written), Arc::clone(&started));
        handle.spawn_blocking(move || {
            started.store(true, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(300));
            written.store(true, Ordering::SeqCst);
        });
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while !started.load(Ordering::SeqCst) {
        assert!(Instant::now() < deadline, "the blocking task never started");
        std::thread::sleep(Duration::from_millis(1));
    }

    rt.shutdown();
    assert!(!rt.is_running(), "shutdown joins the runtime thread");
    assert!(
        written.load(Ordering::SeqCst),
        "a blocking write still running is finished before shutdown returns"
    );
    assert!(
        wait(handle.spawn(async {})).is_err(),
        "a shut-down runtime runs no more tasks"
    );
}

/// Dropping it (an orchestrator error path) still stops the runtime: its
/// tasks are dropped, so nothing keeps running on a forgotten thread.
#[test]
fn dropping_the_runtime_stops_it() {
    let rt = IngestRuntime::start().expect("ingest runtime");
    let alive = Arc::new(());
    {
        let held = Arc::clone(&alive);
        rt.handle().spawn(async move {
            let _held = held;
            std::future::pending::<()>().await;
        });
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while Arc::strong_count(&alive) == 1 {
        assert!(Instant::now() < deadline, "the task never started");
        std::thread::sleep(Duration::from_millis(1));
    }
    drop(rt);
    while Arc::strong_count(&alive) > 1 {
        assert!(
            Instant::now() < deadline,
            "a dropped ingest runtime must stop and drop its tasks"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}
