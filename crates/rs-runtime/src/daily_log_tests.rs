//! `DailyLogFile` on an injected clock in a temp dir.

use super::*;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

/// A clock the test moves by hand.
#[derive(Clone)]
struct TestClock(Arc<Mutex<DateTime<Utc>>>);

impl TestClock {
    fn at(rfc3339: &str) -> Self {
        Self(Arc::new(Mutex::new(rfc3339.parse().unwrap())))
    }
    fn set(&self, rfc3339: &str) {
        *self.0.lock().unwrap() = rfc3339.parse().unwrap();
    }
    fn advance_days(&self, days: i64) {
        let mut t = self.0.lock().unwrap();
        *t += chrono::Duration::days(days);
    }
    fn boxed(&self) -> Clock {
        let c = self.0.clone();
        Box::new(move || *c.lock().unwrap())
    }
}

fn day(s: &str) -> NaiveDate {
    s.parse().unwrap()
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_default()
}

/// Archive file names in `dir`, sorted.
fn archives(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| archive_day(n).is_some())
        .collect();
    names.sort();
    names
}

#[test]
fn archive_names_round_trip_and_reject_other_files() {
    assert_eq!(
        archive_name(day("2026-10-04"), 0),
        "restreamer.2026-10-04.log"
    );
    assert_eq!(
        archive_name(day("2026-10-04"), 2),
        "restreamer.2026-10-04.2.log"
    );
    assert_eq!(
        archive_day("restreamer.2026-10-04.log"),
        Some(day("2026-10-04"))
    );
    assert_eq!(
        archive_day("restreamer.2026-10-04.2.log"),
        Some(day("2026-10-04"))
    );
    for other in [
        "restreamer.log",
        "restreamer.log.old",
        "restreamer.2026-10-04.log.old",
        "restreamer.2026-13-04.log",
        "restreamer.2026-10-04..log",
        "restreamer.2026-10-04.x.log",
        "restreamerX2026-10-04.log",
        "other.2026-10-04.log",
        "stall.log",
    ] {
        assert_eq!(archive_day(other), None, "{other}");
    }
}

#[test]
fn a_new_utc_day_archives_the_live_file_under_the_day_it_covers() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::at("2026-10-04T09:00:00Z");
    let mut log = DailyLogFile::open_with_clock(dir.path(), KEEP_DAYS, clock.boxed()).unwrap();
    log.write_all(b"sunday 09:16 stall\n").unwrap();
    clock.set("2026-10-04T23:59:59Z");
    log.write_all(b"sunday late\n").unwrap();
    clock.set("2026-10-05T00:00:00Z");
    log.write_all(b"monday\n").unwrap();
    log.flush().unwrap();

    assert_eq!(
        read(&dir.path().join("restreamer.2026-10-04.log")),
        "sunday 09:16 stall\nsunday late\n"
    );
    assert_eq!(read(&log.live_path()), "monday\n");
    assert_eq!(log.live_path(), dir.path().join("restreamer.log"));
}

#[test]
fn a_restart_on_the_same_day_appends() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::at("2026-10-04T09:00:00Z");
    let mut log = DailyLogFile::open_with_clock(dir.path(), KEEP_DAYS, clock.boxed()).unwrap();
    log.write_all(b"before restart\n").unwrap();
    drop(log);
    // The file's mtime is the real now; let the clock agree with it.
    let now: DateTime<Utc> = SystemTime::now().into();
    clock.set(&now.to_rfc3339());
    let mut log = DailyLogFile::open_with_clock(dir.path(), KEEP_DAYS, clock.boxed()).unwrap();
    log.write_all(b"after restart\n").unwrap();
    log.flush().unwrap();
    assert_eq!(read(&log.live_path()), "before restart\nafter restart\n");
    assert!(archives(dir.path()).is_empty());
}

#[test]
fn a_restart_on_a_later_day_archives_the_previous_live_file_first() {
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("restreamer.log");
    fs::write(&live, "sunday's production\n").unwrap();
    let sunday: SystemTime = "2026-10-04T12:00:00Z"
        .parse::<DateTime<Utc>>()
        .unwrap()
        .into();
    File::options()
        .write(true)
        .open(&live)
        .unwrap()
        .set_modified(sunday)
        .unwrap();

    let clock = TestClock::at("2026-10-06T08:00:00Z");
    let mut log = DailyLogFile::open_with_clock(dir.path(), KEEP_DAYS, clock.boxed()).unwrap();
    log.write_all(b"tuesday\n").unwrap();
    log.flush().unwrap();
    assert_eq!(
        read(&dir.path().join("restreamer.2026-10-04.log")),
        "sunday's production\n",
        "archived under its own last-modified day"
    );
    assert_eq!(read(&live), "tuesday\n");
}

#[test]
fn an_existing_archive_of_the_same_day_is_never_replaced() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("restreamer.2026-10-04.log"), "first\n").unwrap();
    fs::write(dir.path().join("restreamer.2026-10-04.1.log"), "second\n").unwrap();
    let clock = TestClock::at("2026-10-04T10:00:00Z");
    let mut log = DailyLogFile::open_with_clock(dir.path(), KEEP_DAYS, clock.boxed()).unwrap();
    log.write_all(b"third\n").unwrap();
    clock.set("2026-10-05T00:00:01Z");
    log.write_all(b"next day\n").unwrap();
    assert_eq!(
        read(&dir.path().join("restreamer.2026-10-04.log")),
        "first\n"
    );
    assert_eq!(
        read(&dir.path().join("restreamer.2026-10-04.1.log")),
        "second\n"
    );
    assert_eq!(
        read(&dir.path().join("restreamer.2026-10-04.2.log")),
        "third\n"
    );
}

#[test]
fn rotation_keeps_14_days_of_archives() {
    let dir = tempfile::tempdir().unwrap();
    // A foreign file and the legacy .old are never touched.
    fs::write(dir.path().join("restreamer.log.old"), "legacy").unwrap();
    fs::write(dir.path().join("config.json"), "{}").unwrap();
    let clock = TestClock::at("2026-09-01T12:00:00Z");
    let mut log = DailyLogFile::open_with_clock(dir.path(), KEEP_DAYS, clock.boxed()).unwrap();
    for i in 0..20 {
        log.write_all(format!("day {i}\n").as_bytes()).unwrap();
        clock.advance_days(1);
    }
    log.write_all(b"day 20\n").unwrap();
    log.flush().unwrap();

    let kept = archives(dir.path());
    assert_eq!(kept.len(), 14, "{kept:?}");
    assert_eq!(kept.first().unwrap(), "restreamer.2026-09-07.log");
    assert_eq!(kept.last().unwrap(), "restreamer.2026-09-20.log");
    assert_eq!(
        read(&dir.path().join("restreamer.2026-09-20.log")),
        "day 19\n"
    );
    assert_eq!(read(&log.live_path()), "day 20\n", "today's live file");
    assert!(dir.path().join("restreamer.log.old").exists());
    assert!(dir.path().join("config.json").exists());
}

#[test]
fn prune_keeps_every_archive_of_a_kept_day() {
    let dir = tempfile::tempdir().unwrap();
    for name in [
        "restreamer.2026-10-01.log",
        "restreamer.2026-10-02.log",
        "restreamer.2026-10-02.1.log",
        "restreamer.2026-10-03.log",
    ] {
        fs::write(dir.path().join(name), "x").unwrap();
    }
    let clock = TestClock::at("2026-10-04T00:00:00Z");
    let log = DailyLogFile::open_with_clock(dir.path(), 2, clock.boxed()).unwrap();
    assert_eq!(
        archives(dir.path()),
        vec![
            "restreamer.2026-10-02.1.log",
            "restreamer.2026-10-02.log",
            "restreamer.2026-10-03.log",
        ]
    );
    assert!(log.prune().unwrap().is_empty(), "nothing more to delete");
    let few = DailyLogFile::open_with_clock(dir.path(), 14, clock.boxed()).unwrap();
    assert!(
        few.prune().unwrap().is_empty(),
        "fewer days than kept: no-op"
    );
}

#[test]
fn a_clock_step_back_rolls_without_losing_anything() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::at("2026-10-05T00:00:10Z");
    let mut log = DailyLogFile::open_with_clock(dir.path(), KEEP_DAYS, clock.boxed()).unwrap();
    log.write_all(b"a\n").unwrap();
    clock.set("2026-10-04T23:59:59Z");
    log.write_all(b"b\n").unwrap();
    clock.set("2026-10-05T00:00:11Z");
    log.write_all(b"c\n").unwrap();
    assert_eq!(read(&dir.path().join("restreamer.2026-10-05.log")), "a\n");
    assert_eq!(read(&dir.path().join("restreamer.2026-10-04.log")), "b\n");
    assert_eq!(read(&log.live_path()), "c\n");
}

#[test]
fn a_refused_rename_falls_back_to_copy_and_truncate() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::at("2026-10-04T10:00:00Z");
    let mut log = DailyLogFile::open_with_clock(dir.path(), KEEP_DAYS, clock.boxed()).unwrap();
    // Windows refuses to rename a file another process holds open without
    // delete sharing (the tray's Get-Content -Wait).
    log.rename = |_, _| Err(io::Error::other("held open"));
    log.write_all(b"sunday\n").unwrap();
    clock.set("2026-10-05T00:00:01Z");
    log.write_all(b"monday\n").unwrap();
    log.flush().unwrap();
    assert_eq!(
        read(&dir.path().join("restreamer.2026-10-04.log")),
        "sunday\n"
    );
    assert_eq!(
        read(&log.live_path()),
        "monday\n",
        "the live file was truncated"
    );
}

#[test]
fn a_failed_archive_is_noted_in_the_live_file_and_logging_goes_on() {
    let dir = tempfile::tempdir().unwrap();
    for n in 0..MAX_ARCHIVES_PER_DAY {
        File::create(dir.path().join(archive_name(day("2026-10-04"), n))).unwrap();
    }
    let clock = TestClock::at("2026-10-04T10:00:00Z");
    let mut log = DailyLogFile::open_with_clock(dir.path(), KEEP_DAYS, clock.boxed()).unwrap();
    log.write_all(b"sunday\n").unwrap();
    clock.set("2026-10-05T00:00:01Z");
    log.write_all(b"monday\n").unwrap();
    log.flush().unwrap();
    let live = read(&log.live_path());
    assert!(live.starts_with("sunday\n"), "{live}");
    assert!(
        live.contains(
            "restreamer.log: archiving restreamer.2026-10-04.log failed \
             (1000 archives of 2026-10-04 already exist)"
        ),
        "{live}"
    );
    assert!(live.ends_with("monday\n"), "{live}");
}

#[test]
fn a_failed_prune_is_noted_in_the_live_file() {
    let dir = tempfile::tempdir().unwrap();
    // A directory with an archive's name cannot be removed as a file.
    fs::create_dir(dir.path().join("restreamer.2026-09-01.log")).unwrap();
    fs::write(dir.path().join("restreamer.2026-09-02.log"), "x").unwrap();
    let clock = TestClock::at("2026-10-04T10:00:00Z");
    let mut log = DailyLogFile::open_with_clock(dir.path(), 1, clock.boxed()).unwrap();
    log.flush().unwrap();
    let live = read(&log.live_path());
    assert!(
        live.contains("restreamer.log: pruning old log archives failed"),
        "{live}"
    );
}

#[test]
fn the_writer_reopens_a_live_file_that_could_not_be_opened_on_roll() {
    let dir = tempfile::tempdir().unwrap();
    let clock = TestClock::at("2026-10-04T10:00:00Z");
    let mut log = DailyLogFile::open_with_clock(dir.path(), KEEP_DAYS, clock.boxed()).unwrap();
    log.file = None;
    log.write_all(b"still logged\n").unwrap();
    log.flush().unwrap();
    assert_eq!(read(&log.live_path()), "still logged\n");
}
