//! Daily rolling `restreamer.log`, kept 14 days (#368).
//!
//! Until #368 the log was one file that `init_tracing` renamed to
//! `restreamer.log.old` at startup once it passed 1 MB. Two test restarts
//! after the Sunday 2026-10-04 production overwrote that day's evidence.
//!
//! Now the live file keeps its fixed name `restreamer.log` (CI's late-join
//! gate, the deploy steps and the tray's "show log" all read that path).
//! When the first line of a new UTC day is written (log timestamps are UTC),
//! the live file is archived as `restreamer.<YYYY-MM-DD>.log` (the day it
//! covers) and a fresh one is started. Archives older than the newest
//! `keep_days` dates are deleted. A startup on a later day archives the
//! previous live file under ITS last-modified date first, so a restart
//! never mixes two days and never discards one.
//!
//! The writer runs on `tracing_appender::non_blocking`'s worker thread, so
//! no logging caller (the ingest runtime included) ever waits for the
//! disk, a rename or a prune. It never logs through `tracing` itself (that
//! would re-enter its own channel): a rotation failure is written into the
//! live file as a plain line, and logging carries on in the un-rotated file.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, NaiveDate, Utc};

/// The live log file name (and the archives' prefix and suffix).
pub const LOG_STEM: &str = "restreamer";
/// Days of archives kept next to the live file.
pub const KEEP_DAYS: usize = 14;

/// The wall clock the writer rolls on.
pub type Clock = Box<dyn Fn() -> DateTime<Utc> + Send>;

/// Archives of one day at most (`.1` ... `.N`); past that, archiving fails
/// and logging continues in the live file.
pub const MAX_ARCHIVES_PER_DAY: u32 = 1_000;

/// `restreamer.log` that rolls daily. Implements `Write` for
/// `tracing_appender::non_blocking`.
pub struct DailyLogFile {
    dir: PathBuf,
    keep_days: usize,
    day: NaiveDate,
    file: Option<File>,
    clock: Clock,
    /// `fs::rename`; tests inject a refusing one to reach the fallback.
    rename: fn(&Path, &Path) -> io::Result<()>,
}

/// `restreamer.<day>.log`, or `restreamer.<day>.<n>.log` for `n > 0` (a
/// second archive of the same day, e.g. after a wall-clock step).
pub fn archive_name(day: NaiveDate, n: u32) -> String {
    if n == 0 {
        format!("{LOG_STEM}.{}.log", day.format("%Y-%m-%d"))
    } else {
        format!("{LOG_STEM}.{}.{n}.log", day.format("%Y-%m-%d"))
    }
}

/// The day of an archive file name, `None` for any other file (the live
/// file, the legacy `restreamer.log.old`, foreign files).
pub fn archive_day(name: &str) -> Option<NaiveDate> {
    let middle = name
        .strip_prefix(LOG_STEM)?
        .strip_prefix('.')?
        .strip_suffix(".log")?;
    let (date, n) = match middle.split_once('.') {
        Some((date, n)) => (date, Some(n)),
        None => (middle, None),
    };
    if n.is_some_and(|n| n.is_empty() || !n.bytes().all(|b| b.is_ascii_digit())) {
        return None;
    }
    NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()
}

impl DailyLogFile {
    /// Open `<dir>/restreamer.log` on the system clock.
    pub fn open(dir: &Path, keep_days: usize) -> io::Result<Self> {
        Self::open_with_clock(dir, keep_days, Box::new(Utc::now))
    }

    /// Open `<dir>/restreamer.log`, rolling on `clock`. A live file last
    /// written on an earlier day is archived under that day first.
    pub fn open_with_clock(dir: &Path, keep_days: usize, clock: Clock) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let today = clock().date_naive();
        let mut log = Self {
            dir: dir.to_path_buf(),
            keep_days,
            day: today,
            file: None,
            clock,
            rename: |from, to| fs::rename(from, to),
        };
        let mut note = None;
        if let Some(day) = log.live_file_day() {
            if day < today {
                note = log.archive_live(day).err();
            }
        }
        log.file = Some(log.open_live()?);
        if let Some(e) = note {
            log.note(&format!(
                "archiving the previous restreamer.log failed: {e}"
            ));
        }
        log.prune_and_note();
        Ok(log)
    }

    /// `<dir>/restreamer.log`.
    pub fn live_path(&self) -> PathBuf {
        self.dir.join(format!("{LOG_STEM}.log"))
    }

    /// The UTC day the existing live file was last written, if it exists.
    fn live_file_day(&self) -> Option<NaiveDate> {
        let modified = fs::metadata(self.live_path()).ok()?.modified().ok()?;
        Some(DateTime::<Utc>::from(modified).date_naive())
    }

    fn open_live(&self) -> io::Result<File> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.live_path())
    }

    /// Move the (closed) live file to the archive of `day`. Never replaces
    /// an existing archive: a second one of the same day gets `.1`, `.2`, ...
    /// If the rename is refused (Windows: another process holds the file
    /// without delete sharing), the content is copied and the live file
    /// truncated instead.
    fn archive_live(&self, day: NaiveDate) -> io::Result<()> {
        let target = (0..MAX_ARCHIVES_PER_DAY)
            .map(|n| self.dir.join(archive_name(day, n)))
            .find(|p| !p.exists())
            .ok_or_else(|| {
                io::Error::other(format!(
                    "{MAX_ARCHIVES_PER_DAY} archives of {day} already exist"
                ))
            })?;
        if (self.rename)(&self.live_path(), &target).is_ok() {
            return Ok(());
        }
        fs::copy(self.live_path(), &target)?;
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(self.live_path())
            .map(drop)
    }

    /// Delete archives whose day is not among the newest `keep_days`.
    /// Returns the files deleted.
    pub fn prune(&self) -> io::Result<Vec<PathBuf>> {
        let mut archives: Vec<(NaiveDate, PathBuf)> = fs::read_dir(&self.dir)?
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                Some((archive_day(&name)?, e.path()))
            })
            .collect();
        let mut days: Vec<NaiveDate> = archives.iter().map(|(d, _)| *d).collect();
        days.sort_unstable_by(|a, b| b.cmp(a));
        days.dedup();
        let Some(&oldest_kept) = days.get(self.keep_days.saturating_sub(1)) else {
            return Ok(Vec::new());
        };
        archives.retain(|(d, _)| *d < oldest_kept);
        let mut removed = Vec::new();
        for (_, path) in archives {
            fs::remove_file(&path)?;
            removed.push(path);
        }
        Ok(removed)
    }

    fn prune_and_note(&mut self) {
        if let Err(e) = self.prune() {
            self.note(&format!("pruning old log archives failed: {e}"));
        }
    }

    /// A plain line about the log itself, into the live file.
    fn note(&mut self, message: &str) {
        if let Some(f) = self.file.as_mut() {
            let _ = writeln!(
                f,
                "{} restreamer.log: {message}",
                (self.clock)().to_rfc3339()
            );
        }
    }

    /// Start a new day: archive the live file under the day it covers.
    fn roll(&mut self, today: NaiveDate) {
        let covered = std::mem::replace(&mut self.day, today);
        if let Some(mut f) = self.file.take() {
            let _ = f.flush();
        }
        let archived = self.archive_live(covered);
        match self.open_live() {
            Ok(f) => self.file = Some(f),
            Err(_) => return,
        }
        if let Err(e) = archived {
            self.note(&format!(
                "archiving {} failed ({e}); this file continues it",
                archive_name(covered, 0)
            ));
        }
        self.prune_and_note();
    }
}

impl Write for DailyLogFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Any day change rolls, a clock step back too: a wrong clock that is
        // corrected later must not freeze the roll until the next restart.
        let today = (self.clock)().date_naive();
        if today != self.day {
            self.roll(today);
        }
        if self.file.is_none() {
            self.file = Some(self.open_live()?);
        }
        self.file.as_mut().expect("opened just above").write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.file.as_mut() {
            Some(f) => f.flush(),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
#[path = "daily_log_tests.rs"]
mod tests;
