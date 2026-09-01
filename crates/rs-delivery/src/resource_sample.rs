//! VPS resource sampling (#353).
//!
//! rs-delivery runs on a Hetzner Linux VPS whose server type (`cpx32` for 3+
//! endpoints) was never validated against real CPU/RAM/disk usage. This module
//! samples the host's resources ~1/min and emits a `VpsResourceSample` audit
//! row into the [`crate::AuditRing`]. The host-side mirror (`mirror_vps_audit`)
//! pulls that row into the stream.lan `audit_log`, so the numbers survive VPS
//! deletion and can back a data-driven tier choice for the next event.
//!
//! All parsing is dependency-free (`/proc` reads + a `df -kP` shell-out) and
//! split into PURE functions so it is unit-tested against fixture strings
//! without touching the filesystem. Sampling is best-effort telemetry: a read
//! that fails degrades the affected field (0 / `None`) and logs a warning — it
//! never aborts delivery.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::AppState;
use rs_core::audit::{Action, Severity, Source};

/// How often the sampler ticks. 1/min matches the other rate-limited telemetry
/// (DiskCachePushSample, lifecycle samples) and stays well under the 500-row
/// audit ring cap over a multi-hour event.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(60);

/// A single point-in-time resource reading of the delivery VPS.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceSample {
    /// System-wide CPU busy percentage (0..=100) over the last sample interval.
    pub cpu_pct: f64,
    /// Number of logical CPUs (context for `cpu_pct` and `load_avg_1m`).
    pub ncpu: usize,
    /// 1-minute load average (`/proc/loadavg` field 1).
    pub load_avg_1m: f64,
    /// Used system memory in MiB (`MemTotal - MemAvailable`).
    pub mem_used_mb: u64,
    /// Total system memory in MiB.
    pub mem_total_mb: u64,
    /// rs-delivery process resident set size in MiB (`/proc/self/status` VmRSS).
    pub proc_rss_mb: u64,
    /// Used disk in MiB on the sampled mount (best-effort `df`; `None` on error).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_used_mb: Option<u64>,
    /// Total disk in MiB on the sampled mount (best-effort `df`; `None` on error).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_total_mb: Option<u64>,
}

/// Cumulative CPU jiffies from `/proc/stat`'s aggregate `cpu` line. CPU% is a
/// DELTA between two of these, so the sampler keeps the previous reading.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CpuTimes {
    pub total: u64,
    pub idle: u64,
}

/// Parse the aggregate `cpu` line of `/proc/stat` into cumulative jiffies.
/// `idle` folds in `iowait` (both are "not busy"); `total` sums every column.
pub fn parse_proc_stat_cpu(proc_stat: &str) -> Option<CpuTimes> {
    let line = proc_stat
        .lines()
        .find(|l| l.starts_with("cpu ") || *l == "cpu")?;
    let nums: Vec<u64> = line
        .split_whitespace()
        .skip(1) // the "cpu" label
        .filter_map(|f| f.parse::<u64>().ok())
        .collect();
    if nums.len() < 4 {
        return None; // need at least user/nice/system/idle
    }
    let total: u64 = nums.iter().sum();
    // idle = column 3 (0-based) + iowait (column 4) when present.
    let idle = nums[3] + nums.get(4).copied().unwrap_or(0);
    Some(CpuTimes { total, idle })
}

/// System-wide busy percentage between two `/proc/stat` samples. Returns 0.0
/// when no time elapsed (identical totals) — avoids a divide-by-zero on the
/// first tick or a too-fast re-read.
pub fn cpu_pct(prev: &CpuTimes, cur: &CpuTimes) -> f64 {
    let total_delta = cur.total.saturating_sub(prev.total);
    if total_delta == 0 {
        return 0.0;
    }
    let idle_delta = cur.idle.saturating_sub(prev.idle);
    let busy = total_delta.saturating_sub(idle_delta);
    (busy as f64 / total_delta as f64) * 100.0
}

/// Count logical CPUs from `/proc/stat` — every `cpuN` (digit-suffixed) line.
pub fn count_cpus(proc_stat: &str) -> usize {
    proc_stat
        .lines()
        .filter(|l| {
            l.strip_prefix("cpu")
                .is_some_and(|rest| rest.chars().next().is_some_and(|c| c.is_ascii_digit()))
        })
        .count()
}

/// Parse `/proc/meminfo` into `(MemTotal_kB, MemAvailable_kB)`.
pub fn parse_meminfo(meminfo: &str) -> Option<(u64, u64)> {
    let mut total = None;
    let mut avail = None;
    for line in meminfo.lines() {
        if let Some(v) = meminfo_kb(line, "MemTotal:") {
            total = Some(v);
        } else if let Some(v) = meminfo_kb(line, "MemAvailable:") {
            avail = Some(v);
        }
    }
    Some((total?, avail?))
}

/// `MemTotal:       16333764 kB` -> `16333764`, for the given `key`.
fn meminfo_kb(line: &str, key: &str) -> Option<u64> {
    line.strip_prefix(key)?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// Parse the rs-delivery process RSS (`VmRSS: <n> kB`) from `/proc/self/status`.
pub fn parse_vmrss_kb(status: &str) -> Option<u64> {
    status.lines().find_map(|line| meminfo_kb(line, "VmRSS:"))
}

/// Parse the 1-minute load average (first field of `/proc/loadavg`).
pub fn parse_loadavg_1m(loadavg: &str) -> Option<f64> {
    loadavg.split_whitespace().next()?.parse().ok()
}

/// Parse `df -kP <path>` output into `(total_kB, used_kB)`. POSIX `-P`
/// guarantees one logical line per filesystem; the first data line whose 2nd
/// and 3rd fields are numeric (1024-blocks total + used) is taken.
pub fn parse_df_kp(df_output: &str) -> Option<(u64, u64)> {
    for line in df_output.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() < 6 {
            continue;
        }
        if let (Ok(total), Ok(used)) = (f[1].parse::<u64>(), f[2].parse::<u64>()) {
            return Some((total, used));
        }
    }
    None
}

/// Assemble a [`ResourceSample`] from raw source strings — the pure core, fully
/// unit-testable without touching `/proc` or spawning `df`. Returns the sample
/// AND the current [`CpuTimes`] so the caller can carry it forward for the next
/// delta.
pub fn sample_from_sources(
    proc_stat: &str,
    meminfo: &str,
    self_status: &str,
    loadavg: &str,
    df_output: Option<&str>,
    prev_cpu: Option<CpuTimes>,
) -> (ResourceSample, Option<CpuTimes>) {
    let cur_cpu = parse_proc_stat_cpu(proc_stat);
    let cpu = match (prev_cpu, cur_cpu) {
        (Some(prev), Some(cur)) => cpu_pct(&prev, &cur),
        _ => 0.0, // first tick, or an unreadable /proc/stat
    };

    let (mem_total_kb, mem_avail_kb) = parse_meminfo(meminfo).unwrap_or((0, 0));
    let mem_used_kb = mem_total_kb.saturating_sub(mem_avail_kb);
    let rss_kb = parse_vmrss_kb(self_status).unwrap_or(0);
    let load_avg_1m = parse_loadavg_1m(loadavg).unwrap_or(0.0);
    let (disk_used_mb, disk_total_mb) = match df_output.and_then(parse_df_kp) {
        Some((total_kb, used_kb)) => (Some(used_kb / 1024), Some(total_kb / 1024)),
        None => (None, None),
    };

    let sample = ResourceSample {
        cpu_pct: (cpu * 10.0).round() / 10.0, // one decimal place
        ncpu: count_cpus(proc_stat),
        load_avg_1m,
        mem_used_mb: mem_used_kb / 1024,
        mem_total_mb: mem_total_kb / 1024,
        proc_rss_mb: rss_kb / 1024,
        disk_used_mb,
        disk_total_mb,
    };
    (sample, cur_cpu)
}

/// Read the raw source strings from the live host. Best-effort: a missing file
/// or a failed `df` yields an empty string / `None`, which `sample_from_sources`
/// degrades gracefully.
fn read_live_sample(
    prev_cpu: Option<CpuTimes>,
    disk_path: &str,
) -> (ResourceSample, Option<CpuTimes>) {
    let proc_stat = std::fs::read_to_string("/proc/stat").unwrap_or_default();
    let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
    let self_status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let loadavg = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
    let df_output = std::process::Command::new("df")
        .args(["-kP", disk_path])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).into_owned());

    sample_from_sources(
        &proc_stat,
        &meminfo,
        &self_status,
        &loadavg,
        df_output.as_deref(),
        prev_cpu,
    )
}

/// Background task: sample the VPS resources every [`SAMPLE_INTERVAL`], store
/// the latest reading on [`AppState`] (so `/api/status` exposes it live) and
/// push a `VpsResourceSample` audit row (so the host mirrors it durably).
/// Spawned once from `main()`. `disk_path` is the mount to `df` (`/` = the
/// whole VPS disk, the tier-relevant figure).
pub async fn run_sampler(state: Arc<AppState>, disk_path: String) {
    let mut prev_cpu: Option<CpuTimes> = None;
    // Prime the CPU baseline so the FIRST emitted sample carries a real busy%
    // rather than 0.0 (the delta needs two readings).
    if let Ok(proc_stat) = std::fs::read_to_string("/proc/stat") {
        prev_cpu = parse_proc_stat_cpu(&proc_stat);
    }
    loop {
        tokio::time::sleep(SAMPLE_INTERVAL).await;
        let (sample, cur_cpu) = read_live_sample(prev_cpu, &disk_path);
        if cur_cpu.is_some() {
            prev_cpu = cur_cpu;
        }
        *state.latest_resource_sample.write().await = Some(sample.clone());
        match serde_json::to_value(&sample) {
            Ok(detail) => {
                state.audit_ring.push(
                    Severity::Info,
                    Source::Vps,
                    None,
                    Action::VpsResourceSample,
                    detail,
                );
            }
            Err(e) => tracing::warn!("resource_sample: serialize failed: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROC_STAT: &str = "\
cpu  100 0 50 800 50 0 0 0 0 0
cpu0 60 0 30 400 20 0 0 0 0 0
cpu1 40 0 20 400 30 0 0 0 0 0
intr 12345
";

    #[test]
    fn parse_proc_stat_cpu_sums_total_and_folds_iowait_into_idle() {
        let ct = parse_proc_stat_cpu(PROC_STAT).unwrap();
        // total = 100+0+50+800+50 = 1000; idle = idle(800)+iowait(50) = 850
        assert_eq!(ct.total, 1000);
        assert_eq!(ct.idle, 850);
    }

    #[test]
    fn parse_proc_stat_cpu_none_on_missing_line() {
        assert!(parse_proc_stat_cpu("intr 1\nctxt 2\n").is_none());
    }

    #[test]
    fn cpu_pct_is_busy_over_total_delta() {
        let prev = CpuTimes {
            total: 1000,
            idle: 850,
        };
        // +200 total, +50 idle => busy delta 150 / 200 = 75%.
        let cur = CpuTimes {
            total: 1200,
            idle: 900,
        };
        assert!((cpu_pct(&prev, &cur) - 75.0).abs() < 1e-9);
    }

    #[test]
    fn cpu_pct_zero_when_no_time_elapsed() {
        let t = CpuTimes {
            total: 500,
            idle: 400,
        };
        assert_eq!(cpu_pct(&t, &t), 0.0);
    }

    #[test]
    fn count_cpus_counts_only_digit_suffixed_lines() {
        // The aggregate "cpu " line does NOT count; cpu0 + cpu1 do.
        assert_eq!(count_cpus(PROC_STAT), 2);
    }

    #[test]
    fn parse_meminfo_returns_total_and_available() {
        let mi = "\
MemTotal:       16333764 kB
MemFree:          200000 kB
MemAvailable:   15000000 kB
Buffers:           10000 kB
";
        assert_eq!(parse_meminfo(mi), Some((16333764, 15000000)));
    }

    #[test]
    fn parse_meminfo_none_when_available_absent() {
        assert!(parse_meminfo("MemTotal: 100 kB\n").is_none());
    }

    #[test]
    fn parse_vmrss_reads_process_rss() {
        let status = "\
Name:\trs-delivery
VmPeak:\t  200000 kB
VmRSS:\t  123456 kB
Threads:\t12
";
        assert_eq!(parse_vmrss_kb(status), Some(123456));
    }

    #[test]
    fn parse_loadavg_takes_first_field() {
        assert_eq!(parse_loadavg_1m("0.52 0.58 0.59 1/234 5678"), Some(0.52));
        assert!(parse_loadavg_1m("").is_none());
    }

    #[test]
    fn parse_df_kp_reads_total_and_used_from_data_line() {
        let df = "\
Filesystem     1024-blocks    Used Available Capacity Mounted on
/dev/sda1        164123456 1234567 156000000       1% /
";
        assert_eq!(parse_df_kp(df), Some((164123456, 1234567)));
    }

    #[test]
    fn parse_df_kp_none_on_header_only() {
        assert!(
            parse_df_kp("Filesystem 1024-blocks Used Available Capacity Mounted on\n").is_none()
        );
    }

    #[test]
    fn sample_from_sources_assembles_all_fields_in_mib() {
        let df = "\
Filesystem     1024-blocks    Used Available Capacity Mounted on
/dev/sda1        164123456 1234567 156000000       1% /
";
        let prev = CpuTimes {
            total: 800,
            idle: 800,
        };
        let self_status = "VmRSS:\t  102400 kB\n"; // 100 MiB
        let meminfo = "MemTotal: 16777216 kB\nMemAvailable: 8388608 kB\n"; // 16 GiB total, 8 used
        let loadavg = "1.25 0.90 0.80 2/300 999";
        let (s, cur) = sample_from_sources(
            PROC_STAT,
            meminfo,
            self_status,
            loadavg,
            Some(df),
            Some(prev),
        );

        // cur cpu total=1000 idle=850; delta over prev(800/800): total 200, idle 50 => 75%.
        assert_eq!(s.cpu_pct, 75.0);
        assert_eq!(
            cur,
            Some(CpuTimes {
                total: 1000,
                idle: 850
            })
        );
        assert_eq!(s.ncpu, 2);
        assert!((s.load_avg_1m - 1.25).abs() < 1e-9);
        assert_eq!(s.mem_total_mb, 16384);
        assert_eq!(s.mem_used_mb, 8192);
        assert_eq!(s.proc_rss_mb, 100);
        assert_eq!(s.disk_total_mb, Some(164123456 / 1024));
        assert_eq!(s.disk_used_mb, Some(1234567 / 1024));
    }

    #[test]
    fn sample_from_sources_first_tick_has_zero_cpu_and_no_disk() {
        let (s, cur) = sample_from_sources(
            PROC_STAT,
            "MemTotal: 1048576 kB\nMemAvailable: 524288 kB\n",
            "VmRSS: 51200 kB\n",
            "0.10 0 0 1/1 1",
            None, // df unavailable
            None, // first tick — no previous CPU baseline
        );
        assert_eq!(s.cpu_pct, 0.0);
        assert!(
            cur.is_some(),
            "the current CpuTimes is still returned to seed the next delta"
        );
        assert_eq!(s.disk_used_mb, None);
        assert_eq!(s.disk_total_mb, None);
        assert_eq!(s.mem_total_mb, 1024);
        assert_eq!(s.mem_used_mb, 512);
    }

    #[test]
    fn resource_sample_round_trips_through_audit_detail_json() {
        // The sample is carried as an audit-row detail Value and mirrored to the
        // host; it must survive serde round-trip (skip_serializing_if on disk
        // fields included).
        let s = ResourceSample {
            cpu_pct: 42.5,
            ncpu: 8,
            load_avg_1m: 3.1,
            mem_used_mb: 2048,
            mem_total_mb: 16384,
            proc_rss_mb: 220,
            disk_used_mb: None,
            disk_total_mb: None,
        };
        let v = serde_json::to_value(&s).unwrap();
        assert!(
            v.get("disk_used_mb").is_none(),
            "None disk field is omitted"
        );
        let back: ResourceSample = serde_json::from_value(v).unwrap();
        assert_eq!(back, s);
    }
}
