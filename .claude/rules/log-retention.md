---
paths:
  - "src-tauri/src/lib.rs"
  - "crates/rs-runtime/src/daily_log*.rs"
---

# restreamer.log retention (#368)

Until #368, `init_tracing` renamed `restreamer.log` to `.log.old` at startup
once it passed 1 MB. Two test restarts after the Sunday 2026-10-04 production
overwrote that day's evidence, and the forensics had to reconstruct the stalls
from DB timestamps.

- `rs_runtime::daily_log::DailyLogFile` is the file writer. The LIVE file is
  always `C:\ProgramData\Restreamer\restreamer.log`: CI's late-join gate, the
  deploy steps and the tray's "show log" (`Get-Content -Tail -Wait`) read that
  exact path. Never switch to `tracing_appender::rolling`, which names the
  live file by date.
- The first write of a new UTC day (log timestamps are UTC) archives the live
  file as `restreamer.<YYYY-MM-DD>.log`, the day it covers. A startup on a
  later day archives the previous live file under its mtime day first. An
  existing archive is never replaced (`.1`, `.2`, ...; at most
  `MAX_ARCHIVES_PER_DAY`, the search is bounded so a mutant cannot loop). ANY
  day change rolls, a clock step back too, so a wrong RTC corrected after
  boot cannot freeze the roll until the next restart.
- The newest 14 archive DAYS are kept (`KEEP_DAYS`); the legacy
  `restreamer.log.old` and foreign files are never touched.
- A refused rename (Windows: a reader without delete sharing) falls back to
  copy + truncate. The rename is an injectable `fn` field, so the fallback is
  tested; a failed archive or prune is tested through the note line. Any
  rotation failure is written into the live file as a plain line. The writer
  never logs through `tracing`: it runs on the `non_blocking` worker, and a
  tracing call there would re-enter its own channel.
- CI's `Get-RestreamerLog` (late-join gate, ci.yml) reads every
  `restreamer.<date>[.N].log` (and the legacy `.old`) written since the step
  started, so a step that crosses UTC midnight keeps its pre-midnight lines.
  A new log reader must do the same.
- It stays behind `tracing_appender::non_blocking` (lossy, 128k lines), so no
  caller waits for the disk. The ingest runtime logs on its hot path.
- Candidates rejected (#368 review): `tracing_appender::rolling` names the
  LIVE file by date; the `file-rotate` crate (docs.rs source read) rotates on
  the file's mtime date, but names the archive by the ROTATION time, not the
  day it covers, and would be a new dependency for ~150 tested lines.
- Tests drive it with an injected clock (`open_with_clock`) and set a file's
  mtime with `File::set_modified`.
