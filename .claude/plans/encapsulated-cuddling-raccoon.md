# Fix: E2E OBS-YouTube delivering server wait + Add strict ownership rule

## Context

Two issues from the current CI run:

1. **E2E OBS-YouTube test fails** at "Wait for delivering server ready" because there is no initial sleep after "Activate delivering". The E2E Streaming Test succeeds because it has `Start-Sleep -Seconds 30` (line 1038) giving the Celery task time to execute and ffmpeg endpoints time to start. The OBS-YouTube test immediately starts polling, hitting the race condition where `endpoints_alive` stays False for the full 4.5 min retry window.

2. **User demands strict rule**: Agent must never dismiss CI failures as "pre-existing" or "unrelated" - all failures are the agent's responsibility to fix.

## Changes

### 1. Add initial wait after "Activate delivering" in OBS-YouTube test

**File:** `.github/workflows/rust-ci.yml` (lines 1740-1744)

Insert a 30-second sleep at the start of the "Wait for delivering server ready" step, matching what the streaming test does. Change line 1744 from immediately polling to:

```powershell
Write-Host "Waiting 30s for Celery task to execute and ffmpeg endpoints to start..."
Start-Sleep -Seconds 30
Write-Host "Now polling for delivering server status..."
```

### 2. Add strict ownership rule to CLAUDE.md

**File:** `CLAUDE.md` (after "CI Monitoring" section, before "Testing" section)

Add new subsection:

```markdown
#### Agent Ownership of CI Failures (ABSOLUTE RULE)

- **EVERY CI failure is YOUR responsibility.** You have full control over all parts of the system — code, CI workflows, deployment, E2E tests, infrastructure. There are NO "pre-existing issues", NO "unrelated failures", NO "flaky tests".
- **NEVER dismiss a CI failure** with phrases like "pre-existing", "unrelated to our change", "infrastructure issue", "known issue", or "flaky". If CI is red, it is YOUR job to make it green before claiming the work is done.
- **NEVER hand the user a red PR** and suggest they merge anyway or re-run the failed job. Fix it yourself.
- If a failure occurs in a part of the system you didn't directly change, that means you failed to account for the full impact of your work. Investigate and fix it.
```

### 3. Update MEMORY.md

Add entry about this pattern: always add initial sleep after activating delivering before polling.

## Files to modify

| File                            | Change                                                            |
| ------------------------------- | ----------------------------------------------------------------- |
| `.github/workflows/rust-ci.yml` | Add 30s sleep at start of "Wait for delivering server ready" step |
| `CLAUDE.md`                     | Add "Agent Ownership of CI Failures" rule                         |
| `memory/MEMORY.md`              | Add delivering activation wait pattern                            |

## Verification

1. Push to dev, monitor CI
2. Check "Wait for delivering server ready" step logs - should show 30s sleep then successful endpoint detection
3. Full E2E OBS-YouTube test should pass (all steps green)
4. PR must reach `mergeable_state: "clean"` before reporting to user
