# Diagnostics

HVD can record stage timings, progress events, and process resource samples to help explain slowdowns or apparent stalls. Diagnostics are opt-in and do not change the duplicate-search result or submission policy.

## Enable diagnostics

Add `--diagnostic-mode` to a normal or test run:

```sh
python -m hydrusvideodeduplicator --diagnostic-mode
```

Set the sampling interval with `--diagnostics-interval` in seconds (default: 2 seconds; the watchdog samples no more frequently than every 0.5 seconds). Choose the output directory with `--log-path`. Without that option, files go to `<DEDUP_DATABASE_DIR>/logs`.

Diagnostics are written only when `--diagnostic-mode` is present. Test-run JSON result reports are a separate output and do not require diagnostics to be enabled.

## Files created

Each diagnostic session uses a UTC timestamp and short unique ID in its filenames. It creates four newline-delimited JSON event logs:

| File prefix | Contents |
| --- | --- |
| `run-diagnostics-…` | Session start/end, overall stages, and watchdog startup issues. |
| `hash-diagnostics-…` | Hydrus file selection, file access, download, decode/perceptual hashing, and hash-database writes. |
| `efi-diagnostics-…` | Hash-cache fingerprinting and EFI index build/seal activity. Older tree-insertion stage names are also assigned to this phase. |
| `search-diagnostics-…` | EFI queries, legacy comparisons, result reporting, and submission/database commits. |

The `run` log has a matching `.summary.json` file with elapsed time, per-stage totals, dropped event count, and paths to the phase/resource logs. Stage durations are inclusive: nested stages overlap, so do not add their durations together.

The independent resource monitor writes a `.resources.jsonl` file and a matching `.resources.jsonl.summary.json`. Resource samples include the active phase/stage, stage elapsed time, time since observed progress, progress counters, CPU usage, and memory. Where available, they also include process I/O transfer counters. The monitor’s scope is the HVD process and its threads, not the watchdog or other processes. Its temporary `.status` file is removed at normal shutdown.

## What the events show

Event records include a UTC timestamp, elapsed seconds since startup, event name, and phase. Depending on the stage, they can also include:

- operation start, completion, interruption, duration, and exception type;
- file-hashing totals and current progress;
- local-path lookup/use/fallback, HTTP download byte count, and decode/hash duration;
- EFI collections fingerprinted/added, index build details, and query counters such as completed frame hashes, Hamming checks, and posting visits;
- duplicate-search progress, comparison counts, and selected comparison details (up to 1,000 by default);
- watchdog resource samples, including resident memory, CPU time/percentage, process I/O counters where available, and sampled/lifetime peaks where supported.

Event recording is capped at 100,000 events per session by default. Once that limit is reached, nonessential events are dropped; session-end and interruption/monitor-unavailable events are retained, and the summary reports the dropped count. Detailed comparison events are separately capped; aggregate comparison counts continue after the cap.

## Reading resource values

- `cpu_percent_one_core` uses 100% for one logical CPU and can exceed 100% when the process uses multiple cores.
- I/O values are cumulative process transfer counters, not physical disk throughput.
- Sampled memory peaks can miss short spikes. Operating-system lifetime peak fields are included where available.
- Resource fields vary by operating system and may be absent if the operating system cannot provide them.

## Privacy and sharing

Diagnostic event data can include Hydrus file hashes, comparison IDs, platform/Python/package versions, and paths to the diagnostic files. Keep logs private and inspect them before sharing. The logs record metadata and measurements, not video contents or API keys.

## Interpreting stalls

Use the active stage, progress counters, `seconds_since_observed_progress`, CPU, and memory together. A phase with no internal counter can be active without changing the last-progress time; the watchdog marks that condition in its sample. High CPU can indicate active hashing/comparison, while low CPU and flat progress can point toward waiting on Hydrus, storage, or another external operation. These signals narrow down where to investigate; they do not by themselves identify the cause.
