"""Opt-in stage tracing with independent resource and progress monitoring."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import importlib.metadata
import mmap
import os
import platform
import subprocess
import sys
import threading
from pathlib import Path
import time
from uuid import uuid4

from .diagnostic_watchdog import HEADER, STATUS_SIZE

from tqdm import tqdm


class SearchDiagnostics:
    def __init__(self, directory: Path, interval=2.0, *, groups=None,
                 trace_limit=None, max_detailed_comparisons=1000):
        self.groups = set(groups or {"time", "cpu", "mem", "io"})
        self.trace_enabled = trace_limit is not None
        self.max_events = trace_limit if trace_limit is not None else 100000
        directory.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_id = f"{stamp}-{uuid4().hex[:8]}"
        self.paths = {phase: directory / f"{phase}-diagnostics-{run_id}.jsonl"
                      for phase in ("run", "hash", "efi", "search")}
        self.streams = {phase: path.open("x", encoding="utf-8") for phase, path in self.paths.items()}
        self.path = self.paths["run"]
        self.started = time.perf_counter()
        self.comparisons = self.file_comparisons = 0
        self.last_display = 0.0
        self.interval = max(.5, float(interval))
        self.max_detailed_comparisons = max_detailed_comparisons
        self.event_count = self.dropped_events = 0
        self.lock = threading.RLock()
        self.operations = []
        self.totals = {}
        self.closed = False
        self.worker = self.shared = self.status_file = None
        self.sequence = 0
        self.last_progress_at = time.monotonic()
        self.last_publish = 0.0
        self.resource_path = self.path.with_suffix('.resources.jsonl')
        self.status_path = self.path.with_suffix('.status')
        try:
            accelerator_version = importlib.metadata.version('hvdaccelerators')
        except importlib.metadata.PackageNotFoundError:
            accelerator_version = None
        self.emit("session_start", python_version=platform.python_version(), platform=platform.platform(),
                  accelerator_version=accelerator_version,
                  logical_cpus=os.cpu_count(), interval_seconds=self.interval,
                  groups=sorted(self.groups), trace_enabled=self.trace_enabled,
                  max_events=trace_limit, max_detailed_comparisons=max_detailed_comparisons)
        if self.groups & {"cpu", "mem", "io"}:
            try:
                self.status_file = self.status_path.open('x+b')
                self.status_file.truncate(STATUS_SIZE)
                self.shared = mmap.mmap(self.status_file.fileno(),STATUS_SIZE)
                self._publish(force=True)
                if getattr(sys, 'frozen', False):
                    command = [sys.executable, '--hvd-internal-diagnostic-watchdog']
                else:
                    command = [sys.executable, '-m', 'hydrusvideodeduplicator.diagnostic_watchdog']
                command.extend(['--pid', str(os.getpid()), '--status', str(self.status_path),
                                '--output', str(self.resource_path), '--interval', str(self.interval),
                                '--metrics', ','.join(sorted(self.groups & {"cpu", "mem", "io"}))])
                process_options = {
                    'stdin': subprocess.DEVNULL,
                    'stdout': subprocess.DEVNULL,
                    'stderr': subprocess.DEVNULL,
                }
                if os.name == 'nt':
                    process_options['creationflags'] = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
                self.worker = subprocess.Popen(command, **process_options)
            except OSError as error:
                self.emit('monitor_unavailable', error_type=type(error).__name__, reason='frozen_executable' if getattr(sys,'frozen',False) else 'watchdog_start_failed')
        tqdm.write("Diagnostic logs: " + ", ".join(f"{phase}={path}" for phase, path in self.paths.items()))

    def _phase_for(self, event, fields):
        operation = fields.get("operation")
        if event in ("session_start", "session_end", "monitor_unavailable"):
            return "run"
        if event == "frame_index_built":
            return "efi"
        if event.startswith("frame_index_query_"):
            return "search"
        if operation is not None:
            if operation in {"hydrus_file_selection", "hashing_and_queueing", "video_fetch_and_hash",
                             "video_download", "decode_and_perceptual_hash", "database_hash_queue_write"}:
                return "hash"
            if operation in {"search_tree_insertion", "tree_node_fetch", "tree_maintenance",
                             "hash_cache_fingerprint", "frame_index_add", "frame_index_seal"}:
                return "efi"
            if operation in {"duplicate_search", "video_search_and_reporting", "native_comparison",
                             "hydrus_relationship_submission", "frame_index_query", "exact_submission_commit",
                             "exact_query_commit", "hydrus_report"}:
                return "search"
        if self.operations:
            return self.operations[-1]["phase"]
        return "run"

    def _publish(self, force=False):
        now = time.monotonic()
        if self.shared is None or (not force and now-self.last_publish < .25): return
        active = self.operations[-1] if self.operations else None
        details = {}
        progress = {}
        for item in self.operations:
            details.update(item['fields'])
            progress.update(item['progress'])
        data = {'stage':active['name'] if active else 'between stages',
                'phase':active['phase'] if active else 'run',
                'stage_started':active['started'] if active else self.last_progress_at,
                'last_progress_at':self.last_progress_at,'closed':self.closed,
                'progress':progress,'details':details,'current_phase_has_counter':bool(active and active['progress'])}
        payload = json.dumps(data).encode('utf-8')
        if len(payload)>STATUS_SIZE-HEADER.size:
            data['details'] = {'truncated':True}
            payload = json.dumps(data).encode('utf-8')
        if len(payload)>STATUS_SIZE-HEADER.size:
            data['progress'] = {'truncated':True}
            payload = json.dumps(data).encode('utf-8')
        self.sequence = (self.sequence+2) % (2**32-2)
        self.shared[:HEADER.size] = HEADER.pack(self.sequence+1,len(payload))
        self.shared[HEADER.size:HEADER.size+len(payload)] = payload
        self.shared[:HEADER.size] = HEADER.pack(self.sequence,len(payload))
        self.last_publish = now

    def emit(self, event: str, **fields):
        with self.lock:
            if self.closed and event != 'session_end': return
            essential = event in ('session_start','session_end','operation_interrupted','monitor_unavailable')
            if not self.trace_enabled and not essential:
                return
            if self.event_count >= self.max_events and not essential:
                self.dropped_events += 1
                return
            self.event_count += 1
            phase = self._phase_for(event, fields)
            record = {'timestamp_utc':datetime.now(timezone.utc).isoformat(),
                      'elapsed_seconds':time.perf_counter()-self.started,'event':event,'phase':phase,**fields}
            stream = self.streams[phase]
            stream.write(json.dumps(record)+'\n')
            stream.flush()

    def progress(self, **fields):
        with self.lock:
            if self.closed or not self.operations: return
            current = self.operations[-1]['progress']
            if any(current.get(key) != value for key,value in fields.items()):
                current.update(fields)
                self.last_progress_at = time.monotonic()
            self._publish()

    @contextmanager
    def operation(self, name: str, _detail=True, **fields):
        started = time.perf_counter()
        item = {'name':name,'phase':self._phase_for('operation', {'operation':name}),
                'fields':fields,'started':time.monotonic(),'progress':{}}
        with self.lock:
            self.operations.append(item)
            self.last_progress_at = time.monotonic()
            self._publish(force=True)
        if _detail and self.trace_enabled: self.emit('operation_start',operation=name,**fields)
        outcome = 'completed'
        try:
            yield
        except BaseException as error:
            outcome = 'interrupted'
            self.emit('operation_interrupted',operation=name,duration_seconds=time.perf_counter()-started,
                      error_type=type(error).__name__,**fields)
            raise
        else:
            if _detail and self.trace_enabled: self.emit('operation_complete',operation=name,duration_seconds=time.perf_counter()-started,**fields)
        finally:
            with self.lock:
                duration = time.perf_counter()-started
                totals = self.totals.setdefault(name,{'calls':0,'inclusive_seconds':0.0,'interrupted':0})
                totals['calls'] += 1
                totals['inclusive_seconds'] += duration
                totals['interrupted'] += outcome == 'interrupted'
                self.operations.remove(item)
                self.last_progress_at = time.monotonic()
                self._publish(force=True)

    @contextmanager
    def video(self, file_hash: str, ordinal: int, total: int):
        self.file_comparisons = 0
        started = time.perf_counter()
        tqdm.write(f"Search {ordinal}/{total}: {file_hash}")
        try:
            with self.operation("video_search_and_reporting", file_hash=file_hash, ordinal=ordinal, total=total):
                yield
        finally:
            tqdm.write(f"  {self.file_comparisons} comparisons completed; {time.perf_counter() - started:.3f}s elapsed")

    def compare(self, calculate, query: bytes, candidate: bytes, node_id: int):
        fields = {
            "candidate_node_id": node_id,
            "query_frame_hashes": len(query) // 32,
            "candidate_frame_hashes": len(candidate) // 32,
        }
        now = time.perf_counter()
        if now - self.last_display >= 2.0:
            tqdm.write(
                f"  Comparing {fields['query_frame_hashes']} x {fields['candidate_frame_hashes']} "
                f"frame hashes; {self.file_comparisons} comparisons completed"
            )
            self.last_display = now
        with self.operation("native_comparison", _detail=self.trace_enabled and self.comparisons < self.max_detailed_comparisons, **fields):
            result = calculate(query, candidate)
        self.file_comparisons += 1
        self.comparisons += 1
        self.progress(comparisons_completed=self.file_comparisons)
        if self.trace_enabled and self.comparisons <= self.max_detailed_comparisons:
            self.emit("comparison_result", distance=result, comparisons_completed=self.comparisons, **fields)
        return result

    def close(self):
        if self.closed: return
        self.closed = True
        self._publish(force=True)
        if self.worker is not None:
            try:
                self.worker.wait(timeout=self.interval+3)
                if self.worker.returncode:
                    self.closed = False
                    self.emit('monitor_unavailable', error_type='WatchdogExit')
                    self.closed = True
            except subprocess.TimeoutExpired:
                self.worker.terminate()
                self.worker.wait(timeout=3)
        if self.shared is not None: self.shared.close()
        if self.status_file is not None: self.status_file.close()
        self.status_path.unlink(missing_ok=True)
        self.emit('session_end',comparisons_completed=self.comparisons,dropped_detail_events=self.dropped_events)
        for stream in self.streams.values():
            stream.close()
        summary = {'diagnostic_groups':sorted(self.groups),'trace_enabled':self.trace_enabled,
                   'trace_event_cap':self.max_events if self.trace_enabled else None,
                   'phase_logs':{phase:str(path) for phase,path in self.paths.items()},
                   'resource_file':str(self.resource_path) if self.worker is not None else None,
                   'watchdog_exit_code':self.worker.returncode if self.worker is not None else None}
        if 'time' in self.groups:
            summary.update(elapsed_seconds=time.perf_counter()-self.started,
                           stage_totals=self.totals,comparisons=self.comparisons,
                           dropped_detail_events=self.dropped_events,
                           comparison_counter_scope='Legacy video scoring calls; frame-index work has separate query counters.',
                           timing_note='Nested stage times overlap; do not sum inclusive durations.')
        self.path.with_suffix('.summary.json').write_text(json.dumps(summary,indent=2)+'\n',encoding='utf-8')
