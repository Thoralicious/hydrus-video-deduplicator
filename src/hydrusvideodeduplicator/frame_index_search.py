"""Exact cached-video search with optional Hydrus submissions."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError
from contextlib import closing, nullcontext
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import time
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

from hydrusvideodeduplicator import _native as vpdq
from tqdm import tqdm

from .__about__ import __version__
from .db_lock import DatabaseBusyError, database_lock
from .frame_index_state import ALGORITHM, FrameSearchState, FrameStateError, state_path_for


class FrameIndexError(RuntimeError):
    pass


LOCAL_TYPES = {2, 14, 15, 20, 21}


def file_hash_text(value):
    if isinstance(value, bytes):
        value = value.hex()
    if not isinstance(value, str) or len(value) != 64:
        raise FrameIndexError('Invalid cached SHA-256 identifier.')
    try:
        bytes.fromhex(value)
    except ValueError as error:
        raise FrameIndexError('Invalid cached SHA-256 identifier.') from error
    return value.lower()


def service_context(api, api_url):
    services = api.get_services().get('services')
    if not isinstance(services, dict) or not services:
        raise FrameIndexError('Missing Hydrus service information.')
    local_keys = {key for key, value in services.items() if value.get('type') in LOCAL_TYPES}
    domain_keys = sorted(key for key, value in services.items() if value.get('type') == 2)
    if not local_keys or not domain_keys:
        raise FrameIndexError('No Hydrus local file domains identified.')
    parsed = urlsplit(api_url)
    host = parsed.hostname or ''
    if ':' in host:
        host = f'[{host}]'
    endpoint = urlunsplit((parsed.scheme.lower(), host + (f':{parsed.port}' if parsed.port else ''),
                           parsed.path.rstrip('/'), '', ''))
    return {'endpoint': endpoint, 'local_domain_keys': domain_keys}


def similarity_cutoff(value: float) -> int:
    # Preserve HVD's existing 101-int(similarity) distance conversion.
    if not math.isfinite(value) or not 1 <= value <= 100:
        raise FrameIndexError('Frame-index similarity must be between 1 and 100.')
    return int(value)


def validate_frame_index_workers(value):
    if value == 0:
        raise FrameIndexError('EFI workers must be positive or negative (all but N logical CPUs).')
    count = max(1, (os.cpu_count() or 1) - abs(value)) if value < 0 else value
    if count > 256:
        raise FrameIndexError('Resolved EFI worker count must not exceed 256.')
    if not hasattr(vpdq, 'ExactFrameIndex'):
        raise FrameIndexError('The bundled native extension is missing ExactFrameIndex; rebuild or reinstall HVD.')
    if count > 1 and not hasattr(vpdq.ExactFrameIndex, 'parallel_stats'):
        raise FrameIndexError('The bundled native extension lacks parallel EFI; rebuild or reinstall HVD.')
    return count


def validate_progress_batch_size(value):
    if not isinstance(value, int) or value < 1 or value > 4096:
        raise FrameIndexError('EFI progress batch size must be between 1 and 4096 collections.')
    return value


def validate_batch_interval_seconds(value):
    if value is not None and (not math.isfinite(value) or value <= 0):
        raise FrameIndexError('EFI batch interval must be a positive number of seconds.')
    return value

class HashProgress:
    """Work-weighted estimate scoped to this invocation's pending queries."""
    def __init__(self, total):
        self.total = total
        self.completed = 0
        self.observed = 0
        self.search_seconds = 0.0
        self.rate = None

    def finish(self, count, seconds):
        self.completed += count
        self.search_seconds += seconds
        if count and seconds > 0:
            sample = seconds/count
            self.rate = sample if self.rate is None else .2*sample+.8*self.rate

    def snapshot(self, active_done=0):
        done = min(self.total, self.completed+active_done)
        self.observed = max(self.observed, done)
        eta = (self.total-done)*self.rate if self.rate is not None else None
        return {'total_query_frame_hashes': self.total,
                'completed_query_frame_hashes': self.completed,
                'observed_query_frame_hashes': self.observed,
                'estimated_search_seconds_remaining': eta,
                'eta_note': 'Approximate smoothed search time; excludes submission/commit time. Interrupted query is retried.'}

def inventory(connection):
    """Small metadata snapshot; packed hashes are streamed separately."""
    aliases = {}
    files = {}
    lengths = {}
    for key, length in connection.execute('SELECT phash_id,length(phash) FROM shape_perceptual_hashes'):
        if not isinstance(key, int) or key < 0 or not isinstance(length, int) or length % 32:
            raise FrameIndexError('Invalid cached collection identifier or packed hash length.')
        lengths[('stored', key)] = length
    for value, key in connection.execute('''SELECT f.file_hash,m.phash_id FROM files f
            JOIN shape_perceptual_hash_map m ON m.hash_id=f.hash_id ORDER BY f.hash_id'''):
        file_hash = file_hash_text(value)
        group = ('stored', key)
        if group not in lengths:
            raise FrameIndexError('File references a missing hash collection.')
        if file_hash in files:
            raise FrameIndexError('A file has multiple cached hash associations.')
        files[file_hash] = group
        aliases.setdefault(group, set()).add(file_hash)
    queue_keys = {}
    queued_files = 0
    # A queued replacement takes precedence over that file's old stored hashes.
    for value, stored_id, length in connection.execute('''SELECT q.file_hash,p.phash_id,length(q.phash)
            FROM phashed_file_queue q LEFT JOIN shape_perceptual_hashes p ON p.phash=q.phash
            ORDER BY q.file_hash'''):
        file_hash = file_hash_text(value)
        if file_hash in queue_keys:
            raise FrameIndexError('A file has multiple queued hash collections.')
        if not isinstance(length, int) or length % 32:
            raise FrameIndexError('Invalid queued packed hash length.')
        group = ('stored', stored_id) if stored_id is not None else ('queued', file_hash)
        old = files.get(file_hash)
        if old is not None:
            aliases[old].discard(file_hash)
        files[file_hash] = group
        aliases.setdefault(group, set()).add(file_hash)
        lengths[group] = length
        queue_keys[file_hash] = value
        queued_files += 1
    aliases = {group: sorted(values) for group, values in aliases.items() if values}
    return aliases, files, lengths, queue_keys, queued_files


def packed_hashes(connection, group, queue_keys):
    kind, key = group
    if kind == 'stored':
        row = connection.execute('SELECT phash FROM shape_perceptual_hashes WHERE phash_id=?', (key,)).fetchone()
    else:
        row = connection.execute('SELECT phash FROM phashed_file_queue WHERE file_hash=?', (queue_keys[key],)).fetchone()
    if row is None or not isinstance(row[0], bytes) or len(row[0]) % 32:
        raise FrameIndexError('Missing or malformed packed frame hashes.')
    return row[0]


def search_interruptibly(index, query, cutoff, executor, diagnostics=None, *, workers=1,
                         should_skip=None, hash_progress=None, hash_bar=None):
    future = executor.submit(index.search, query, cutoff, workers) if workers > 1 else executor.submit(index.search, query, cutoff)
    started = last_display = time.perf_counter()
    try:
        while True:
            try:
                return future.result(timeout=.1)
            except TimeoutError:
                if should_skip is not None and should_skip(): raise KeyboardInterrupt
                active,total,done,checks,visits = index.progress()
                if active and hash_progress is not None:
                    snapshot = hash_progress.snapshot(done)
                    if hash_bar is not None:
                        hash_bar.update(max(0, snapshot['observed_query_frame_hashes']-hash_bar.n))
                    if diagnostics is not None: diagnostics.progress(**snapshot)
                if diagnostics is not None:
                    active,total,done,checks,visits = index.progress()
                    if active: diagnostics.progress(query_hashes_completed=done,total_query_hashes=total,hamming_checks=checks,posting_visits=visits)
                now = time.perf_counter()
                if now-last_display >= 2:
                    active, total, done, checks, visits = index.progress()
                    if active:
                        parallel = index.parallel_stats()[0] if hasattr(index, 'parallel_stats') else 1
                        eta = hash_progress.snapshot(done)['estimated_search_seconds_remaining'] if hash_progress is not None else None
                        remaining = f'; approximate search remaining {eta/3600:.2f} h' if eta is not None else ''
                        tqdm.write(f'  Index search: {done:,}/{total:,} query hashes; {parallel} worker(s); {now-started:.1f}s elapsed{remaining}')
                    last_display = now
    except BaseException:
        # Cancellation may arrive between submission and native activation.
        while not future.done():
            try:
                index.cancel()
                future.result(timeout=.1)
            except (TimeoutError, KeyboardInterrupt):
                continue
            except Exception:
                break
        raise


def record_pair_result(pair_store, matches, query_hash, candidate_hash, score):
    direction = pair_store.execute(
        'INSERT OR IGNORE INTO directions VALUES (?,?,?)',
        (query_hash, candidate_hash, float(score)),
    )
    if direction.rowcount:
        matches.write(json.dumps({'query_sha256': query_hash, 'candidate_sha256': candidate_hash,
                                  'directional_score': score}) + '\n')
    left, right = sorted((query_hash, candidate_hash))
    pair = pair_store.execute('INSERT OR IGNORE INTO pairs VALUES (?,?)', (left, right))
    return bool(direction.rowcount), bool(pair.rowcount)


def _run_frame_index(database: Path, *, threshold=50.0, query_hashes=None,
                     diagnostics=None, submit_pair=None, context=None,
                     acquire_lock=True, should_skip=None, progress=None, workers=1,
                     output_directory=None, record_pair=None, submission_enabled=True,
                     persist_progress=False, progress_batch_size=32,
                     batch_interval_seconds=1.0):
    cutoff = similarity_cutoff(threshold)
    track_state = submit_pair is not None and (submission_enabled or persist_progress)
    if track_state and not context:
        raise FrameIndexError('Live exact search requires a Hydrus client context.')
    state = None
    hash_progress = None
    index = None
    pair_store = None
    pair_store_path = None
    progress_batch = []
    progress_batch_started = None
    if not hasattr(vpdq, 'ExactFrameIndex'):
        raise FrameIndexError('The bundled native extension is missing ExactFrameIndex; rebuild or reinstall HVD.')
    workers = validate_frame_index_workers(workers)
    progress_batch_size = validate_progress_batch_size(progress_batch_size)
    batch_interval_seconds = validate_batch_interval_seconds(batch_interval_seconds)
    selected = {file_hash_text(value) for value in query_hashes} if query_hashes else None
    database = Path(database).resolve(strict=True)
    started = time.perf_counter()
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    output_directory = Path(output_directory) if output_directory is not None else database.parent / 'logs'
    directory = output_directory / f'efi-search-{stamp}-{uuid4().hex[:8]}'
    report = {'status': 'running', 'complete': False,
              'mode': 'submit' if submit_pair is not None and submission_enabled else 'no_submit',
              'algorithm': ALGORITHM, 'database': str(database), 'workers': workers,
              'requested_threshold': threshold, 'effective_integer_threshold': cutoff,
              'progress_batch_size': progress_batch_size,
              'batch_interval_seconds': batch_interval_seconds,
              'progress_batches_committed': 0,
              'progress_query_collections_committed': 0,
              'unique_pairs': 0, 'submitted_pairs': 0, 'ledger_skips': 0, 'completed_query_collections': 0,
              'coverage': 'All cached files, including queued hashes. Old VP-tree progress is not used.',
              'scope': 'Cached perceptual hashes; no Hydrus tag-query or current-file-presence filtering.'}
    directory.mkdir(parents=True, exist_ok=False)

    def commit_progress_batch():
        nonlocal progress_batch_started
        if not progress_batch:
            return
        batch_count = len(progress_batch)
        batch_file_count = sum(map(len, progress_batch))
        with diagnostics.operation('exact_query_commit', collections=batch_count,
                                   files=batch_file_count) if diagnostics is not None else nullcontext():
            state.finish_queries(context, progress_batch, digests, cutoff)
        progress_batch.clear()
        progress_batch_started = time.perf_counter()
        report['progress_batches_committed'] += 1
        report['progress_query_collections_committed'] += batch_count
        if diagnostics is not None:
            diagnostics.emit('frame_index_progress_checkpoint',
                             collections=batch_count,
                             total_collections_committed=report['progress_query_collections_committed'])

    try:
        with (database_lock(database) if acquire_lock else nullcontext()), closing(sqlite3.connect(database.as_uri()+'?mode=ro', uri=True)) as connection:
            connection.execute('PRAGMA query_only=ON')
            connection.execute('BEGIN')
            versions = connection.execute('SELECT version FROM version').fetchall()
            if versions != [(__version__,)]:
                raise FrameIndexError('Database version differs; use an explicitly upgraded test copy.')
            aliases, files, lengths, queue_keys, queued_count = inventory(connection)
            if selected is not None and not selected <= files.keys():
                raise FrameIndexError('One or more requested query SHA-256 hashes are absent from the cache.')
            groups = list(aliases)
            query_groups = [group for group in groups if selected is None or selected.intersection(aliases[group])]
            report.update(cached_files=len(files), indexed_collections=len(groups), queued_files=queued_count,
                          indexed_frame_hashes=sum(lengths[group]//32 for group in groups),
                          total_query_collections=len(query_groups),
                          selected_query_sha256=sorted(selected) if selected is not None else None)
            digests = {}
            pending_files = None
            pending_to_replay = []
            if track_state:
                # Bind coverage to file membership and exact packed bytes, not SQL IDs or tree layout.
                fingerprint_started = time.perf_counter()
                with diagnostics.operation('hash_cache_fingerprint',total_collections=len(groups)) if diagnostics is not None else nullcontext():
                    for fingerprint_count,group in enumerate(tqdm(groups, desc='Checking exact-search progress', unit='collection'),1):
                        if should_skip is not None and should_skip(): raise KeyboardInterrupt
                        value = hashlib.sha256(packed_hashes(connection, group, queue_keys)).digest()
                        for file_hash in aliases[group]: digests[file_hash] = value
                        if diagnostics is not None: diagnostics.progress(collections_fingerprinted=fingerprint_count)
                report['fingerprint_seconds'] = time.perf_counter()-fingerprint_started
                state = FrameSearchState(state_path_for(database))
                report['coverage_invalidated'] = state.prepare(digests)
                pending_to_replay = state.pending_pairs(context, cutoff)
                if submission_enabled:
                    for hash_a, hash_b, score_a_to_b, score_b_to_a in pending_to_replay:
                        if should_skip is not None and should_skip():
                            raise KeyboardInterrupt
                        if state.already_submitted(context, hash_a, hash_b, digests):
                            state.acknowledge(context, hash_a, hash_b, digests)
                            continue
                        with diagnostics.operation('hydrus_report', file_a=hash_a, file_b=hash_b) if diagnostics is not None else nullcontext():
                            submit_pair(hash_a, hash_b)
                        with diagnostics.operation('exact_submission_commit') if diagnostics is not None else nullcontext():
                            state.acknowledge(context, hash_a, hash_b, digests)
                        report['submitted_pairs'] += 1
                else:
                    report['pending_pairs'] = len(pending_to_replay)
                completed = state.completed_at(context, cutoff)
                requested = set(files) if selected is None else selected
                pending_files = requested-completed
                query_groups = [group for group in query_groups if pending_files.intersection(aliases[group])]
                report['previously_completed_query_files'] = len(requested & completed)
                report['pending_query_files'] = len(pending_files)
                report['total_query_collections'] = len(query_groups)
                report['state_database'] = str(state_path_for(database))
            hash_progress = HashProgress(sum(lengths[group]//32 for group in query_groups))
            report.update(hash_progress.snapshot())
            tqdm.write(f'Exact frame-index search: up to {workers} worker(s); pending query hashes: {hash_progress.total:,}')
            index = vpdq.ExactFrameIndex() if query_groups else None
            build_started = time.perf_counter()
            report['index_add_seconds'] = report['index_seal_seconds'] = 0.0
            if index is not None:
                add_started = time.perf_counter()
                with diagnostics.operation('frame_index_add',total_collections=len(groups)) if diagnostics is not None else nullcontext():
                    for native_id, group in enumerate(tqdm(groups, desc='Building frame index', unit='collection')):
                        if should_skip is not None and should_skip(): raise KeyboardInterrupt
                        index.add(native_id, packed_hashes(connection, group, queue_keys))
                        if diagnostics is not None: diagnostics.progress(collections_added=native_id+1)
                report['index_add_seconds'] = time.perf_counter()-add_started
                tqdm.write('Sealing frame index...')
                seal_started = time.perf_counter()
                with diagnostics.operation('frame_index_seal') if diagnostics is not None else nullcontext():
                    index.seal()
                report['index_seal_seconds'] = time.perf_counter()-seal_started
            report['build_seconds'] = time.perf_counter()-build_started
            report['native_stats'] = list(index.stats()) if index is not None else None
            if diagnostics is not None:
                diagnostics.emit('frame_index_built', **{key: report[key] for key in
                                 ('cached_files','indexed_collections','indexed_frame_hashes','build_seconds')})
            pair_store_path = directory / '.pair-store.sqlite'
            pair_store = sqlite3.connect(pair_store_path)
            pair_store.executescript('''
                CREATE TABLE pairs(hash_a TEXT,hash_b TEXT,PRIMARY KEY(hash_a,hash_b));
                CREATE TABLE directions(query_hash TEXT,candidate_hash TEXT,score REAL,
                                         PRIMARY KEY(query_hash,candidate_hash));
            ''')
            with (directory/'query-events.jsonl').open('x', encoding='utf-8') as query_events, \
                    (directory/'matches.jsonl').open('x', encoding='utf-8') as matches, \
                    (directory/'queries.jsonl').open('x', encoding='utf-8') as queries, \
                    ThreadPoolExecutor(max_workers=1) as executor, \
                    tqdm(total=hash_progress.total, desc='EFI query hashes', unit='hash') as hash_bar:
                for hash_a, hash_b, score_a_to_b, score_b_to_a in pending_to_replay:
                    if record_pair is not None:
                        if score_a_to_b is not None:
                            record_pair(hash_a, hash_b, score_a_to_b)
                        if score_b_to_a is not None:
                            record_pair(hash_b, hash_a, score_b_to_a)
                    if score_a_to_b is not None:
                        record_pair_result(pair_store, matches, hash_a, hash_b, score_a_to_b)
                    if score_b_to_a is not None:
                        record_pair_result(pair_store, matches, hash_b, hash_a, score_b_to_a)
                report['unique_pairs'] = pair_store.execute('SELECT COUNT(*) FROM pairs').fetchone()[0]
                if state is not None:
                    progress_batch_started = time.perf_counter()
                for ordinal, group in enumerate(tqdm(query_groups, desc='Searching frame index', unit='collection'), 1):
                    if should_skip is not None and should_skip(): raise KeyboardInterrupt
                    if progress is not None: progress(ordinal-1,len(query_groups))
                    query = packed_hashes(connection, group, queue_keys)
                    query_files = [value for value in aliases[group]
                                   if (selected is None or value in selected)
                                   and (pending_files is None or value in pending_files)]
                    report['active_query'] = {'ordinal':ordinal,'query_sha256':query_files,'query_frame_hashes':len(query)//32}
                    query_events.write(json.dumps({'event':'query_start','timestamp_utc':datetime.now(timezone.utc).isoformat(),**report['active_query']})+'\n'); query_events.flush()
                    lookup_started = time.perf_counter()
                    with diagnostics.operation('frame_index_query',**report['active_query']) if diagnostics is not None else nullcontext():
                        result = search_interruptibly(index, query, cutoff, executor, diagnostics=diagnostics,
                                                       workers=workers, should_skip=should_skip,
                                                       hash_progress=hash_progress, hash_bar=hash_bar)
                    index_seconds = time.perf_counter()-lookup_started
                    _, total, done, checks, visits = index.progress()
                    query_record = {'workers': workers, 'hamming_checks': checks, 'posting_visits': visits, 'ordinal': ordinal, 'query_sha256': query_files, 'query_frame_hashes': len(query)//32,
                                    'index_seconds': index_seconds, 'returned_collections': len(result)}
                    for native_id, score in result:
                        if native_id < 0 or native_id >= len(groups) or not cutoff <= score <= 100:
                            raise FrameIndexError('Native index returned an invalid collection or score.')
                        for file_a in query_files:
                            for file_b in aliases[groups[native_id]]:
                                if file_a == file_b:
                                    continue
                                if should_skip is not None and should_skip(): raise KeyboardInterrupt
                                new_direction, new_pair = record_pair_result(pair_store, matches, file_a, file_b, score)
                                if new_direction and record_pair is not None:
                                    record_pair(file_a, file_b, score)
                                if state is not None and not submission_enabled and not state.already_submitted(context, file_a, file_b, digests):
                                    state.record_pending(context, file_a, file_b, digests, score)
                                if not new_pair:
                                    continue
                                report['unique_pairs'] += 1
                                if state is not None:
                                    if state.already_submitted(context, file_a, file_b, digests):
                                        report['ledger_skips'] += 1
                                        continue
                                    if submission_enabled:
                                        scope = diagnostics.operation('hydrus_report', file_a=file_a, file_b=file_b) if diagnostics is not None else nullcontext()
                                        with scope:
                                            submit_pair(file_a, file_b)
                                        # Only acknowledged API calls enter the persistent ledger.
                                        with diagnostics.operation('exact_submission_commit') if diagnostics is not None else nullcontext():
                                            state.acknowledge(context, file_a, file_b, digests)
                                        report['submitted_pairs'] += 1
                                    else:
                                        report['pending_pairs'] = report.get('pending_pairs', 0) + 1

                    matches.flush()
                    pair_store.commit()
                    query_record['total_seconds'] = time.perf_counter()-lookup_started
                    queries.write(json.dumps(query_record)+'\n')
                    queries.flush()
                    query_events.write(json.dumps({'event':'query_complete','timestamp_utc':datetime.now(timezone.utc).isoformat(),**query_record})+'\n'); query_events.flush()
                    report['active_query'] = None
                    report['completed_query_collections'] += 1
                    if state is not None:
                        progress_batch.append(query_files)
                        interval_due = (batch_interval_seconds is not None
                                        and time.perf_counter()-progress_batch_started >= batch_interval_seconds)
                        if len(progress_batch) >= progress_batch_size or interval_due:
                            commit_progress_batch()
                    hash_progress.finish(len(query)//32, index_seconds)
                    snapshot = hash_progress.snapshot()
                    hash_bar.update(max(0, snapshot['observed_query_frame_hashes']-hash_bar.n))
                    report.update(snapshot)
                    if progress is not None: progress(ordinal,len(query_groups))
                    if diagnostics is not None:
                        diagnostics.emit('frame_index_query_complete', **query_record)
                else:
                    if state is not None:
                        commit_progress_batch()
                    report['status'] = 'complete'
                    report['complete'] = True
    except KeyboardInterrupt:
        report['status'] = 'interrupted'
    except Exception as error:
        report['status'] = 'failed'
        # Avoid serializing arbitrary SQL/database/error contents into the report.
        report['error_type'] = type(error).__name__
        if isinstance(error, (FrameIndexError, FrameStateError, DatabaseBusyError)):
            report['error'] = str(error)
    finally:
        if state is not None: state.close()
        report['progress_query_collections_uncheckpointed'] = len(progress_batch)
        if pair_store is not None:
            pair_store.close()
            pair_store_path.unlink(missing_ok=True)
        if hash_progress is not None: report.update(hash_progress.snapshot())
        if index is not None and report.get('native_stats') is not None and hasattr(index, 'storage_stats'):
            report['final_storage'] = index.storage_stats()
        if report.get('active_query') is not None:
            with (directory/'query-events.jsonl').open('a',encoding='utf-8') as query_events:
                query_events.write(json.dumps({'event':'query_'+report['status'],'timestamp_utc':datetime.now(timezone.utc).isoformat(),**report['active_query']})+'\n')
        report['elapsed_seconds'] = time.perf_counter()-started
        report['report_directory'] = str(directory)
        (directory/'summary.json').write_text(json.dumps(report, indent=2)+'\n', encoding='utf-8')
    tqdm.write(f"EFI run summary: {directory/'summary.json'}")
    tqdm.write(f"{report['unique_pairs']:,} unique candidate pairs; status: {report['status']}")
    return report


def run_frame_index_search(database: Path, submit_pair, *, context, threshold=50.0,
                           query_hashes=None, diagnostics=None,
                           should_skip=None, progress=None, workers=1,
                           output_directory=None, record_pair=None, submission_enabled=True,
                           persist_progress=False, acquire_lock=True, progress_batch_size=32,
                           batch_interval_seconds=1.0):
    """Search the complete cached frame index and optionally submit its unique pairs."""
    return _run_frame_index(database, threshold=threshold, query_hashes=query_hashes,
                            diagnostics=diagnostics, submit_pair=submit_pair,
                            context=context, acquire_lock=acquire_lock, should_skip=should_skip, progress=progress,
                            workers=workers, output_directory=output_directory, record_pair=record_pair,
                            submission_enabled=submission_enabled, persist_progress=persist_progress,
                            progress_batch_size=progress_batch_size,
                            batch_interval_seconds=batch_interval_seconds)
