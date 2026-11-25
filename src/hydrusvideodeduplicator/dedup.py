from __future__ import annotations

import gc
import hashlib
import json
import logging
import math
import os
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, closing, nullcontext
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from requests import RequestException
from urllib3.exceptions import HTTPError

from rich import print
from tqdm import tqdm

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from typing import TypeAlias

    FileHash = str

import hydrusvideodeduplicator.hydrus_api as hydrus_api

from .client import HVDClient
from .db import DedupeDB, vptree
from .db_lock import DatabaseBusyError, database_lock
from .frame_index_search import FrameIndexError, run_frame_index_search, service_context
from .hashing import compute_phash
from .page_logger import HydrusPageLogger
from hvdaccelerators import vpdq


@dataclass
class PerceptuallyHashedFile:
    """Class for perceptually hashed files."""

    file_hash: FileHash
    perceptual_hash: bytes


@dataclass
class FailedPerceptuallyHashedFile:
    """Class for failed perceptually hashed files."""

    file_hash: FileHash
    exc: Exception


class HydrusApiException(Exception):
    """Wrapper around hydrus_api.HydrusAPIException to avoid some coupling with hydrus_api outside of FileHasher."""

    def __init__(self, cause: Exception, *, transport_failure: bool = False):
        super().__init__(str(cause))
        self.cause = cause
        self.transport_failure = transport_failure


class HashingCircuitBreakers:
    """Per-run guards for repeatedly failing local-path and HTTP video access."""

    FAILURE_LIMIT = 3

    def __init__(self):
        self._lock = Lock()
        self._local_path_failures = 0
        self._local_path_disabled = False
        self._http_failures = 0
        self._http_open = False

    @property
    def local_path_enabled(self) -> bool:
        with self._lock:
            return not self._local_path_disabled

    @property
    def http_open(self) -> bool:
        with self._lock:
            return self._http_open

    def local_path_succeeded(self) -> None:
        with self._lock:
            self._local_path_failures = 0

    def local_path_failed(self) -> bool:
        """Return True only when this failure disables local-path attempts."""
        with self._lock:
            if self._local_path_disabled:
                return False
            self._local_path_failures += 1
            if self._local_path_failures >= self.FAILURE_LIMIT:
                self._local_path_disabled = True
                return True
            return False

    def http_succeeded(self) -> None:
        with self._lock:
            self._http_failures = 0

    def http_failed(self) -> bool:
        """Return True only when this failure opens the HTTP breaker."""
        with self._lock:
            if self._http_open:
                return False
            self._http_failures += 1
            if self._http_failures >= self.FAILURE_LIMIT:
                self._http_open = True
                return True
            return False


def _is_hydrus_transport_failure(error: Exception) -> bool:
    return isinstance(error, (hydrus_api.ConnectionError, RequestException, HTTPError))


class FailedPerceptualHashException(Exception):
    """Exception for when files are failed to be perceptually hashed."""

    def __init__(self, other_exc: str = ""):
        super().__init__(other_exc)
        self.other_exc = other_exc


class FileHasher:
    """
    A class to fetch a file from Hydrus and phash it.
    """

    def __init__(
        self,
        client: HVDClient,
        num_threads: int = 0,
        diagnostics=None,
        *,
        api=None,
        circuit_breakers: HashingCircuitBreakers | None = None,
    ):
        self.client = client
        self.api = client.client if api is None else api
        self.num_threads = num_threads
        self.diagnostics = diagnostics
        self.circuit_breakers = circuit_breakers or HashingCircuitBreakers()

    def _record_local_path_failure(self) -> None:
        if self.circuit_breakers.local_path_failed():
            tqdm.write("Local-path access disabled for this run after three consecutive failures; using HTTP fallback.")

    def _local_file_path(self, file_hash: str) -> Path | None:
        if not self.circuit_breakers.local_path_enabled:
            return None

        api_url = getattr(self.api, "api_url", None)
        try:
            hostname = urlsplit(api_url).hostname if isinstance(api_url, str) else None
        except ValueError:
            hostname = None
        if hostname not in {"localhost", "127.0.0.1", "::1"}:
            return None

        try:
            with self.diagnostics.operation("local_path_lookup", file_hash=file_hash) if self.diagnostics else nullcontext():
                result = self.api.get_file_path(hash_=file_hash)
            path = Path(result["path"])
        except (hydrus_api.HydrusAPIException, RequestException, AttributeError, KeyError, TypeError, ValueError) as exc:
            if _is_hydrus_transport_failure(exc):
                self._record_local_path_failure()
            return None
        if not path.is_absolute():
            return None
        if path.drive.startswith("\\\\"):
            trusted_share = os.environ.get("HVD_TRUSTED_UNC_SHARE", "").rstrip("\\/")
            if not trusted_share or path.drive.casefold() != trusted_share.casefold():
                return None

        try:
            grace_seconds = float(os.environ.get("HVD_NAS_GRACE_SECONDS", "2"))
            if not math.isfinite(grace_seconds) or grace_seconds < 0:
                raise ValueError
        except ValueError:
            grace_seconds = 2.0

        deadline = time.monotonic() + grace_seconds
        while True:
            try:
                with path.open("rb"):
                    pass
                self.circuit_breakers.local_path_succeeded()
                return path
            except OSError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._record_local_path_failure()
                    return None
                time.sleep(min(0.1, remaining))

    def _fetch_file(self, file_hash: str, *, timeout=None) -> bytes:
        if self.circuit_breakers.http_open:
            error = RuntimeError("HTTP hashing circuit is open after repeated Hydrus connection failures.")
            raise HydrusApiException(error, transport_failure=True)
        try:
            with self.diagnostics.operation("video_download", file_hash=file_hash) if self.diagnostics else nullcontext():
                response = (self.api.get_file(hash_=file_hash) if timeout is None
                            else self.api.get_file(hash_=file_hash, timeout=timeout))
                with closing(response):
                    content = response.content
            self.circuit_breakers.http_succeeded()
            if self.diagnostics:
                self.diagnostics.progress(downloaded_bytes=len(content))
                self.diagnostics.emit("video_download_complete", file_hash=file_hash, downloaded_bytes=len(content))
        except (hydrus_api.HydrusAPIException, RequestException, HTTPError) as exc:
            transport_failure = _is_hydrus_transport_failure(exc)
            if transport_failure:
                self.circuit_breakers.http_failed()
            else:
                self.circuit_breakers.http_succeeded()
            raise HydrusApiException(exc, transport_failure=transport_failure) from exc
        return content

    def _phash_file(self, file: bytes | Path) -> bytes:
        try:
            with self.diagnostics.operation("decode_and_perceptual_hash") if self.diagnostics else nullcontext():
                phash = compute_phash(file, self.num_threads)
            phash_bytes: bytes = phash.bytes
        except OSError as exc:
            if isinstance(file, Path):
                raise
            raise FailedPerceptualHashException(str(exc)) from exc
        except Exception as exc:
            raise FailedPerceptualHashException(str(exc))

        # sanity check
        # Note: Hashes may have 0 bytes if there was no frames that were high enough quality to be used.
        if phash_bytes is None or (len(phash_bytes) % vpdq.VpdqHash.bytesPerPdqHash != 0):
            raise FailedPerceptualHashException("phash_str was None or len not multiple of 32.")

        return phash_bytes

    def prepare_buffer(self, file_hash: str, size: int) -> tuple[bytes, bool] | None:
        """Fetch a bounded input buffer, preferring the local Hydrus file path."""
        if size <= 0:
            return None
        path = self._local_file_path(file_hash)
        if path is not None:
            try:
                with path.open("rb") as source:
                    if os.fstat(source.fileno()).st_size == size:
                        content = source.read(size + 1)
                        if len(content) == size:
                            return content, True
            except OSError:
                self._record_local_path_failure()
        if self.circuit_breakers.http_open:
            error = RuntimeError("HTTP hashing circuit is open after repeated Hydrus connection failures.")
            return FailedPerceptuallyHashedFile(file_hash, HydrusApiException(error, transport_failure=True))
        try:
            with closing(self.api.get_file(hash_=file_hash, timeout=(3, 30))) as response:
                content = response.raw.read(size + 1, decode_content=True)
                if len(content) == size:
                    self.circuit_breakers.http_succeeded()
                    return content, False
        except (hydrus_api.HydrusAPIException, RequestException, HTTPError) as exc:
            if _is_hydrus_transport_failure(exc):
                self.circuit_breakers.http_failed()
                return FailedPerceptuallyHashedFile(file_hash, HydrusApiException(exc, transport_failure=True))
            self.circuit_breakers.http_succeeded()
        except (OSError, TypeError):
            pass
        return None

    def hash_buffer(self, file_hash: str, prepared: tuple[bytes, bool] | None):
        if prepared is None:
            return self.fetch_and_phash_file(file_hash)
        content, local = prepared
        try:
            phash = self._phash_file(content)
        except FailedPerceptualHashException as exc:
            return FailedPerceptuallyHashedFile(file_hash, exc)
        if self.diagnostics:
            self.diagnostics.emit("video_buffer_used", file_hash=file_hash,
                                  delivery="local_path" if local else "http")
        return PerceptuallyHashedFile(file_hash, phash)

    def fetch_and_phash_file(self, file_hash: str) -> PerceptuallyHashedFile | FailedPerceptuallyHashedFile:
        """
        Retrieves the file from Hydrus and calculates its perceptual hash and returns the result.

        Returns FailedPerceptuallyHashedFile with the failed video hash if there are any errors.
        """
        local_path = self._local_file_path(file_hash)
        if local_path is not None:
            try:
                phash = self._phash_file(local_path)
            except OSError:
                self._record_local_path_failure()
                if self.diagnostics:
                    self.diagnostics.emit("video_local_path_fallback", file_hash=file_hash)
            except FailedPerceptualHashException:
                if self.diagnostics:
                    self.diagnostics.emit("video_local_path_fallback", file_hash=file_hash)
            else:
                if self.diagnostics:
                    self.diagnostics.emit("video_local_path_used", file_hash=file_hash)
                return PerceptuallyHashedFile(file_hash, phash)

        return self._fetch_and_phash_http(file_hash)

    def _fetch_and_phash_http(self, file_hash: str):
        try:
            file = self._fetch_file(file_hash)
        except HydrusApiException as exc:
            # Add a delay before turning so that if there is some transient issue
            # the next file to be hashed won't also immediately error.
            # This is a hack. There should probably be some counter in the result generator
            # for the number of failures before hashing is stopped entirely.
            if exc.transport_failure and not self.circuit_breakers.http_open:
                time.sleep(3)
            return FailedPerceptuallyHashedFile(file_hash, exc)

        try:
            phash = self._phash_file(file)
        except FailedPerceptualHashException as exc:
            return FailedPerceptuallyHashedFile(file_hash, exc)

        return PerceptuallyHashedFile(file_hash, phash)


def input_buffer_budget() -> int:
    try:
        return max(0, int(os.environ.get("HVD_INPUT_BUFFER_MIB", "768"))) * 1024**2
    except ValueError:
        return 768 * 1024**2


class VideoInputQueue:
    """Keep the active input and lookahead buffers within one RAM budget."""

    def __init__(self, worker, foreground: FileHasher, background: FileHasher,
                 files: Sequence[str], budget: int):
        self.worker = worker
        self.foreground = foreground
        self.background = background
        self.files = files
        self.budget = budget
        self.pending = deque()
        self.reserved = 0
        self.active_size = 0
        self.metadata_start = -1
        self.sizes = {}

    def metadata(self, index: int):
        start = index // 128 * 128
        if start != self.metadata_start:
            self.metadata_start = start
            try:
                response = self.foreground.api.get_file_metadata(
                    hashes=self.files[start:start + 128],
                    create_new_file_ids=False,
                    include_services_object=False,
                )
                self.sizes = {row["hash"]: row for row in response["metadata"]}
            except (hydrus_api.HydrusAPIException, RequestException, KeyError, TypeError, ValueError):
                self.sizes = {}
        return self.sizes.get(self.files[index], {})

    def take(self, index: int) -> tuple[bytes, bool] | FailedPerceptuallyHashedFile | None:
        self.reserved -= self.active_size
        self.active_size = 0
        next_index = self.pending[-1][0] + 1 if self.pending else index
        for position in range(next_index, len(self.files)):
            size = self.metadata(position).get("size")
            if not isinstance(size, int) or not 0 < size <= self.budget - self.reserved:
                break
            work = self.worker.submit(self.background.prepare_buffer, self.files[position], size)
            self.pending.append((position, size, work))
            self.reserved += size
        if not self.pending:
            return None
        position, self.active_size, work = self.pending.popleft()
        assert position == index
        try:
            prepared = work.result()
        except Exception:
            self.active_size = 0
            self.discard()
            return None
        if isinstance(prepared, FailedPerceptuallyHashedFile):
            self.active_size = 0
            self.discard()
            return prepared
        if prepared is None:
            self.active_size = 0
            self.discard()
        return prepared

    def discard(self) -> None:
        for _, _, work in self.pending:
            if not work.cancel():
                try:
                    work.result()
                except Exception:
                    pass
        self.pending.clear()
        self.active_size = 0
        self.reserved = 0


@dataclass
class PerceptualHashingStats:
    success_hash_count: int = 0
    failed_from_api_errors_count: int = 0
    failed_from_phash_count: int = 0
    skipped_after_hydrus_outage_count: int = 0
    stopped_due_to_hydrus_outage: bool = False


class CancelledPerceptualHashException(Exception):
    """Exception for when perceptual hashing is cancelled."""

    def __init__(self, stats: PerceptualHashingStats):
        super().__init__()
        self.stats = stats


@dataclass
class NoneProgress:
    placeholder: None


@dataclass
class HashingProgress:
    complete: int
    total: int


@dataclass
class BuildingSearchTreeProgress:
    complete: int
    total: int


@dataclass
class SearchingForDuplicatesProgress:
    complete: int
    total: int


@dataclass
class DoneProgress:
    placeholder: None


@dataclass
class DedupeProgress:
    progress: NoneProgress | HashingProgress | BuildingSearchTreeProgress | SearchingForDuplicatesProgress


if TYPE_CHECKING:
    UpdateDedupeProgressCallback: TypeAlias = Callable[[DedupeProgress], None]


class HydrusVideoDeduplicator:
    hydlog = logging.getLogger("hvd")
    hydlog.setLevel(logging.INFO)
    _DEBUG = False

    def __init__(
        self,
        db: DedupeDB.DedupeDb,
        client: HVDClient,
        similarity_threshold: float = 50.0,
        job_count: int = -2,
        failed_page_name: str | None = None,
        custom_query: Sequence[str] | None = None,
        update_progress_callback: UpdateDedupeProgressCallback | None = None,
        should_skip_step_fn: Callable[[], bool] | None = None,
    ):
        self.db = db
        self.diagnostics = None
        self.submit_results = True
        self.test_run_pairs = {}
        self.frame_index = True
        self.frame_index_workers = -2
        self.frame_index_persist_progress = False
        self.log_directory = self.db.db_dir / "logs"
        self.client = client
        self.similarity_threshold = similarity_threshold
        self.job_count = job_count
        self.page_logger = None if failed_page_name is None else HydrusPageLogger(self.client, failed_page_name)
        self.search_tags = self.get_search_tags(custom_query)
        self.update_progress_callback = update_progress_callback
        if self.update_progress_callback:
            self.update_progress_callback(NoneProgress(placeholder=None))
        self.should_skip_step_fn = should_skip_step_fn

    def get_search_tags(self, custom_query: Sequence[str] | None) -> list[str]:
        # system:filetype tags are really inconsistent
        search_tags = [
            "system:filetype=video, gif, apng",
            "system:has duration",
            "system:file service is not currently in trash",
        ]

        if custom_query is not None:
            # Remove whitespace and empty strings
            custom_query = [x for x in custom_query if x.strip()]
            if len(custom_query) > 0:
                search_tags.extend(custom_query)
                print(f"[yellow]Custom Query: {custom_query}")
        return search_tags

    def deduplicate(
        self,
        skip_hashing: bool,
        skip_search: bool = False,
    ) -> int:
        try:
            with database_lock(self.db.db_dir / self.db.db_name):
                return self._deduplicate(skip_hashing, skip_search)
        except DatabaseBusyError:
            print("[red]Another HVD operation is already using this database directory.")
            raise

    def _deduplicate(self, skip_hashing: bool, skip_search: bool) -> int:
        """
        Run all deduplicate functions.

        Dedupe Algorithm:
        1. Perceptually hash the videos.
        2. Insert the perceptual hashes into the vptree
        3. Search for similar videos in the vptree.
        4. Mark the similar videos as potential duplicates in Hydrus.

        Returns the number of similar pairs found during searching.
        """
        num_similar_pairs = 0

        if skip_hashing:
            print("[yellow]Skipping perceptual hashing")
            hydrus_outage = False
        else:
            with self.diagnostics.operation("hydrus_file_selection") if self.diagnostics else nullcontext():
                video_hashes = list(self.client.get_video_hashes(self.search_tags))
            video_hashes = self.filter_unhashed(video_hashes)
            print(f"[blue]Found {len(video_hashes)} eligible files to perceptually hash.")
            print("\nTip: You can skip perceptual hashing at any time by pressing CTRL+C.\n")
            self.hydlog.info("Starting perceptual hash processing")
            self.db.begin_transaction()
            with self.db.conn:
                stats = PerceptualHashingStats()
                try:
                    with self.diagnostics.operation("hashing_and_queueing", total_files=len(video_hashes)) if self.diagnostics else nullcontext():
                        stats = self.add_perceptual_hashes_to_db(video_hashes)
                except CancelledPerceptualHashException as exc:
                    # Interrupted, but on purpose.
                    stats = exc.stats
                    print("[yellow]Perceptual hash processing was interrupted! Progress was saved.")
                else:
                    print("[green]Finished perceptual hash processing.")
                finally:
                    if self.update_progress_callback:
                        self.update_progress_callback(NoneProgress(None))
                    # Print some useful stats and info for users
                    total_failures = stats.failed_from_api_errors_count + stats.failed_from_phash_count
                    if total_failures > 0:
                        print(f"[yellow]Perceptual hash processing had {total_failures} total failed files.")

                        if stats.failed_from_api_errors_count > 0:
                            print(
                                f"[yellow]{stats.failed_from_api_errors_count} failures were due to API errors. Ensure Hydrus is running and accessible before trying again."  # noqa: E501
                            )

                        if stats.failed_from_phash_count > 0:
                            print(
                                f"[yellow]{stats.failed_from_phash_count} failures were from an error during perceptual hashing. Are the files corrupted?"  # noqa: E501
                            )
                            print(
                                "\nTip: You could have seen which files failed directly in Hydrus by "
                                "creating a Hydrus page with the name 'failed' and "
                                "running the program with '--failed-page-name=failed'\n"
                            )
                    print(f"[green]Added {stats.success_hash_count} new perceptual hashes to the database.")
                    if stats.skipped_after_hydrus_outage_count:
                        print(
                            f"[yellow]Skipped {stats.skipped_after_hydrus_outage_count} remaining files "
                            "after repeated Hydrus connection failures."
                        )
                    hydrus_outage = stats.stopped_due_to_hydrus_outage

            if hydrus_outage:
                # EFI can still search the local database and save its pair report,
                # but it must not try Hydrus submissions while the API is unavailable.
                self.submit_results = False
                print(
                    f"[yellow]Hydrus remained unavailable. Continuing duplicate search without submitting "
                    f"relationships; EFI results will be written under {self.log_directory}."
                )

        if skip_search:
            print("[yellow]Skipping search index construction and duplicate search.")
            self.hydlog.info("Search skipped after perceptual hashing.")
            if self.update_progress_callback:
                self.update_progress_callback(DoneProgress(None))
            return 0

        if not self.frame_index:
            print("\nTip: You can skip building the search tree at any time by pressing CTRL+C.\n")
            self.db.begin_transaction()
            with self.db.conn:
                try:
                    with self.diagnostics.operation("search_tree_insertion") if self.diagnostics else nullcontext():
                        self.process_phashed_file_queue()
                except KeyboardInterrupt:
                    print("[yellow]Building the search tree was interrupted! Progress was saved.")
                else:
                    print("[green]Finished fully building the search tree.")

            self.db.begin_transaction()
            with self.db.conn:
                try:
                    self.run_maintenance()
                except KeyboardInterrupt:
                    print("[yellow]Maintenance was interrupted!")
                else:
                    print("[green]Finished maintenance.")

        # Number of potential duplicates before adding more.
        # This is just to print info for the user.
        # Note: This will be inaccurate if the user searches for duplicates in the Hydrus client
        #       while this is running.
        pre_dedupe_count = None
        if not hydrus_outage:
            pre_dedupe_count = self.client.get_potential_duplicate_count_hydrus()

        print("\nTip: You can skip finding potential duplicates at any time by pressing CTRL+C.\n")
        self.db.begin_transaction()
        with self.db.conn:
            try:
                num_similar_pairs = self.find_potential_duplicates()
            except KeyboardInterrupt:
                print("[yellow]Searching for duplicates was interrupted! Progress was saved.")

        # Statistics for user
        if hydrus_outage:
            print(f"[green]{num_similar_pairs} potential duplicate pairs found; see the EFI JSON report.")
        else:
            post_dedupe_count = self.client.get_potential_duplicate_count_hydrus()
            new_dedupes_count = post_dedupe_count - pre_dedupe_count
            if new_dedupes_count > 0:
                print(f"[green]{new_dedupes_count} new potential duplicate pairs marked for manual processing!")
            else:
                print("[green]No new potential duplicate pairs found.")

        self.hydlog.info(f"{num_similar_pairs} similar file pairs found.")
        self.hydlog.info("Deduplication done.")
        if self.update_progress_callback:
            self.update_progress_callback(DoneProgress(None))

        return num_similar_pairs

    def filter_unhashed(self, file_hashes: list[FileHash]) -> list[FileHash]:
        """
        Get only the files that have not been perceptually hashed in the db from a list of files.
        """
        all_phashed_files = self.db.get_phashed_files()
        return [file_hash for file_hash in file_hashes if file_hash not in all_phashed_files]

    def add_perceptual_hashes_to_db(self, video_hashes: Sequence[str]) -> PerceptualHashingStats:
        """
        Retrieves the video from Hydrus,
        calculates the perceptual hash,
        and then add it to the database.
        """
        stats = PerceptualHashingStats()
        try:
            with tqdm(
                total=len(video_hashes),
                desc="Perceptually hashing files",
                dynamic_ncols=True,
                unit="file",
                colour="BLUE",
            ) as pbar, ExitStack() as input_stack:
                circuit_breakers = HashingCircuitBreakers()
                filehasher = FileHasher(
                    self.client, self.job_count, self.diagnostics,
                    circuit_breakers=circuit_breakers,
                )
                input_queue = None
                new_session_client = getattr(self.client.client, "new_session_client", None)
                if video_hashes and (budget := input_buffer_budget()) and callable(new_session_client):
                    background_api = new_session_client()
                    input_stack.callback(background_api.session.close)
                    worker = ThreadPoolExecutor(max_workers=1)
                    input_stack.callback(lambda: worker.shutdown(wait=True, cancel_futures=True))
                    input_queue = VideoInputQueue(
                        worker,
                        filehasher,
                        FileHasher(
                            self.client, self.job_count, self.diagnostics,
                            api=background_api,
                            circuit_breakers=circuit_breakers,
                        ),
                        video_hashes,
                        budget,
                    )
                    input_stack.callback(input_queue.discard)
                successful_phashed_count = 0
                for index, video_hash in enumerate(video_hashes):
                    if self.diagnostics:
                        self.diagnostics.progress(files_hashed=pbar.n, total_files=pbar.total)
                    if self.update_progress_callback:
                        self.update_progress_callback(HashingProgress(complete=pbar.n + 1, total=pbar.total))
                    if self.should_skip_step_fn and self.should_skip_step_fn():
                        return stats

                    prepared = input_queue.take(index) if input_queue is not None else None
                    local_buffer = isinstance(prepared, tuple) and prepared[1]
                    if isinstance(prepared, FailedPerceptuallyHashedFile):
                        result = prepared
                    else:
                        with self.diagnostics.operation("video_fetch_and_hash", file_hash=video_hash) if self.diagnostics else nullcontext():
                            result = filehasher.hash_buffer(video_hash, prepared)
                    del prepared
                    if local_buffer and isinstance(result, FailedPerceptuallyHashedFile):
                        input_queue.discard()
                        result = filehasher._fetch_and_phash_http(video_hash)
                    if isinstance(result, FailedPerceptuallyHashedFile):
                        # We only want to add the failure to the page if the file was the actual cause of failure.
                        if isinstance(result.exc, HydrusApiException):
                            stats.failed_from_api_errors_count += 1
                            print("[red]Hydrus API error during perceptual hashing:")
                            print(f"{result.exc}")
                        else:
                            stats.failed_from_phash_count += 1
                            print("[red]Failed to perceptually hash a file.")
                            print(f"Failed file SHA256 hash: {result.file_hash}")
                            print(f"{result.exc}")
                            if self.page_logger:
                                self.page_logger.add_failed_video(result.file_hash)
                    else:
                        with self.diagnostics.operation("database_hash_queue_write") if self.diagnostics else nullcontext():
                            self.db.add_to_phashed_files_queue(result.file_hash, result.perceptual_hash)
                        stats.success_hash_count += 1
                        successful_phashed_count += 1
                        # Commit periodically to save progress in case something fails.
                        COMMIT_BATCH_SIZE = 4
                        if successful_phashed_count % COMMIT_BATCH_SIZE == 0:
                            self.db.commit()

                    # Collect garbage now to avoid huge memory usage from the video files and frames.
                    gc.collect()
                    pbar.update(1)
                    if self.update_progress_callback:
                        self.update_progress_callback(HashingProgress(complete=pbar.n, total=pbar.total))
                    if circuit_breakers.http_open:
                        stats.stopped_due_to_hydrus_outage = True
                        stats.skipped_after_hydrus_outage_count = len(video_hashes) - pbar.n
                        if input_queue is not None:
                            input_queue.discard()
                        break
        except KeyboardInterrupt:
            raise CancelledPerceptualHashException(stats)
        gc.collect()
        return stats

    def mark_videos_as_duplicates(self, video1_hash: str, video2_hash: str):
        """Mark a pair of videos as duplicates in Hydrus."""
        if not self.submit_results:
            return
        new_relationship = {
            "hash_a": video1_hash,
            "hash_b": video2_hash,
            "relationship": int(hydrus_api.DuplicateStatus.POTENTIAL_DUPLICATES),
            "do_default_content_merge": True,
        }

        with self.diagnostics.operation("hydrus_relationship_submission") if self.diagnostics else nullcontext():
            self.client.client.set_file_relationships([new_relationship])

    def process_phashed_file_queue(self):
        """
        Process the files in the phashed files queue.
        This inserts the queue entries into their respective tables and then inserts the file into the vptree.
        """
        results = self.db.execute("SELECT file_hash, phash FROM phashed_file_queue").fetchall()
        with tqdm(
            total=len(results),
            desc="Building search tree",
            dynamic_ncols=True,
            unit="file",
            colour="BLUE",
        ) as pbar:
            processed_count = 0
            for file_hash, perceptual_hash in results:
                if self.update_progress_callback:
                    self.update_progress_callback(BuildingSearchTreeProgress(complete=pbar.n, total=pbar.total))
                if self.should_skip_step_fn and self.should_skip_step_fn():
                    return

                self.db.add_file(file_hash)
                self.db.add_perceptual_hash(perceptual_hash)
                self.db.associate_file_with_perceptual_hash(file_hash, perceptual_hash)
                self.db.execute(
                    "DELETE FROM phashed_file_queue WHERE file_hash = :file_hash AND phash = :phash",
                    {"file_hash": file_hash, "phash": perceptual_hash},
                )
                if self.diagnostics:
                    self.diagnostics.progress(tree_files_inserted=pbar.n + 1, total_files=pbar.total)
                processed_count += 1
                # Commit periodically to save progress in case something fails.
                COMMIT_BATCH_SIZE = 64
                if processed_count % COMMIT_BATCH_SIZE == 0:
                    self.db.commit()
                pbar.update(1)
                if self.diagnostics:
                    self.diagnostics.progress(tree_files_inserted=pbar.n, total_files=pbar.total)
                if self.update_progress_callback:
                    self.update_progress_callback(BuildingSearchTreeProgress(complete=pbar.n, total=pbar.total))
            if self.update_progress_callback:
                self.update_progress_callback(BuildingSearchTreeProgress(complete=len(results), total=len(results)))

    def run_maintenance(self):
        """Run maintenance, if needed."""
        # TODO: Currently broken. If it's cancelled while running it will report all the branches as broken.
        return
        tree = vptree.VpTreeManager(self.db)

        if tree.maintenance_due():
            # TODO: Do further testing on this.
            print("[blue]Running search tree maintenance...")
            tree.maintain_tree()

    def find_potential_duplicates(
        self,
    ) -> int:
        """
        Find potential duplicates in the database and mark them as such in Hydrus.

        Returns the number of similar file pairs found.
        """
        if self.frame_index:
            print("Searching the complete exact frame index.")
            context = None
            if self.submit_results or self.frame_index_persist_progress:
                try:
                    identity = service_context(self.client.client, self.client.client.api_url)
                    context = hashlib.sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest()
                except Exception as error:
                    raise FrameIndexError(f"Could not identify the Hydrus client ({type(error).__name__}).") from None

            def record_pair(file_a: str, file_b: str, score: int):
                pair = tuple(sorted((file_a, file_b)))
                pair_scores = self.test_run_pairs.setdefault(pair, [None, None])
                pair_scores[0 if file_a == pair[0] else 1] = float(score)

            self.db.commit()
            if self.diagnostics:
                self.diagnostics.progress(efi_search_started=True)
            result = run_frame_index_search(
                self.db.db_dir / self.db.db_name,
                self.mark_videos_as_duplicates,
                context=context,
                threshold=self.similarity_threshold,
                diagnostics=self.diagnostics,
                should_skip=self.should_skip_step_fn,
                progress=lambda complete, total: self.update_progress_callback(
                    SearchingForDuplicatesProgress(complete=complete, total=total)
                ) if self.update_progress_callback else None,
                workers=self.frame_index_workers,
                output_directory=self.log_directory,
                record_pair=record_pair,
                submission_enabled=self.submit_results,
                persist_progress=self.frame_index_persist_progress,
                acquire_lock=False,
            )
            if not result["complete"]:
                raise FrameIndexError(
                    f"EFI search stopped ({result['status']}); see {result['report_directory']}"
                )
            return result["unique_pairs"]

        tree = vptree.VpTreeManager(self.db)
        search_threshold = vptree.fix_vpdq_similarity(self.similarity_threshold)

        files = self.db.execute(
            "SELECT hash_id FROM shape_search_cache WHERE searched_distance is NULL or searched_distance < :threshold",
            {"threshold": search_threshold},
        ).fetchall()
        pending_hash_ids = {row[0] for row in files}
        new_hash_ids = {
            row[0]
            for row in self.db.execute(
                "SELECT hash_id FROM shape_search_cache WHERE searched_distance IS NULL"
            ).fetchall()
        }

        # Previously completed queries do not normally run again when a video is
        # added. Compare those old query hashes directly against only new hashes
        # so old -> new matches are not missed.
        new_phashes = []
        for new_hash_id in new_hash_ids:
            phash_id = self.db.get_phash_id_from_hash_id(new_hash_id)
            if phash_id is not None:
                phash = self.db.get_phash(phash_id)
                file_hash = self.db.get_file_hash(new_hash_id)
                if phash is not None and file_hash is not None:
                    new_phashes.append((new_hash_id, phash, file_hash))

        found_pairs = set()

        def mark_pair_once(hash_a: str, hash_b: str) -> None:
            pair = tuple(sorted((hash_a, hash_b)))
            if pair in found_pairs:
                return
            found_pairs.add(pair)
            self.hydlog.info(f'Similar files found: "{hash_a}" and "{hash_b}"')
            self.mark_videos_as_duplicates(hash_a, hash_b)

        with tqdm(
            dynamic_ncols=True, total=len(files), desc="Finding potential duplicates", unit="file", colour="BLUE"
        ) as pbar:
            processed_count = 0
            for hash_id in files:
                if self.update_progress_callback:
                    self.update_progress_callback(SearchingForDuplicatesProgress(complete=pbar.n, total=pbar.total))
                if self.should_skip_step_fn and self.should_skip_step_fn():
                    # Stop searching, but still report the pairs found so far.
                    break

                hash_id = hash_id[0]
                with self.diagnostics.operation("duplicate_search", hash_id=hash_id) if self.diagnostics else nullcontext():
                    result = tree.search_file(hash_id, max_hamming_distance=search_threshold)
                file_hash_a = self.db.get_file_hash(hash_id)
                for similar_hash_id, distance in result:
                    file_hash_b = self.db.get_file_hash(similar_hash_id)
                    if hash_id != similar_hash_id:
                        score = max(0.0, min(100.0, 101.0 - distance))
                        pair = tuple(sorted((file_hash_a, file_hash_b)))
                        pair_scores = self.test_run_pairs.setdefault(pair, [None, None])
                        pair_scores[0 if file_hash_a == pair[0] else 1] = score
                        mark_pair_once(file_hash_a, file_hash_b)

                # TODO:
                # Do we need to add the below line here? See _PerceptualHashesSearchForPotentialDuplicates in Hydrus.
                # group_of_hash_ids = self._STL( self._Execute( 'SELECT hash_id FROM shape_search_cache WHERE searched_distance IS NULL or searched_distance < ?;', ( search_distance, ) ).fetchmany( 10 ) )   # noqa: E501
                # Update the search cache
                self.db.execute(
                    "UPDATE shape_search_cache SET searched_distance = ? WHERE hash_id = ?;",
                    (search_threshold, hash_id),
                )

                processed_count += 1
                # Commit periodically to save progress in case something fails.
                COMMIT_BATCH_SIZE = 64
                if processed_count % COMMIT_BATCH_SIZE == 0:
                    self.db.commit()

                pbar.update(1)
                if self.diagnostics:
                    self.diagnostics.progress(videos_searched=pbar.n, total_videos=pbar.total)
                if self.update_progress_callback:
                    self.update_progress_callback(SearchingForDuplicatesProgress(complete=pbar.n, total=pbar.total))

        if new_phashes:
            old_query_rows = self.db.execute(
                "SELECT shape_search_cache.hash_id, shape_perceptual_hashes.phash, files.file_hash "
                "FROM shape_search_cache "
                "JOIN shape_perceptual_hash_map USING (hash_id) "
                "JOIN shape_perceptual_hashes USING (phash_id) "
                "JOIN files USING (hash_id) "
                "WHERE searched_distance >= :threshold",
                {"threshold": search_threshold},
            ).fetchall()
            with tqdm(
                dynamic_ncols=True,
                total=len(old_query_rows),
                desc="Checking existing files against new hashes",
                unit="file",
                colour="BLUE",
            ) as reverse_pbar:
                for old_hash_id, old_phash, old_file_hash in old_query_rows:
                    if old_hash_id in pending_hash_ids:
                        reverse_pbar.update(1)
                        continue
                    for new_hash_id, new_phash, new_file_hash in new_phashes:
                        if self.should_skip_step_fn and self.should_skip_step_fn():
                            break
                        if old_hash_id == new_hash_id:
                            continue
                        distance = vptree.calculate_distance(old_phash, new_phash)
                        if distance <= search_threshold:
                            score = max(0.0, min(100.0, 101.0 - distance))
                            pair = tuple(sorted((old_file_hash, new_file_hash)))
                            pair_scores = self.test_run_pairs.setdefault(pair, [None, None])
                            pair_scores[0 if old_file_hash == pair[0] else 1] = score
                            mark_pair_once(old_file_hash, new_file_hash)
                    reverse_pbar.update(1)
                    if self.should_skip_step_fn and self.should_skip_step_fn():
                        break

        return len(found_pairs)
