from __future__ import annotations

import logging
import atexit
from contextlib import closing, nullcontext
from enum import Enum
import json
import sqlite3
from typing import TYPE_CHECKING, Annotated, List, Optional

if TYPE_CHECKING:
    from typing import NoReturn

from pathlib import Path

import typer
from rich import print

from hydrusvideodeduplicator.__about__ import __version__
from hydrusvideodeduplicator.client import (
    ClientAPIException,
    FailedHVDClientConnection,
    HVDClient,
    create_client,
)
from hydrusvideodeduplicator.config import Config
from hydrusvideodeduplicator.db import DedupeDB
from hydrusvideodeduplicator.dedup import HydrusVideoDeduplicator
from hydrusvideodeduplicator.dedup_util import print_and_log
from hydrusvideodeduplicator.search_diagnostics import SearchDiagnostics
from dotenv import find_dotenv, load_dotenv

import sys

"""
Parameters:
- api_key will be read from env var $HYDRUS_API_KEY or .env file
- api_url will be read from env var $HYDRUS_API_URL or .env file
- to add custom queries, do
  --custom-query="series:twilight" --custom-query="character:edward" ... etc for each query
- threshold is the min % matching to be considered similar. 100% is very very similar.
- verbose turns on logging
- debug turns on logging and sets the logging level to debug
"""

loaded_dotenv = load_dotenv()
config = Config.load_from_env()


class TestRunMode(str, Enum):
    NEW = "new"
    TEMP = "temp"
    SAVE = "save"


def prepare_test_database(directory: Path, mode: TestRunMode) -> Path:
    if mode is TestRunMode.SAVE:
        DedupeDB.set_db_name("videohashes.sqlite")
        return directory / "videohashes.sqlite"

    source = directory / "videohashes.sqlite"
    target = directory / "test-videohashes.sqlite"
    temporary = directory / ".test-videohashes.sqlite.tmp"
    directory.mkdir(parents=True, exist_ok=True)
    if mode is TestRunMode.TEMP and not source.is_file():
        raise typer.BadParameter("--test-run temp requires an existing videohashes.sqlite database.")
    for path in (target, Path(str(target) + "-wal"), Path(str(target) + "-shm"), temporary):
        path.unlink(missing_ok=True)

    if mode is TestRunMode.TEMP:
        with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as source_db:
            with closing(sqlite3.connect(temporary)) as target_db:
                source_db.backup(target_db)
                target_db.commit()
        temporary.replace(target)

    DedupeDB.set_db_name("test-videohashes.sqlite")
    return target


def main(
    api_key: Annotated[
        Optional[str],
        typer.Option(help="Hydrus API Key", show_default=False, prompt=False if config.hydrus_api_key else True),
    ] = config.hydrus_api_key,
    api_url: Annotated[Optional[str], typer.Option(help="Hydrus API URL")] = config.hydrus_api_url,
    overwrite: Annotated[Optional[Optional[bool]], typer.Option(hidden=True)] = None,  # deprecated
    query: Annotated[Optional[List[str]], typer.Option(help="Custom Hydrus tag query")] = config.hydrus_query,
    threshold: Annotated[
        Optional[float], typer.Option(help="Similarity threshold for a pair of videos where 100 is identical")
    ] = config.similarity_threshold,
    skip_hashing: Annotated[
        Optional[bool], typer.Option(help="Skip perceptual hashing and just search for duplicates")
    ] = False,
    file_service_key: Annotated[
        Optional[List[str]], typer.Option(help="Local file service key")
    ] = config.hydrus_local_file_service_keys,
    verify_cert: Annotated[
        Optional[str], typer.Option(help="Path to TLS cert. This forces verification.")
    ] = config.requests_ca_bundle,
    clear_search_tree: Annotated[
        Optional[bool], typer.Option(help="Clear the search tree that tracks what files have already been compared.")
    ] = False,
    clear_search_cache: Annotated[
        Optional[bool],
        typer.Option(
            help="Clear the search cache that tracks what files have been compared with a given similarity threshold."
        ),
    ] = False,
    failed_page_name: Annotated[
        Optional[str], typer.Option(help="The name of the Hydrus page to add failed files to.")
    ] = config.failed_page_name,
    job_count: Annotated[
        Optional[int],
        typer.Option(help="Number of CPU threads to use for perceptual hashing. Default is all but one core."),
    ] = config.job_count,
    dedup_database_dir: Annotated[
        Optional[Path], typer.Option(help="The directory to store the database used for dedupe.")
    ] = config.dedupe_database_dir,
    diagnostic_mode: Annotated[bool, typer.Option("--diagnostic-mode", help="Record program stages and resource usage.")] = False,
    diagnostics_interval: Annotated[float, typer.Option(help="Seconds between resource samples.")] = 2.0,
    log_path: Annotated[Optional[Path], typer.Option(help="Directory for diagnostic and test-run JSON files.")] = None,
    no_submit: Annotated[bool, typer.Option("--no-submit", help="Do not send duplicate relationships to Hydrus.")] = False,
    test_run: Annotated[Optional[TestRunMode], typer.Option(help="Test mode: new, temp, or save.")] = None,
    verbose: Annotated[Optional[bool], typer.Option(help="Verbose logging")] = False,
    debug: Annotated[Optional[bool], typer.Option(hidden=True)] = False,
    gui: Annotated[  # This gui arg is unused, it's only here to show on --help. See run_main() for more info.
        Optional[bool], typer.Option(help="Launch the GUI. This ignores all other CLI arguments.")
    ] = config.hvd_gui,
):
    # Fix mypy errors from optional parameters
    assert threshold is not None and skip_hashing is not None and job_count is not None

    # CLI debug parameter sets log level to info or debug
    loglevel = logging.INFO
    if debug:
        loglevel = logging.DEBUG
        verbose = True

    logging.basicConfig(format=" %(asctime)s - %(name)s: %(message)s", datefmt="%H:%M:%S", level=loglevel)
    logger = logging.getLogger("main")
    logger.debug("Starting Hydrus Video Deduplicator.")

    def exit_from_failure() -> NoReturn:
        print_and_log(logger, "Exiting due to failure...")
        raise typer.Exit(code=1)

    # Verbose sets whether logs are shown to the user at all.
    # Logs are separate from printing in this program.
    if not verbose:
        logging.disable()

    DedupeDB.set_db_dir(dedup_database_dir)
    log_directory = log_path if log_path is not None else dedup_database_dir / "logs"
    test_run_report_path = None
    if test_run is not None:
        if clear_search_cache or clear_search_tree:
            raise typer.BadParameter("Search cache/tree clearing cannot be combined with --test-run.")
        test_run_report_path = prepare_test_database(dedup_database_dir, test_run)
    else:
        DedupeDB.set_db_name("videohashes.sqlite")
    diagnostics = SearchDiagnostics(log_directory, interval=diagnostics_interval) if diagnostic_mode else None
    if diagnostics is not None:
        atexit.register(diagnostics.close)

    # Print a warning if the deprecated overwrite option is set
    if overwrite is not None:
        pretty_overwrite = "--" + ("" if overwrite is True else "no-") + "overwrite"
        print_and_log(
            logger,
            f"WARNING: '{pretty_overwrite}' option was deprecated and does nothing as of 0.7.0. Remove it from your args.",  # noqa: E501
        )

    # CLI overwrites env vars with no default value
    if not api_key:
        api_key = config.hydrus_api_key

    # Check for necessary variables
    if not api_key:
        print_and_log(logger, "Hydrus API key is not set. Please set with '--api-key'.")
        exit_from_failure()
    # This should not happen because there's a default val in config.py
    if not api_url:
        print_and_log(logger, "Hydrus API URL is not set. Please set with '--api-url'.")
        exit_from_failure()

    if threshold < 0.0 or threshold > 100.0:
        print_and_log(logger, f"Invalid similarity threshold: {threshold}. Must be between 0 and 100.", logging.ERROR)
        exit_from_failure()

    # Client connection
    print_and_log(logger, f"Connecting to Hydrus at {api_url}")
    try:
        with diagnostics.operation("hydrus_connect_and_permissions") if diagnostics else nullcontext():
            hvdclient = create_client(file_service_key, api_url, api_key, verify_cert)
            api_version = hvdclient.get_api_version()
            hydrus_api_version = hvdclient.get_hydrus_api_version()
            print_and_log(logger, f"Dedupe API version: 'v{api_version}'")
            print_and_log(logger, f"Hydrus API version: 'v{hydrus_api_version}'")
            hvdclient.verify_permissions()
    except (FailedHVDClientConnection, ClientAPIException) as exc:
        print_and_log(logger, f"{exc.pretty_msg} \nException: {exc}", logging.FATAL)
        exit_from_failure()

    if debug:
        HVDClient._log.setLevel(logging.DEBUG)

    # Deduplication

    if DedupeDB.does_db_exist():
        print_and_log(logger, f"Found existing database at: '{DedupeDB.get_db_file_path()}'")
        db = DedupeDB.DedupeDb(DedupeDB.get_db_dir(), DedupeDB.get_db_name())
        with diagnostics.operation("database_open") if diagnostics else nullcontext():
            db.init_connection()
        # Upgrade the database before doing anything.
        db.begin_transaction()
        db_upgraded = False
        with db.conn:
            with diagnostics.operation("database_upgrade") if diagnostics else nullcontext():
                db_upgraded = db.upgrade_db()
        # Vacuum DB after a successful database upgrade. This can reduce space by 1/2 in cases of large
        # db migrations.
        if db_upgraded:
            print_and_log(
                logger,
                "Database upgraded, vacuuming to save space.",
            )
            db_stats = DedupeDB.get_db_stats(db)
            print_and_log(
                logger,
                f"Database filesize before vacuum: {db_stats.file_size} bytes.",
            )
            with diagnostics.operation("database_vacuum") if diagnostics else nullcontext():
                db.vacuum()
            db_stats = DedupeDB.get_db_stats(db)
            print_and_log(
                logger,
                f"Database filesize after vacuum: {db_stats.file_size} bytes.",
            )
        db_stats = DedupeDB.get_db_stats(db)

        print_and_log(
            logger,
            f"Database has {db_stats.num_videos} videos already perceptually hashed.",
        )
        print_and_log(
            logger,
            f"Database filesize: {db_stats.file_size} bytes.",
        )

        if clear_search_tree:
            db.begin_transaction()
            with db.conn:
                db.clear_search_tree()
            print("[green] Cleared the search tree.")

        if clear_search_cache:
            db.begin_transaction()
            with db.conn:
                db.clear_search_cache()
            print("[green] Cleared the search cache.")

    else:
        print_and_log(logger, f"Database not found. Creating one at '{DedupeDB.get_db_file_path()}'", logging.INFO)
        if not DedupeDB.get_db_dir().exists():
            DedupeDB.create_db_dir()
        db = DedupeDB.DedupeDb(DedupeDB.get_db_dir(), DedupeDB.get_db_name())
        with diagnostics.operation("database_open") if diagnostics else nullcontext():
            db.init_connection()
        db.begin_transaction()
        with db.conn:
            with diagnostics.operation("database_create_schema") if diagnostics else nullcontext():
                db.create_tables()
        db_stats = DedupeDB.get_db_stats(db)

    deduper = HydrusVideoDeduplicator(
        db,
        client=hvdclient,
        similarity_threshold=threshold,
        job_count=job_count,
        failed_page_name=failed_page_name,
        custom_query=query,
    )
    deduper.diagnostics = diagnostics
    deduper.no_submit = no_submit

    if debug:
        deduper.hydlog.setLevel(logging.DEBUG)
        deduper._DEBUG = True

    try:
        with diagnostics.operation("deduplication") if diagnostics else nullcontext():
            num_similar_pairs = deduper.deduplicate(skip_hashing=skip_hashing)
            if test_run_report_path is not None:
                log_directory.mkdir(parents=True, exist_ok=True)
                report_path = log_directory / "pairs.json"
                rows = []
                for (file_a, file_b), (score_a_to_b, score_b_to_a) in sorted(deduper.test_run_pairs.items()):
                    rows.append({"file_a": file_a, "file_b": file_b,
                                 "score_a_to_b": score_a_to_b, "score_b_to_a": score_b_to_a})
                report_path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    finally:
        if diagnostics is not None:
            diagnostics.close()

    db.close()

    return num_similar_pairs


def run_main(gui: bool, is_executable: bool):
    print(f"[blue]Hydrus Video Deduplicator {__version__} [/]\n")
    if loaded_dotenv:
        # Note: This doesn't mean any values were actually loaded from the .env file.
        print(f"Config file found: '{find_dotenv()}'")
    if gui or bool(int(config.hvd_gui)):
        try:
            from hydrusvideodeduplicator.gui.gui import gui_main
        except ImportError as exc:
            print_and_log(
                logging.getLogger("main"),
                f"Failed to import GUI dependencies. Did you install the GUI dependencies? Error: {exc}",  # noqa: E501
                logging.FATAL,
            )
            raise typer.Exit(code=1) from exc

        gui_main(config)
    else:
        # Allow launching the GUI via CLI using --gui. This is required before the typer.run()
        # because the typer checks if arguments exist, but we don't need any of the args for the GUI
        # to run.
        if len(sys.argv) > 1 and sys.argv[1] == "--gui":
            try:
                from hydrusvideodeduplicator.gui.gui import gui_main
            except ImportError as exc:
                print_and_log(
                    logging.getLogger("main"),
                    f"Failed to import GUI dependencies. Did you install the GUI dependencies? Error: {exc}",  # noqa: E501
                    logging.FATAL,
                )
                raise typer.Exit(code=1) from exc

            gui_main(config)
        else:
            try:
                typer.run(main)
            except KeyboardInterrupt as exc:
                raise typer.Exit(-1) from exc
            finally:
                if is_executable:
                    # Hang the console window for the CLI exe, because 99% of users will be running this
                    # interactively and will want this pause to see errors/the final results. The other 1% should file
                    # a Github issue if this causes them issues and they want some --no-interactive option, or they should just # noqa: E501
                    # run the Python install.
                    input("Press ENTER to exit...")


if __name__ == "__main__":
    run_main(gui=False, is_executable=False)
