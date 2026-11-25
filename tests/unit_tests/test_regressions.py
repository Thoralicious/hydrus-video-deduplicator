"""Search/submission regressions using disposable databases and a mocked Hydrus API."""

from contextlib import closing
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import typer
from typer.testing import CliRunner

from hydrusvideodeduplicator import entrypoint
from hydrusvideodeduplicator.db import DedupeDB, vptree
from hydrusvideodeduplicator.dedup import HydrusVideoDeduplicator
from hydrusvideodeduplicator.frame_index_search import run_frame_index_search
from hydrusvideodeduplicator.frame_index_state import FrameSearchState, state_path_for


@pytest.fixture
def database(tmp_path):
    db = DedupeDB.DedupeDb(tmp_path, "videohashes.sqlite")
    db.init_connection()
    vptree.TemporaryIntegerTableNameCache()
    db.create_tables()
    db.commit()
    try:
        yield db
    finally:
        db.close()


def add_hashed(db, file_hash, packed):
    db.add_file(file_hash)
    db.add_perceptual_hash(packed)
    db.associate_file_with_perceptual_hash(file_hash, packed)


def make_deduper(db, threshold=50):
    client = SimpleNamespace(client=SimpleNamespace(set_file_relationships=Mock()))
    return HydrusVideoDeduplicator(db, client=client, similarity_threshold=threshold)


def exact_search(db, submit, threshold, **kwargs):
    return run_frame_index_search(
        db.db_dir / db.db_name,
        submit,
        context="test-client",
        threshold=threshold,
        workers=1,
        output_directory=db.db_dir / "logs",
        **kwargs,
    )


def test_pending_pairs_respect_threshold_and_survive_for_later_submission(database):
    db = database
    db.add_to_phashed_files_queue("a" * 64, bytes(32) + bytes([255]) * 32)
    db.add_to_phashed_files_queue("b" * 64, bytes(32) + bytes([85]) * 32)
    db.commit()
    submit = Mock()
    dry = exact_search(db, submit, 50, submission_enabled=False, persist_progress=True)
    strict = exact_search(db, submit, 90)
    assert dry["complete"] and strict["complete"]
    assert strict["unique_pairs"] == strict["submitted_pairs"] == 0
    submit.assert_not_called()
    with closing(FrameSearchState(state_path_for(db.db_dir / db.db_name))) as state:
        assert len(state.pending_pairs("test-client")) == 1
    relaxed = exact_search(db, submit, 50)
    assert relaxed["complete"] and relaxed["submitted_pairs"] == 1
    submit.assert_called_once_with("a" * 64, "b" * 64)
    repeated = exact_search(db, submit, 50)
    assert repeated["complete"] and repeated["submitted_pairs"] == 0
    assert submit.call_count == 1


def test_pending_replay_reports_only_qualifying_directions(database):
    db = database
    db.add_to_phashed_files_queue("a" * 64, bytes(32))
    db.add_to_phashed_files_queue("b" * 64, bytes(32) + bytes([255]) * 32)
    db.commit()
    submit = Mock()
    assert exact_search(db, submit, 50, submission_enabled=False, persist_progress=True)["complete"]
    report_pairs = Mock()
    report = exact_search(db, submit, 90, record_pair=report_pairs)
    assert report["complete"] and report["submitted_pairs"] == 1
    report_pairs.assert_called_once_with("a" * 64, "b" * 64, 100)
    matches = (Path(report["report_directory"]) / "matches.jsonl").read_text().splitlines()
    assert [json.loads(row)["directional_score"] for row in matches] == [100]


def test_pending_submission_failure_can_be_retried(database):
    db = database
    for key in ("a" * 64, "b" * 64):
        db.add_to_phashed_files_queue(key, bytes(32))
    db.commit()
    submit = Mock()
    assert exact_search(db, submit, 50, submission_enabled=False, persist_progress=True)["complete"]
    submit.side_effect = RuntimeError("Simulated submission failure")
    failed = exact_search(db, submit, 50)
    assert failed["status"] == "failed" and failed["submitted_pairs"] == 0
    submit.reset_mock(side_effect=True)
    retry = exact_search(db, submit, 50)
    assert retry["complete"] and retry["submitted_pairs"] == 1
    submit.assert_called_once()


@pytest.mark.parametrize("interruption", ["skip", "keyboard", "submission"])
def test_reverse_search_interruption_keeps_new_files_pending(database, monkeypatch, interruption):
    db = database
    old_files, new_file = ("a" * 64, "b" * 64), "c" * 64
    for key in old_files:
        add_hashed(db, key, bytes(32))
        db.execute("UPDATE shape_search_cache SET searched_distance=11 WHERE hash_id=?", (db.get_hash_id(key),))
    add_hashed(db, new_file, bytes(32) + bytes([255]) * 32)
    db.commit()
    deduper = make_deduper(db, 90)
    deduper.frame_index = False
    submit = deduper.client.client.set_file_relationships
    actual_distance = vptree.calculate_distance
    checks = 0

    def distance(*args):
        nonlocal checks
        checks += 1
        if interruption == "keyboard" and checks == 2:
            raise KeyboardInterrupt
        return actual_distance(*args)

    with monkeypatch.context() as scoped:
        scoped.setattr(vptree, "calculate_distance", distance)
        if interruption == "skip":
            deduper.should_skip_step_fn = lambda: checks >= 1
            assert deduper.find_potential_duplicates() == 1
        else:
            if interruption == "submission":
                submit.side_effect = [None, RuntimeError("Simulated submission failure")]
            error = KeyboardInterrupt if interruption == "keyboard" else RuntimeError
            with pytest.raises(error):
                deduper.find_potential_duplicates()
    db.commit()
    assert db.execute(
        "SELECT searched_distance FROM shape_search_cache WHERE hash_id=?", (db.get_hash_id(new_file),)
    ).fetchone() == (None,)
    deduper.should_skip_step_fn = None
    submit.reset_mock(side_effect=True)
    assert deduper.find_potential_duplicates() == 2
    pairs = {frozenset((call.args[0][0]["hash_a"], call.args[0][0]["hash_b"])) for call in submit.call_args_list}
    assert pairs == {frozenset((key, new_file)) for key in old_files}
    assert db.execute(
        "SELECT searched_distance FROM shape_search_cache WHERE hash_id=?", (db.get_hash_id(new_file),)
    ).fetchone() == (11,)


def test_legacy_no_submission_run_does_not_consume_live_progress(database):
    db = database
    for key in ("a" * 64, "b" * 64):
        add_hashed(db, key, bytes(32))
    db.commit()
    deduper = make_deduper(db)
    deduper.frame_index = False
    deduper.submit_results = False
    assert deduper.find_potential_duplicates() == 1
    db.commit()
    deduper.client.client.set_file_relationships.assert_not_called()
    assert db.execute("SELECT searched_distance FROM shape_search_cache").fetchall() == [(None,), (None,)]
    deduper.submit_results = True
    assert deduper.find_potential_duplicates() == 1
    deduper.client.client.set_file_relationships.assert_called_once()
    db.commit()
    assert deduper.find_potential_duplicates() == 0


@pytest.mark.parametrize("collect", [False, True])
def test_efi_only_collects_pairs_when_requested(database, monkeypatch, collect):
    deduper = make_deduper(database)
    deduper.submit_results = False
    deduper.collect_test_run_pairs = collect

    def search(*args, **kwargs):
        record = kwargs["record_pair"]
        if collect:
            record("a" * 64, "b" * 64, 100)
            record("b" * 64, "a" * 64, 50)
        else:
            assert record is None
        return {"complete": True, "unique_pairs": 1}

    monkeypatch.setattr("hydrusvideodeduplicator.dedup.run_frame_index_search", search)
    assert deduper.find_potential_duplicates() == 1
    assert deduper.test_run_pairs == ({("a" * 64, "b" * 64): [100, 50]} if collect else {})


@pytest.mark.parametrize("collect", [False, True])
def test_legacy_only_collects_pairs_when_requested(database, collect):
    for key in ("a" * 64, "b" * 64):
        add_hashed(database, key, bytes(32))
    database.commit()
    deduper = make_deduper(database)
    deduper.frame_index = False
    deduper.submit_results = False
    deduper.collect_test_run_pairs = collect
    assert deduper.find_potential_duplicates() == 1
    assert deduper.test_run_pairs == ({("a" * 64, "b" * 64): [100, 100]} if collect else {})


def test_test_database_copy_accepts_relative_directory_and_copies_state(database, monkeypatch):
    root = database.db_dir
    database.execute("INSERT INTO files(file_hash) VALUES (?)", ("a" * 64,))
    database.commit()
    with closing(FrameSearchState(state_path_for(root / database.db_name))) as state:
        state.record_pending("test-client", "a" * 64, "b" * 64, {"a" * 64: b"a", "b" * 64: b"b"}, 50)
    monkeypatch.chdir(root.parent)
    monkeypatch.setattr(DedupeDB, "_DB_FILE_NAME", "videohashes.sqlite")
    target = entrypoint.prepare_test_database(Path(root.name), entrypoint.TestRunMode.TEMP)
    assert target == root / "test-videohashes.sqlite"
    with closing(sqlite3.connect(target)) as copy:
        assert copy.execute("SELECT file_hash FROM files").fetchall() == [("a" * 64,)]
    with closing(FrameSearchState(state_path_for(target))) as state:
        assert state.pending_pairs("test-client") == [("a" * 64, "b" * 64, 50, None)]


def test_missing_source_preserves_previous_test_database(tmp_path, monkeypatch):
    target = tmp_path / "test-videohashes.sqlite"
    target.write_bytes(b"Previous test database")
    monkeypatch.setattr(DedupeDB, "_DB_FILE_NAME", "videohashes.sqlite")
    with pytest.raises(typer.BadParameter, match="existing videohashes"):
        entrypoint.prepare_test_database(tmp_path, entrypoint.TestRunMode.TEMP)
    assert target.read_bytes() == b"Previous test database"


@pytest.mark.parametrize("mode", [None, *entrypoint.TestRunMode])
@pytest.mark.parametrize("submit_results", [None, "true", "false"])
def test_cli_only_collects_test_run_reports(database, monkeypatch, capsys, mode, submit_results):
    submission_enabled = mode is None if submit_results is None else submit_results == "true"
    client = Mock()
    client.get_api_version.return_value = 1
    client.get_hydrus_api_version.return_value = 1
    monkeypatch.setattr(entrypoint, "create_client", Mock(return_value=client))
    monkeypatch.setattr(DedupeDB, "_db_dir", database.db_dir)
    monkeypatch.setattr(DedupeDB, "_DB_FILE_NAME", database.db_name)

    def deduplicate(deduper, **kwargs):
        assert deduper.collect_test_run_pairs is (mode is not None)
        assert deduper.submit_results is submission_enabled
        deduper._record_test_run_pair("a" * 64, "b" * 64, 100)
        deduper._record_test_run_pair("b" * 64, "a" * 64, 50)
        return 1

    monkeypatch.setattr(HydrusVideoDeduplicator, "deduplicate", deduplicate)
    assert (
        entrypoint.main(
            api_key="review-dummy",
            api_url="http://localhost:45869",
            threshold=50,
            job_count=1,
            dedup_database_dir=database.db_dir,
            test_run=mode,
            submit_results=submit_results,
            frame_index_workers=1,
            verbose=True,
        )
        == 1
    )
    message = (
        "Duplicate results will be submitted to Hydrus."
        if submission_enabled
        else "Duplicate results will NOT be submitted to Hydrus."
    )
    assert message in capsys.readouterr().out
    report = database.db_dir / "logs" / "pairs.json"
    if mode is None:
        assert not report.exists()
    else:
        assert json.loads(report.read_text()) == {
            "file_a": "a" * 64,
            "file_b": "b" * 64,
            "score_a_to_b": 100,
            "score_b_to_a": 50,
        }


def test_cli_rejects_invalid_submission_option():
    with pytest.raises(typer.BadParameter, match="either true or false"):
        entrypoint.main(api_key="review-dummy", submit_results="invalid")


@pytest.mark.parametrize("skip_hashing", [False, True])
def test_docker_entrypoint_uses_supported_cli_arguments(tmp_path, skip_hashing):
    bash = shutil.which("bash")
    if bash is None and Path("C:/Program Files/Git/bin/bash.exe").is_file():
        bash = "C:/Program Files/Git/bin/bash.exe"
    if bash is None:
        pytest.skip("Bash is required to execute the Docker entrypoint")
    wrapper = tmp_path / "python"
    wrapper.write_text('#!/usr/bin/env bash\nprintf "%s\\0" "$@" > "$HVD_CAPTURE"\n', encoding="utf-8")
    wrapper.chmod(0o755)
    capture = tmp_path / "arguments"
    env = {key: value for key, value in os.environ.items() if not key.startswith(("HYDRUS_", "HVD_"))}
    env.update(
        PATH=str(tmp_path) + os.pathsep + env["PATH"],
        HYDRUS_API_KEY="review key with 'quotes'",
        SKIP_HASHING=str(skip_hashing).lower(),
        HVD_CAPTURE=capture.as_posix(),
    )
    script = Path(__file__).parents[2] / "docker-entrypoint.sh"
    subprocess.run([bash, script.as_posix()], env=env, check=True, capture_output=True, timeout=10)
    arguments = capture.read_bytes().decode().rstrip("\0").split("\0")
    assert arguments[:2] == ["-m", "hydrusvideodeduplicator"]
    assert arguments[arguments.index("--api-key") + 1] == env["HYDRUS_API_KEY"]
    assert "--skip-hashing" not in arguments and "--no-skip-hashing" not in arguments
    if skip_hashing:
        assert arguments[arguments.index("--skip") + 1] == "hash"
    else:
        assert "--skip" not in arguments
    app = typer.Typer()
    app.command()(entrypoint.main)
    result = CliRunner().invoke(app, arguments[2:] + ["--help"])
    assert result.exit_code == 0, result.output
