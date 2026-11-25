from concurrent.futures import ThreadPoolExecutor
import random
import time
import pytest
from hydrusvideodeduplicator import _native as vpdq


def oracle(query, targets, cutoff):
    return [(key, score) for key, data in targets if (score := vpdq.matchHashBytes(query, data, 31)) >= cutoff]


@pytest.mark.parametrize("workers", [1, 2, 4, 14])
def test_randomized_directional_scores_and_counters(workers):
    rng = random.Random(741)
    pool = [rng.randbytes(32) for _ in range(40)]
    targets = [(2**63 + i, b"".join(rng.choices(pool, k=rng.randrange(0, 50)))) for i in range(35)]
    index = vpdq.ExactFrameIndex()
    for key, data in targets:
        index.add(key, data)
    index.seal()
    for length in (0, 1, 15, 16, 17, 31, 65, 193):
        query = b"".join(rng.choices(pool, k=length))
        for cutoff in (1, 49, 50, 51, 99, 100):
            serial = index.search(query, cutoff, 1)
            counters = index.progress()[1:]
            assert serial == oracle(query, targets, cutoff)
            assert index.search(query, cutoff, workers) == serial
            assert index.progress()[1:] == counters
            assert index.parallel_stats()[0] == 0


@pytest.mark.parametrize("workers", [1, 2, 4, 14])
def test_boundary_repeated_frames_and_rounding(workers):
    base = bytes(32)

    def change(n):
        bits = [part * 16 + bit for bit in (0, 1) for part in range(16)]
        return sum(1 << b for b in bits[:n]).to_bytes(32, "little")

    targets = [(n, change(n) * 3) for n in (0, 29, 30, 31, 32)]
    index = vpdq.ExactFrameIndex()
    for key, data in targets:
        index.add(key, data)
    index.seal()
    assert index.search(base * 137, 100, workers) == [(0, 100), (29, 100), (30, 100)]
    for cutoff in (49, 50, 51):
        query = base * 49 + bytes([255]) * 32 * 51
        assert index.search(query, cutoff, workers) == oracle(query, targets, cutoff)


def test_validation_reuse_and_storage():
    index = vpdq.ExactFrameIndex()
    index.add(1, bytes(32))
    index.seal()
    for workers in (0, -2, 257):
        with pytest.raises(ValueError):
            index.search(bytes(32), 50, workers)
    index.search(bytes(32) * 256, 50, 4)
    size = index.stats()[2]
    assert index.storage_stats()["native_capacity_bytes"] == size
    assert index.parallel_stats()[1] > 0
    index.release_search_scratch()
    assert index.parallel_stats() == [0, 0]
    assert index.stats()[2] < size
    assert index.search(bytes(32), workers=2) == [(1, 100)]


def test_parallel_cancellation_and_reuse():
    rng = random.Random(91)
    index = vpdq.ExactFrameIndex()
    for key in range(32):
        index.add(key, rng.randbytes(32 * 300))
    index.seal()
    query = rng.randbytes(32 * 100000)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(index.search, query, 50, 4)
        deadline = time.monotonic() + 10
        while not index.progress()[0] and not future.done() and time.monotonic() < deadline:
            time.sleep(0.001)
        assert index.progress()[0]
        assert index.cancel()
        with pytest.raises(RuntimeError, match="cancelled"):
            future.result(timeout=10)
    assert index.progress()[0] == 0
    for workers in (4, 1, 2):
        assert index.search(b"", 50, workers) == []
        assert index.search(bytes(32) * 65, 50, workers) == []


def test_parallel_work_is_distributed():
    rng = random.Random(46)
    index = vpdq.ExactFrameIndex()
    index.add(1, rng.randbytes(32 * 1000))
    index.seal()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(index.search, rng.randbytes(32 * 10000), 50, 4)
        deadline = time.monotonic() + 10
        while index.parallel_stats()[0] != 4 and not future.done() and time.monotonic() < deadline:
            time.sleep(0.001)
        assert index.parallel_stats()[0] == 4
        assert future.result(timeout=30) == []
    assert index.progress()[2] == 10000


def test_real_video_fixtures():
    import json
    from pathlib import Path

    fixtures = json.loads((Path(__file__).parent / "fixtures" / "video-hashes.json").read_text())
    targets = [(key, bytes.fromhex(data)) for key, data in enumerate(fixtures.values())]
    index = vpdq.ExactFrameIndex()
    for key, data in targets:
        index.add(key, data)
    index.seal()
    for _, query in targets:
        query = query * 9
        reference = oracle(query, targets, 50)
        for workers in (1, 2, 4, 14):
            assert index.search(query, 50, workers) == reference
