from concurrent.futures import ThreadPoolExecutor
import random
import time

import pytest
from hydrusvideodeduplicator import _native as vpdq


def expected(query, targets, threshold):
    return sorted(
        (key, score) for key, target in targets if (score := vpdq.matchHashBytes(query, target, 31)) >= threshold
    )


def test_random_scores_and_repeated_jobs():
    rng = random.Random(83)
    a = rng.randbytes(32)
    b = rng.randbytes(32)
    targets = [(1, a), (2, b), (3, a + b), (4, b + a + a), (5, b""), (6, rng.randbytes(32 * 7))]
    index = vpdq.ExactFrameIndex()
    for key, packed in targets:
        index.add(key, packed)
    index.seal()
    for query in [a, b, a + b, b + a + a, rng.randbytes(32 * 5), b""]:
        for cutoff in (1, 33, 50, 66, 100):
            assert sorted(index.search(query, cutoff)) == expected(query, targets, cutoff)
    assert index.stats()[0] == 14
    assert index.stats()[1] == 6


def test_hamming_cutoff_and_pigeonhole_boundary():
    base = random.Random(17).randbytes(32)

    def changed(positions):
        result = bytearray(base)
        for bit in positions:
            result[bit // 8] ^= 1 << (bit % 8)
        return bytes(result)

    positions = [part * 16 + offset for part in range(16) for offset in (0, 1)]
    targets = [(count, changed(positions[:count])) for count in (0, 1, 29, 30, 31, 32)]
    index = vpdq.ExactFrameIndex()
    for key, packed in targets:
        index.add(key, packed)
    index.seal()
    assert sorted(index.search(base, 100)) == [(n, 100) for n in (0, 1, 29, 30)]


def test_owner_dedup_query_repeats_and_truncation():
    rng = random.Random(42)
    a = rng.randbytes(32)
    b = bytes(x ^ 255 for x in a)
    targets = [(900, a * 8), (901, b), (902, a + b), (903, b"")]
    index = vpdq.ExactFrameIndex()
    for key, packed in targets:
        index.add(key, packed)
    index.seal()
    for query in (a * 2 + b * 3, a * 49 + b * 51):
        assert sorted(index.search(query, 1)) == expected(query, targets, 1)


def test_invalid_inputs_and_sealing():
    index = vpdq.ExactFrameIndex()
    with pytest.raises(RuntimeError):
        index.search(bytes(32))
    with pytest.raises(ValueError):
        index.add(1, b"x")
    index.add(1, b"")
    with pytest.raises(ValueError):
        index.add(1, bytes(32))
    index.seal()
    index.seal()
    with pytest.raises(RuntimeError):
        index.add(2, bytes(32))
    with pytest.raises(ValueError):
        index.search(b"x")
    for threshold in (0, 101):
        with pytest.raises(ValueError):
            index.search(bytes(32), threshold)
    assert index.search(b"") == []
    assert not index.cancel()


def test_same_hash_in_different_collections():
    index = vpdq.ExactFrameIndex()
    for key in range(20):
        index.add(key, bytes(32) * 2)
    index.seal()
    assert index.search(bytes(32), 100) == [(key, 100) for key in range(20)]


def test_index_cancellation_and_reuse():
    index = vpdq.ExactFrameIndex()
    frame = random.Random(2).randbytes(32)
    index.add(1, frame)
    index.seal()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(index.search, frame * 200000, 50)
        deadline = time.monotonic() + 10
        while not index.progress()[0] and not future.done() and time.monotonic() < deadline:
            time.sleep(0.001)
        assert index.progress()[0]
        assert index.cancel()
        with pytest.raises(RuntimeError, match="cancelled"):
            future.result(timeout=10)
    assert not index.progress()[0]
    assert index.search(frame, 100) == [(1, 100)]
