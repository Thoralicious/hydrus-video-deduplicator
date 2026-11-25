"""Exercise the bundled module through HVD's public hashing path."""

from importlib import metadata
from io import BytesIO
import json
from pathlib import Path

import av
import numpy as np
from PIL import Image
import pytest

from hydrusvideodeduplicator import _native
from hydrusvideodeduplicator.search_diagnostics import SearchDiagnostics
from hydrusvideodeduplicator.vpdqpy.vpdqpy import Vpdq


@pytest.mark.parametrize("threads", [1, 2])
def test_lossless_video_hashing_matches_frame_hash(threads):
    with Image.open(Path(__file__).with_name("test.png")) as source:
        image = source.convert("RGB").resize((512, 512))
    pixels = np.asarray(image)
    reference = _native.VideoHasher(1, 512, 512, threads)
    reference.hash_frame(pixels.tobytes())
    expected = reference.finish()
    assert len(expected) == 1
    buffer = BytesIO()
    with av.open(buffer, mode="w", format="matroska") as output:
        stream = output.add_stream("ffv1", rate=1)
        stream.width = stream.height = 512
        stream.pix_fmt = "bgr0"
        for _ in range(3):
            for packet in stream.encode(av.VideoFrame.from_ndarray(pixels, format="rgb24")):
                output.mux(packet)
        for packet in stream.encode():
            output.mux(packet)
    actual = Vpdq.computeHash(buffer.getvalue(), num_threads=threads)
    assert actual == expected


def test_package_metadata_has_no_external_accelerator_dependency():
    requirements = metadata.requires("hydrusvideodeduplicator") or []
    assert not any("hvdaccelerators" in requirement.lower() for requirement in requirements)
    assert _native.__source_commit__ == "1851cfbf4a59926b9b5e7f4fe7793effc668db9e"


def test_diagnostics_identify_the_bundled_native_revision(tmp_path):
    diagnostics = SearchDiagnostics(tmp_path, groups={"time"}, trace_limit=10)
    diagnostics.close()
    session = json.loads(diagnostics.path.read_text().splitlines()[0])
    assert session["accelerator_version"] == "bundled-" + _native.__source_commit__[:12]
