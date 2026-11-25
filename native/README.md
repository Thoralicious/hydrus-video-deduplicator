# Bundled native algorithms

Selected source from [Thoralicious/hvdaccelerators](https://github.com/Thoralicious/hvdaccelerators),
commit `1851cfbf4a59926b9b5e7f4fe7793effc668db9e` (the revision previously pinned by this fork).

- `bindings.cpp`: PDQ frame hashing, video hashing, packed-hash conversion, and directional matching.
- `exact_frame_index.h`: exact frame indexing, parallel queries, cancellation, progress, and storage diagnostics.
- `pdq/` and `vpdq/`: the hashing implementation and its build files.

The unused standalone `ParallelMatcher` binding and header are omitted. The imported C++ namespace is
`hvd_native`, and the Python extension is installed as `hydrusvideodeduplicator._native`.
Algorithms and packed hash formats are preserved. Existing hash databases remain compatible.

The binding and index are covered by `LICENSE-hvdaccelerators.txt` (MIT). The PDQ and VPDQ licenses
are retained in their source directories. All three license texts are also installed in the Python
package's `_licenses` directory for binary distributions.

Native tests and their known image/video-hash fixtures were imported from the same revision into
`tests/native`, with imports adjusted to use the bundled extension.
