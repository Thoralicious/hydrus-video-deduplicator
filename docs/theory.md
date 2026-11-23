# Theory

![Hydrus Video Deduplicator High Level Diagram](img/hvd_high_level_diagram.drawio.svg)

1. First, video files are perceptually hashed. These perceptual hashes are cached in a local database.

1. Then, the EFI builds an exact frame-hash index from the cached hashes. The index is built in memory for each run; completed queries and acknowledged Hydrus submissions are tracked separately so interrupted runs can resume.

1. The index searches each cached video at the selected similarity threshold. Qualifying unique pairs are marked as potential duplicates in Hydrus through the Client API when result submission is enabled.

Test runs do not submit duplicate relationships by default. Use `--test-run new` to start with a fresh temporary database or `--test-run temp` to use a disposable copy of the current database. Test runs save discovered pairs as JSON under the log directory. Add `--submit-results true` only when you intend test-run results to be sent to Hydrus; `--submit-results false` disables submissions during a normal run. `--use-vptree` selects the legacy search path.

`--skip search` completes hashing, then skips search-index construction and duplicate search. `--skip hash` uses cached hashes and proceeds with index construction and duplicate search.
