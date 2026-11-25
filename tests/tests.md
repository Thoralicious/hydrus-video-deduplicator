# testdb

testdb is a submodule that contains the Hydrus DB and videos for testing purposes.

It is a submodule to avoid bloating this repo with large media files used for testing.

## Instructions

To checkout the submodule:

```sh
git submodule update --init --recursive
```

To run tests:

```sh
hatch run test:all
```

To run only specific tests, e.g., vcr:

```sh
hatch run test:vcr
```

See [pyproject.toml](../pyproject.toml) test.scripts for full list of test groups.

## Bundled native tests

`hatch run test:native` checks PDQ hashing, lossless-video hashing, packed-hash conversions,
and serial/parallel EFI results, counters, cancellation, and reuse. Its small image and cached-hash
fixtures live in `tests/native`; these tests do not need the Hydrus `testdb` submodule.

## Result-submission and search regression tests

`tests/unit_tests/test_regressions.py` uses disposable databases and mocked Hydrus submissions. Run it without the media/VCR fixtures:

```sh
hatch run test:regressions
```

Or run it directly in an environment with the project and its test dependencies installed:

```sh
python -m pytest tests/unit_tests/test_regressions.py
```

Coverage includes:

- Normal-run and test-run submission defaults, explicit overrides, invalid values, and CLI messages.
- Test-run JSON reports and disabled in-memory pair collection during normal runs.
- EFI pending-pair thresholds, directional reports, and retries after failed submissions.
- Legacy incremental-search cancellation, submission failures, and later submission after a dry run.
- Relative-path test-database copies and preservation of an existing test database when its source is missing.
- Docker entrypoint argument parsing with and without hashing enabled (requires Bash).

---

HVD DEV ONLY:

To update the submodule to main:

```sh
git submodule foreach git pull origin main
```
