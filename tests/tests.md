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

## Planned test: result-submission defaults

Add this case when the CLI or Hydrus submission tests are next expanded. Use a mocked submission callback so verifying the normal-run default cannot send duplicate relationships to a live Hydrus instance.

- With `--submit-results` omitted in a normal run, verify submissions are enabled and the CLI says duplicate results will be submitted.
- With it omitted for each `--test-run` mode (`new`, `temp`, and `save`), verify submissions are disabled, the CLI says duplicate results will NOT be submitted, and test-run JSON output is still written.
- Verify explicit `--submit-results true` and `--submit-results false` override both defaults, and reject any other value.

---

HVD DEV ONLY:

To update the submodule to main:

```sh
git submodule foreach git pull origin main
```
