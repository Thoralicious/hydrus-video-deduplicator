# Usage

## Create a Hydrus API key

1. In Hydrus, open **Manage Services → Client API** and enable the [Client API service](https://hydrusnetwork.github.io/hydrus/client_api.html#enabling_the_api).
2. Create a client API key with **permits everything** enabled. Do not set blacklist or whitelist filters for the key.
3. Copy the key when Hydrus displays it. It is a long hexadecimal string, and HVD needs this key to connect.

## Connect HVD to Hydrus

By default, HVD connects to `http://localhost:45869`. Set a different address with `--api-url`. If you use HTTPS, enable **use https** in the Hydrus Client API settings and use an `https://` URL in HVD. Certificate verification is disabled by default; for the CLI, pass `--verify-cert` with the certificate path to enable verification. In the GUI, set `REQUESTS_CA_BUNDLE` to the certificate path.

If HVD runs inside [WSL](https://learn.microsoft.com/en-us/windows/wsl/) and Hydrus runs on Windows, enable **allow non-local connections** in Hydrus' Client API settings and use the appropriate Windows host address.

## Run HVD

On Windows, launch `hydrusvideodeduplicator-windows-x86_64.exe`. On Linux or macOS, run:

```sh
python -m hydrusvideodeduplicator
```

Enter the copied API key when prompted. You can also provide it with `--api-key`.

To launch the GUI, add `--gui` and install the GUI dependencies. See all command-line options with `--help`.

HVD saves hashing progress to its database and continues from that progress on later runs. You can interrupt processing with `Ctrl+C` and resume later. This is useful when the first hashing pass would take a long time.

## Select videos with a Hydrus query

Use `--query` to choose which videos HVD processes. The query uses Hydrus tag syntax. For example, to select videos tagged `character:batman`:

```sh
python -m hydrusvideodeduplicator --query "character:batman"
```

You can combine the query with other options, such as an alternate API address:

```sh
python -m hydrusvideodeduplicator --api-url "http://localhost:45869" --query "character:batman"
```

## Test runs and stage controls

Normal runs submit duplicate relationships by default. Test runs default to **not** submitting them and save discovered pairs as JSON. Test-run reports and diagnostic logs default to `<DEDUP_DATABASE_DIR>/logs`; use `--log-path` to choose another output directory.

| Option | Database behavior |
| --- | --- |
| `--test-run new` | Start with a fresh `test-videohashes.sqlite` database. |
| `--test-run temp` | Replace `test-videohashes.sqlite` with a disposable copy of the existing `videohashes.sqlite`, then continue from its progress. |
| `--test-run save` | Use `videohashes.sqlite` and preserve progress made during the run. |

To allow a test run to submit duplicate relationships, explicitly add `--submit-results true`. Add `--submit-results false` to disable submissions during a normal run too. HVD prints whether results will be submitted before processing.

EFI query progress is checkpointed after 32 collections or 1 second since the last checkpoint, whichever comes first. Set `--efi-progress-batch-size` to change the collection limit and `--batch-interval-seconds` to change the time interval. The timer is checked after a collection finishes, so one long collection can delay its checkpoint. If interrupted, HVD may repeat completed queries since the last checkpoint, but their results remain safe to recompute.

Use `--skip search` to hash selected videos and stop before EFI index construction and duplicate search. Use `--skip hash` to search using hashes already stored in the database, without hashing new videos.

Add `--diagnostic-mode` to write phase logs and resource samples. See the [diagnostics guide](./diagnostics.md) for details.

## File access and prefetch

When the Hydrus API address is a loopback address, HVD prefers the local file path returned by Hydrus. If the path is unavailable, it falls back to downloading the file through the API. HVD retries opening a local path for up to two seconds by default before falling back; set `HVD_NAS_GRACE_SECONDS` to change that delay.

Network-share paths are used only when their exact share root is trusted. Set `HVD_TRUSTED_UNC_SHARE` to that root, for example `\\server\share`.

HVD prefetches known-size inputs with one background worker while hashing the current video. The active and prefetched input buffers share a 768 MiB budget by default. Set `HVD_INPUT_BUFFER_MIB` to change the budget, or to `0` to disable prefetch while keeping local-path access. Video decoding and other process memory are outside this input-buffer limit.

See the [FAQ](./faq.md) for more help.
