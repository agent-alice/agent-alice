# Codex runtime bundles

New Alice initialization requires a complete Codex distribution containing the
primary executable and its matching `codex-code-mode-host`. Discovery follows
the native sibling layout and managed packages with `codex-package.json`,
`bin/` and `codex-resources/`. It does not search PATH for an unrelated host or
silently select direct-only tools when a distribution has no companion.

The default installer hashes both files, copies and rechecks them, writes a
pair manifest, and publishes one new directory before recording its executable
in Alice's configuration. The directory identity includes both hashes, so two
primary versions cannot overwrite each other's fixed-name host. Existing
directories with changed files or manifests are rejected. The source files can
subsequently move or disappear without invalidating the private pinned copy.

The manifest is `.alice-codex-bundle.json` beside the pinned executable. It
records the selected primary path/hash, companion path/hash, primary version,
and original distribution paths. Those paths stay in private runtime storage.
The companion does not expose a version command, so the contract binds files
from the selected distribution; it does not infer a version from a successful
`--help`. Local manifests detect drift, not malicious edits by the same user.

`--no-pin` retains the original distribution. Its reference manifest is stored
privately under a key derived from the exact primary path and hash. Both source
files remain dependencies and both are checked. No file is written into the
original distribution directory.

`RuntimeConfig.verify_binary()` checks both files when a manifest is present.
`runtime_bundle_status(config)` is a read-only interface for doctor/status:

- `verified_files`: the recorded pair still matches its file hashes;
- `unverified_legacy`: an older configuration has no pair record;
- `invalid`: a recorded pair is missing, changed or inconsistent.

These states do not claim successful tool execution. `native_verified` remains
false in this file-integrity interface. New release gates can call
`verify_runtime_bundle(config, require=True)` to require a pair, followed by
`tests/test_native_code_mode.py` to verify actual execution. That native test
uses `initialize_config` to create the private pair, then an owned App Server
and a recorded localhost Responses endpoint to call `functions.exec`, which
reads a synthetic file through its real child Code Mode host. No live model,
personal account, Desktop connection or external website is needed.

## Explicit migration of an existing installation

The old `config.json` schema is unchanged, so an earlier Alice release can still
read the migrated configuration and ignore the new pair manifest. Historical
main-only pins remain readable and explicitly unverified until migrated; a
nearby unrecorded host is never automatically adopted as a verified companion.

After fully stopping the service, callers can run the synchronous migration
function from an environment containing the updated Alice package:

```python
from pathlib import Path
from alice_codex.config import repin_codex_bundle

result = repin_codex_bundle(
    Path("/path/to/private/alice-home"),
    Path("/path/to/original-complete-distribution/codex"),
)
```

The selected primary must match the previously recorded hash and version.
The migration holds the service lock, rejects a live or uncertain control
socket and a remaining native socket, creates the complete pair and its
manifest, then atomically changes only `codex_binary` in the original JSON.
Other known and unknown JSON fields, Codex TOML, authentication, conversations,
schedules and old executable files are preserved. Configuration changed by a
concurrent writer is not overwritten. Async callers must use
`asyncio.to_thread`; CLI and shared lifecycle-maintenance integration are
separate entry-point work.

If the original primary has changed, select an archived complete distribution
with the recorded hash. This migration does not authorize a different Codex
version. Build and validate a new release for an upgrade. Older releases can
continue to point at the previous binary or the newly pinned complete pair;
rollback must retain new conversation and schedule data.

Restart the owned App Server after changing a runtime pair. Loaded threads cache
Code Mode availability; copying a missing file beside a running process does
not establish that its existing sessions have recovered. Optional browser MCP,
Node, third-party tools and Desktop capabilities retain their own dependency
and lifecycle requirements.
