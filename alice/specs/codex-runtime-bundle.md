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
false in this file-integrity interface. Release policy 6 requires a recorded
pair and separates native protocol checks from the mandatory `native_pair`
check in `tests/test_native_code_mode.py`. Without explicit `--native`, ordinary
checks may pass but the candidate remains `promotable=false`. The native pair
check invokes the installed candidate's CLI initialization to create a private
copy, then uses an owned App Server
and a recorded localhost Responses endpoint to call `functions.exec`, which
reads a synthetic file through its real child Code Mode host. No live model,
personal account, Desktop connection or external website is needed.

## Explicit migration of an existing installation

The old `config.json` schema is unchanged, so an earlier Alice release can still
parse the migrated configuration. This is data compatibility, not approval to
run an older main-only release under the new gate. Historical
main-only pins remain readable and explicitly unverified until migrated; a
nearby unrecorded host is never automatically adopted as a verified companion.

After fully stopping the service, use the updated CLI:

```sh
alice runtime status
alice runtime repin --codex /path/to/original-complete-distribution/codex
alice doctor
```

The CLI holds the shared `offline_maintenance(config)` context throughout the
copy and atomic switch. This synchronous context holds lifecycle, bootstrap
and service locks in that order. It rejects loaded supervision even before a
control socket exists, live recorded owners and uncertain sockets. Callers must
run it off an asyncio event loop and must not reacquire those locks or call
public launchd transitions inside it. Browser installation and release switching
can use the same public boundary.

The existing standalone Python API retains its service-lock contract. Callers
must independently ensure that no supervisor can start during this lower-level
operation; prefer the CLI or the shared maintenance context for installations
with a supervisor:

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
`asyncio.to_thread`. Inside the shared context the CLI calls the private locked
implementation, avoiding a second acquisition of the same service lock.

If the original primary has changed, select an archived complete distribution
with the recorded hash. This migration does not authorize a different Codex
version. Build and validate a new release for an upgrade. Policy 6 candidates
freeze their own complete pair and bind both hashes in the manifest and verified
report. Their original distribution paths are provenance only. If runtime
configuration exists, its actual recorded pair must match both candidate hashes;
a missing, damaged or different valid host cannot reuse previous tool evidence.

Old candidates and reports remain readable, but cannot be promoted or used for
automatic rollback under policy 6. Stop and uninstall an older supervisor before
activating a new candidate. Verify and activate two compatible policy 6 candidates
to establish a usable previous, then install a new independent supervisor; its
metadata binds the release policy and host hash. If any step fails, retain the
old files and reports, keep the runtime stopped and diagnose the failure. Do not
rewrite historical reports or restore old business data. See the README's ordered
migration procedure. Schema compatibility remains a separate requirement.

Restart the owned App Server after changing a runtime pair. Loaded threads cache
Code Mode availability; copying a missing file beside a running process does
not establish that its existing sessions have recovered. Optional browser MCP,
Node, third-party tools and Desktop capabilities retain their own dependency
and lifecycle requirements.

Policy 6 additionally binds installed resource-epoch and identity-rebinding
capabilities, and collects every required artifact test. See
[release compatibility](release-compatibility.md) for the independent supervisor
and data-footprint checks. Earlier policy 5 reports remain historical evidence.
