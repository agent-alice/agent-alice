# Release compatibility and acceptance

Release policy 6 binds the installed wheel, dependency environment, fixed Codex
primary/Code Mode host pair, source snapshot and observed verification results.
Policy 4 and 5 manifests and their reports remain readable historical evidence;
they cannot authorize activation or rollback under policy 6. Rebuild and verify
a new candidate without modifying the old files or reports.

## Capabilities beyond database versions

Matching SQLite schemas is necessary but does not establish that an older
service can safely interpret newer runtime state. Two installed capabilities
are therefore recorded in candidate metadata and the verification report:

| Installed constant | Metadata field | Required native acceptance group |
| --- | --- | --- |
| `service.RESOURCE_EPOCH_CAPABILITY` | `resource_epoch_capability` | `native_resource_epoch` |
| `identity.IDENTITY_HOOK_COMPAT_VERSION` | `identity_hook_compat_version` | `native_identity` |

Missing constants in historical packages mean capability 0. The supported new
value is integer 1; booleans, unknown versions and malformed declarations are
rejected. The gate imports these constants in the actual installed candidate
with its private interpreter. It rechecks installed bytes and declarations
before trusting the report. A declaration alone does not prove behavior: each
capability also requires its own nonempty native acceptance group using the same
installed candidate and fixed runtime pair. Failed, skipped or empty groups
block promotion.

`runtime.json` retains its existing version. If it contains `resource_epochs`
or a server epoch reference, an epoch-unaware candidate is rejected, including
when the SQLite resource schema is 2 and `server` is null. A capable candidate
must pass Service's shared pure `validate_resource_epoch_journal`; release code
does not maintain a second parser. A structurally valid `prepared` record passes
this format check. Actual Service recovery must still refuse a blind respawn
until its uncertain prior launch has been reconciled. Unknown additive fields
follow the owner's parser contract rather than being discarded by a release.

Identity compatibility is required if the private identity runtime manifest,
delivery directory or precisely recognized Alice hooks exist. Inspecting hooks
also covers a crash after initialization writes the hooks but before Service
writes its ready manifest. The check uses the identity module's shared pure
recognizer and manifest validator. Unrelated third-party hooks are not adopted.
An old interpreter path may be absent or damaged: a capable replacement is
allowed to rebind the owned hooks to itself. The gate never runs that old path.

Activation, checked current/start, manual rollback and automatic fallback use
the same data compatibility checks. An incompatible or malformed record blocks
the code switch and leaves current pointers and business data intact. These
guards do not restore a snapshot or claim compatibility with all future formats.

## Independent supervisor

The installed supervisor is an independent environment and must understand a
candidate's capabilities even before that candidate writes its first new
record. Its actual installed declarations must match its recorded metadata and
be at least the candidate's declarations; the release policy and fixed runtime
pair must also match. Safe cleanup of a known owned orphan may precede the data
check. Starting a new service or switching a pointer may not.

Before adopting policy 6, stop and uninstall an older supervisor. Verify and
activate two compatible policy 6 candidates to establish a usable previous,
then install the independent supervisor from the compatible current candidate.
If a step fails, preserve both versions and data, keep the runtime stopped and
diagnose the failure. Stop persists an autonomy pause; start restores the
process and existing threads, while explicit `alice resume` resumes automatic
dispatch. Job enablement remains recorded. Abnormal recovery never silently
clears that pause.

## Complete artifact collection

The required artifact check collects all tests marked `artifact`, excluding
`native` and `live`. The core installed smoke file must still exist. A failing
artifact test in a second file therefore blocks promotion instead of being
silently omitted. Native epoch and identity files run in their own required
groups exactly once; ordinary native tests exclude those files and the existing
Code Mode pair group. All five runtime-related environment values come from the
candidate's private pair, including both file hashes. No paid model is required.

Compatibility regression tests use real synthetic installed wheels to exercise
declaration probing, report binding and rejected switches. They do not claim
that the synthetic native fixtures prove Service behavior. The separate actual
native tests exercise owned processes against recorded localhost Responses;
only a full candidate run establishes evidence for the combined source snapshot.
