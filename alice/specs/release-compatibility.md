# Release compatibility and acceptance

Release policy 6 binds the installed wheel, dependency environment, fixed Codex
primary/Code Mode host pair, source snapshot and observed verification results.
Policy 4 and 5 manifests and their reports remain readable historical evidence;
they cannot authorize activation or rollback under policy 6. Rebuild and verify
a new candidate without modifying the old files or reports.

## Capabilities beyond database versions

Matching SQLite schemas is necessary but does not establish that an older
service can safely interpret newer runtime state. Four installed capabilities
are therefore recorded in candidate metadata and the verification report:

| Installed constant | Metadata field | Required native acceptance group |
| --- | --- | --- |
| `service.RESOURCE_EPOCH_CAPABILITY` | `resource_epoch_capability` | `native_resource_epoch` |
| `identity.IDENTITY_HOOK_COMPAT_VERSION` | `identity_hook_compat_version` | `native_identity` |
| `summary_partitions.SUMMARY_COMMIT_SCHEMA` | `summary_commit_schema` | `native_summary` for schema 2 |
| `heartbeat.HEARTBEAT_SOURCES_CONFIG_VERSION` | `heartbeat_sources_config_version` | `native_heartbeat_sources` |

Missing constants in historical packages mean capability 0. The supported new
value is integer 1 for epochs, identity and heartbeat; summary declarations may
be integer 0, 1 or 2. Booleans, unknown versions and malformed declarations are
rejected. The gate imports these constants in the actual installed candidate
with its private interpreter. It rechecks installed bytes and declarations
before trusting the report. A declaration alone does not prove behavior: each
capability also requires its own nonempty native acceptance group using the same
installed candidate and fixed runtime pair. Failed, skipped or empty groups
block promotion.

The summary and heartbeat fields must be explicitly recorded in new manifests
and bound reports, including when actual probes return 0. Earlier policy-6
reports without them remain historical evidence and require a newly staged
candidate; the verifier does not rewrite them or infer legacy summary support.
An explicitly probed 0 can pass with no relevant data/configuration footprint
and a compatible bootstrap. This is format compatibility rather than a fixed
minimum feature list. The migration's eventual current and previous candidates
must both implement summary schema 2 and heartbeat configuration version 1.

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

Every `memory-state/commits/*.json` record is checked with the summary owner's
pure `validate_summary_commit_header`, passing the candidate's actual declared
schema explicitly. Capability 0 therefore cannot read even a schema-1 commit;
schema-2 partitioning, pending and committed records all require capability 2.
This header check does not claim to validate a partition DAG or recover it.
Those behaviors are exercised by the required structural and native gates.

The presence of the raw `heartbeat_sources` key in `config.json` requires
heartbeat capability 1, including null or an empty source list. The shared
`parse_heartbeat_sources` then validates its value without network or mutation.
Removing the field to permit an older rollback is not part of the release
operation. Invalid headers, configuration or symbolic links are rejected.

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
silently omitted. Native epoch, identity, summary and heartbeat files run in their own required
groups exactly once; ordinary native tests exclude those files and the existing
Code Mode pair group. All five runtime-related environment values come from the
candidate's private pair, including both file hashes. No paid model is required.

For summary schema 2 the `summary_boundary` check also invokes the candidate's
interpreter with `-I` and the absolute structural probe path, without `--case`.
It requires all three full boundaries (one 17 MiB record, multiple records over
16 MiB, and 10,001 short records), no debug-only case, and all eight named
provenance/recovery regression guards. The JSON flags, typed guard count and
individual outcomes are validated even if the subprocess exits 0. This is a
structural check without native/model execution; the separate installed native
summary test covers child execution and restart. Missing required files, empty
groups, partial probes, skipped tests and failed commands all block promotion.

Compatibility regression tests use real synthetic installed wheels to exercise
declaration probing, report binding and rejected switches. They do not claim
that the synthetic native fixtures prove Service behavior. The separate actual
native tests exercise owned processes against recorded localhost Responses;
only a full candidate run establishes evidence for the combined source snapshot.
