# Deterministic identity loading — issue 19

The host supplies a bounded SOUL/USER/MEMORY snapshot before inference. A tool
failure must not erase Alice's identity. This change preserves the native root,
source bytes and archives; it does not modify Codex, implement a model loop or
reconstruct compressed conversation history. Only synthetic fixtures are used.

## Sources and trust

The fixed source set is SOUL.md (16 KiB), USER.md (16 KiB) and memory/MEMORY.md
(32 KiB), at most 64 KiB combined and 96 KiB after serialization. UTF-8, nonempty
regular files, unchanged directory/file identities and complete content are
required. Missing, linked, invalid, changing or oversized sources fail explicitly;
nothing is silently truncated. Content hashes determine a stable revision, while
mtime is diagnostic metadata only. Original logs and daily archives stay outside
the bundle.

SOUL supplies subordinate personality/style. USER and MEMORY are delimited
reference data, potentially historical, incorrect or superseded. Neither supplies
new authority, tasks, publication permission, spending permission or a new Goal.
The latest complete bundle supersedes older snapshot data; it does not erase
history. File reads remain available for editing and source investigation.

New empty installations receive generic, clearly unpopulated identity templates.
Template installation never replaces an existing file, including an imported or
manually edited identity. Legacy import during `alice init --source` precedes
these defaults. A later import into an already initialized workspace can report
ordinary source/workspace conflicts rather than overwriting a default or edit.

## Canonical context and native hook delivery

Service startup builds the bundle before spawning Codex, writes the managed
private developer instructions and binds hooks to that candidate's installed
Python. New roots and confirmed notLoaded same-ID resumes also receive the bundle
through public developerInstructions. Cached client absence is not proof of a
cold thread. Loaded/subscribed resume overrides can be ignored, and even a cold
resume can retain the old canonical content in reconstructed history until the
next compaction. Neither API success is recorded as proof of replacement.

Native SessionStart(startup/resume/compact) and UserPromptSubmit supply the latest
version before direct TUI input, including a cold resume's first request. The
canonical bundle remains the fallback independent of a file-reading tool or hook
success. Native compaction rebuilds canonical context; the compact hook then
supplies current dynamic records. Earlier canonical versions may remain in
history until compaction: this is versioned supersession, not in-place rewriting.

Only exact generated user-config handlers are trusted using the native
hooks/list key and currentHash. Third-party hooks, plugins, MCP entries and
comments remain outside Alice's authorization. Trust must be established before
any thread is loaded: updating the trust file does not hot-reload an already
loaded thread's captured hooks. The active service is not changed by installing
this source. Native hook spilling is disabled only for events that support
additionalContext; Stop cannot emit that field.

Handled source errors return an explicit stop. Native hook launch errors,
missing Python, timeouts and untrusted handlers can fail open, so hooks are not
the sole source of baseline identity. No claim of universal hook fail-closed
behavior is made.

## Receipts and bounded deduplication

Private state/identity-delivery files store per-session generation, source
metadata and pending delivery nonce. Producing stdout or receiving an injection
RPC receipt is not an ACK. A native Stop must expose a nonempty assistant result;
a bounded append-only transcript scan must additionally find that nonce followed
by the same Stop turn's assistant message. Resume and compaction invalidate the
old generation. A late Stop, failed provider request or cancelled hook cannot
ACK a new window. Unchanged, acknowledged revisions are not appended each turn.
SessionStart and its following UserPromptSubmit can conservatively duplicate one
bundle when delivery is still unconfirmed.

This additional evidence uses the native legacy rollout format actually tested
with the pinned pair. It reads at most 8 MiB since the pending cursor, at most
512 KiB per line, and never stores raw transcript bodies in the ledger. Missing
transcripts, new formats, changed files and exceeded scan limits remain uncertain
and cause a later resend, never an invented ACK. Other native history modes need
separate verification; this is not a native-history import or compaction engine.

A normal Stop only acknowledges its existing input. If a normal turn changes a
preference, the next user input automatically receives the new bundle without
an extra response. Goal continuations are native ResponseItem inputs and do not
run UserPromptSubmit. A changed snapshot can therefore use a narrowly bounded
root-only Stop injection only when public goal/get explicitly reports an active
goal with tokenBudget=null. All finite-budget goals are excluded from Stop
injection, even when their last reported usage is below the limit.
Public thread/inject_items during an active turn can produce another sampling
and assistant response in that same turn. It is not a cost-free next-turn update.
A durable turn ID fence permits at most one attempt per turn, even after timeout,
restart or another MEMORY update in the continuation. Unknown or inactive goal
state never authorizes injection. Native V2 children reject this direct-input
API; managed worker roots receive fresh canonical bundles, and native children
retain their inherited context rather than bypassing native ownership rules.

The native goal usage reported during Stop can lag current sampling until
turn/completed. For that reason finite-budget goals refresh only at the next
UserPromptSubmit or compact/startup/resume event. During uninterrupted autonomous
execution, their supplied memory can lag file changes until that boundary. The
latest supplied revision identifies this limitation; reported active status is
not treated as an instantaneous spending guarantee. No second token-accounting
engine is introduced here.

## Configuration and release compatibility

RuntimeConfig JSON and the scheduler/memory database schema do not gain required
fields. state/identity-runtime.json version 1 records hook_compat_version 1,
installed Python, workspace/config paths, bundle hashes and native hook hashes.
Unsupported versions fail without resetting the file. The public module constant
IDENTITY_HOOK_COMPAT_VERSION is the corresponding release capability.

Every candidate startup must rebind only its owned hooks to its own interpreter
and obtain current native trust hashes before loading threads. A previous release
that does not know these hooks can retain a broken newer interpreter in its
configuration. Such a previous release is not a valid unattended rollback target:
the release integration must require this rebinding capability or provide an
independently verified configuration recovery path. The identity change does not
weaken release gates or silently modify launchers, bootstrap or release code.

## Acceptance and current state

Unit coverage includes bounded/coherent reads, record trust, revisions, unknown
receipts, late Stop, compaction fences, Goal eligibility, one attempt per turn,
config preservation, exact hook authorization and non-destructive templates.
Recorded localhost Responses requests from the actual binary pair cover new
roots, loaded/cold resume negative controls, real compaction, direct remote TUI,
revision deduplication, provider failure/retry and a real code-mode tool failure.
Goal tests count both provider requests and native turn/completed events: equal
request counts alone cannot prove there was no extra model sampling.

Reports distinguish source tests, installed-candidate tests and protocol research.
A prior successful exploratory result can be superseded by a stricter negative
control; the original evidence is retained and its interpretation corrected.
Current status: implementation and candidate validation in progress. Nothing has
been deployed; no production identity, memory, thread or service has been changed.
