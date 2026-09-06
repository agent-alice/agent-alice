# Task policy and host heartbeat runtime

This backend implements the policy interfaces described in
[task-policy-interfaces.md](task-policy-interfaces.md). It uses the existing
`TaskPolicy.decide` engine, rather than a second budget implementation.

## Admission and recovery

An operator can assign all five limits to a stable named task. A configured
default applies to explicit named work when it first submits input; it does not
automatically apply to `main`, `new`, `summary:*`, `scheduled:*`, or recurring
summary plans. An explicit policy on `main` is an operator decision. These are
limits on a bounded task, not a lifetime allowance for hourly/daily/weekly/monthly
maintenance. The default configuration remains `null`.

The first admitted input starts the clock. Before calling native `turn/start` or
queue input, one SQLite transaction saves the attempt, input fingerprint, original
root ID and cumulative usage. Definite pause, busy and dependency deferrals do not
consume an attempt. Retries with the original request ID reuse its receipt; a new
ID does not reset accumulated usage. Updating limits preserves usage and all
receipts, and does not clear a pause.

Attempts count Alice-admitted host inputs, not every internal model iteration.
Both `task_policy_status` and MCP `task_status` report
`enforcement_scope: "alice_admission"` alongside `target`, `policy`, `usage` and
`decision`. A null policy remains unconfigured even when that capability field is
present. Setter receipts stay immutable and do not contain this status field.
Another native client can send input without crossing this host's transaction;
the public protocol cannot atomically intercept that caller's RPC. Use Alice's
admission entry point for budgeted work. Observed new native turns re-arm an
expired deadline's stop obligation, but this cannot undo effects before detection.

SQLite is written before runtime.json and native input. A crash in that gap leaves
an unresolved charged receipt; startup reconstructs only an Alice reconciliation
record bound to its original root. It never constructs user conversation history,
creates a replacement root, or repeats input. Previously running attempts become
unknown after server recovery. Missing or mismatched root ownership fails closed.

Native completion means the executor stopped. It does not prove an external goal
was achieved or the source was unchanged. Completed turns therefore leave policy
outcome `unknown`; confirmed interrupted/failed turns record `failed`. Independent
host reconciliation may use `Store.finish_task_attempt` with evidence for a
positive outcome. There is deliberately no MCP/control endpoint accepting a
model-authored `passed`, `unchanged` or completion receipt. Application-specific
business reconciliation is still required; changing limits cannot resolve an
unknown effect.

An independent watcher evaluates elapsed limits even if the scheduler is waiting
for an RPC. At expiry it first persists pause and the pending native-stop
obligation, then invokes the owned root's public stopTree operation. A busy final
legal attempt is not stopped merely because its attempt balance is zero. A stop
which cannot finish within five seconds fails the service closed and invokes the
existing owned-server shutdown path. This is an eventual stop protocol with native
and OS latency, not a hard real-time deadline or reversal of external effects.

Resume checks policy and pause revisions after native awaits. A queued input or
paused Goal can resume under a policy only when it belongs to a current, confirmed
charged attempt. An unused policy or a terminal/unknown attempt cannot activate
old native work without admission. Explicitly resuming an empty paused root can
clear its flag without consuming an attempt.

## Trusted heartbeat observations

Host code registers `CollectionSpec` with
`Service.register_heartbeat_source(target, spec, wait_seconds=...)`. The positive
finite interval is explicit and separate from a task's lifetime budget. There is
no URL discovery from prompts, model observations or HEARTBEAT.md, and no source
registration through CLI/MCP. Production source configuration is an integration
dependency; registrations must be restored explicitly after process restart.

The host directly collects the registered HTTP collection. A known receipt requires
complete pagination, required fields, successful HTTP fetches and the collector's
freshness checks. Its scope hash binds URL/query, subject, collection, metrics,
credential environment names, authentication-context version and freshness policy.
Only hashes and bounded metadata are persisted, not credentials, raw bodies or
URLs. Hosts must change the authentication-context version when account context
changes. Known describes the fetched IDs and requested metrics; it does not prove
all account content, origin revalidation, or a business goal.

Receipts have stable IDs and observed timestamps. Duplicate IDs cannot change
content, and older/equal timestamps cannot advance the accepted watermark or
extend a wait. Latest observation and last good observation are distinct. Unknown
never means unchanged. Same-scope known snapshots compare sorted IDs and required
metrics, excluding poll timestamps. New scope establishes a new baseline.

The host rechecks sources at a finite interval even while the task is paused or
policy/native ambiguity prevents admission. Heartbeat dispatch compares the latest
known snapshot to the one actually consumed by an admitted input. Thus identical
new receipts and HEARTBEAT.md edits do not trigger duplicate work; a pending source
change survives a pause and subsequent unchanged rechecks. Consumption and the
runtime intent are saved together before native input. Rejected/older observations
cannot borrow a previously known snapshot to authorize new work. New evidence
does not clear a pause, unknown outcome, or resource/task limit.

Without an explicitly registered external source the observation is
`unconfigured`. The existing general periodic self-review heartbeat continues
through ordinary resource, concurrency, pause and explicit task-policy admission;
it uses no external unchanged optimization. This path does not acquire an implicit
lifetime budget on the permanent main thread. A registered but unavailable source
blocks that source-dependent monitor and keeps checking, without claiming success.

## Data compatibility and verification

The schedule database remains schema version 1. New versioned settings namespaces
hold policy state, immutable request/attempt receipts and host observations. Runtime
state retains version 1 and adds policy stop obligations and consumed host receipts.
Heartbeat observations are append-only indexed rows with a small per-target
snapshot. Routine polling reads a fixed number of rows and writes two rows for a
new observation. Full startup auditing and disk usage still grow with retained
history; this change does not introduce pruning or discard evidence.
No existing jobs, events, native IDs or raw conversation data are replaced. Invalid
or future versions in these namespaces fail closed and preserve the data.

Older code can retain these rows but does not enforce the new contracts. Rollback
therefore requires paused automatic work and owned-server cleanup; do not resume
budgeted/monitored tasks under an older backend without an explicit compatibility
decision. Never restore an older database/runtime snapshot over new records.

Unit tests use synthetic transports, clocks and temporary stores. Host collector
tests use isolated HTTP fixtures. Native policy tests use the installed public
Codex binary with isolated HOME/CODEX_HOME and localhost Responses fixtures; they
exercise deadline interruption, persisted pause/usage and completed-to-unknown
recovery through the CLI. These checks do not execute a paid model or establish
business success. Candidate promotion still requires the same-artifact gate and
the integration owner's separate deployment decision.
