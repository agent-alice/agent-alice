# Runtime-bound skills (F06 / F09)

Task: installed skills must execute the package serving Alice's MCP connection,
including after a candidate restart or rollback, without guessing a system
Python or requiring the source checkout's test fixtures.

Baseline: the three templates invoked bare `python -m alice_codex...`; the
learning template referenced source-only fixtures. Merely discovering these
skills did not prove their commands ran in an installed workspace.

`runtime_info` is a read-only MCP tool reporting its own process interpreter,
package directory, workspace, explicit command argument arrays and bundled
synthetic evaluation inputs. The interpreter path retains the virtualenv link;
commands and the managed MCP launch use `-I` to exclude the working directory
and `PYTHONPATH`. It neither
selects nor activates a release, imports source-tree code, starts a model turn,
nor reads authentication. Re-query after MCP reconnect/restart; do not cache a
previous candidate's command paths. Existing trusted MCP startup and release
rebinding remain responsible for which process is serving the connection.

The existing 20-case task/oracle/correction files move byte-for-byte into package
data, remaining separate files and one canonical source. Tests use those same
files. The fixed suite is public regression evidence and does not establish
fresh-task reuse or compact continuity. Private cases/reports remain outside Git.

Scope also includes cleaned, on-demand research and runtime observation
diagnosis skills. Historical private material is not copied into package data.
Zhihu account authentication, research endpoints and publication adapters remain
separate migration work; no publisher is added by these changes.

Acceptance: real stdio MCP catalog/call, actual returned command arrays in a
clean installed workspace, failure for a commented draft, success for a clean
draft, the bundled 20-case evaluator, and `-I` isolation from a shadow package.
Existing lifecycle gates still cover shutdown and candidate rebinding. No live
model or account is needed. Template installation continues to preserve local
edits; updating known old factory copies requires a separate reviewed migration.
