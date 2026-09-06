# Alice

Alice is a persistent agent built on the public interfaces of Codex CLI. Codex provides model execution, tools, native threads, subagents and context compaction. The Alice extension manages continuity, durable scheduling, memory provenance and controlled releases.

## Current status

The migration candidate, synthetic tests and CI workflow are in [`alice/`](alice/README.md). Production cutover is not complete. Component tests do not establish autonomous task capability.

See [installation and commands](alice/README.md), [development and verification rules](alice/specs/development.md), and [parallel task instructions](docs/parallel-tasks.md).

## Development workflow

1. Each issue defines scope, dependencies and observable acceptance criteria.
2. Each independently testable task uses its own branch and worktree, with explicit file ownership.
3. Pull requests link the issue and record the tests actually run and remaining limitations.
4. Integration verifies the installed artifact, including startup, MCP calls, interruption, restart and recovery.
5. Only a verified candidate can replace the running release. Rollback must retain new runtime data.

CI builds and verifies an installed candidate on macOS and Linux with Python 3.11 and 3.14. Every pull request runs the required `all checks passed` gate, including documentation changes. The main branch requires that GitHub Actions check and an up-to-date PR; administrators are also subject to the rule. The default CI does not call a real model. An agent reporting success is not a substitute for integration evidence.

## Data boundary

Git contains source, synthetic fixtures and project documentation. Identity files, personal memory, conversation archives, runtime logs, credentials and local deployment state remain outside the repository.

## Architecture boundary

Use Codex CLI / App Server interfaces. Alice does not duplicate the model loop or context compaction and does not require changes to Codex core.
