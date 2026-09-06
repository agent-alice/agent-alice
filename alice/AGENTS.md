# Alice extension development

Follow the [public development rules](specs/development.md). While this extension is developed inside another repository, also follow that repository's applicable instructions.

- Use Codex CLI / App Server public interfaces; do not modify Codex core or duplicate its model loop and context compaction.
- Keep identity, memory, credentials, raw runtime logs and deployment state outside source control. Use only synthetic fixtures and owned test processes.
- Use an issue, an independent branch/worktree and explicit file ownership for each independently testable task; coordinate shared interfaces before editing.
- Validate behavior through the real entry point and installed artifact. Keep unit, controlled native, artifact and live-model evidence distinct; missing, skipped or failed required checks block promotion.
- Keep active runtime and development separate. Preserve a verified previous release, compatible schemas and all new records during rollback.
- Report the checks actually run and what remains unverified. A model-authored passed field, a test file or one green component does not establish release readiness or learning capability.
- The current local service targets macOS and Linux. Build migration in reviewable stages; no single stage claims final cutover.
