# Persistent task policy interfaces

The configuration and CLI reuse `resources.TaskPolicy`; they do not create a second policy engine. Service/store enforcement is a separate required integration. Until that integration is present, the CLI endpoints fail explicitly as unsupported. This document is not evidence that automatic work is bounded.

`RuntimeConfig.task_policy` is an optional object with five explicit limits. Older version 1 files without the field remain readable with `None`, without rewriting the file or enabling a policy. An object must contain exactly `max_elapsed_seconds`, `max_attempts`, `max_retries`, `retry_wait_seconds`, and `unchanged_wait_seconds`. Time and wait limits must be positive finite numbers, attempts a positive integer, and retries a nonnegative integer.

The operator can inspect a target with `alice task-policy status --target main`. Explicit initial configuration or extension uses the service's single writer:

```sh
alice task-policy set --target research --request-id policy-research-1 \
  --max-elapsed-seconds 1200 --max-attempts 6 --max-retries 1 \
  --retry-wait-seconds 30 --unchanged-wait-seconds 120
```

These values are an example, not automatic defaults. The CLI sends `task_policy_set` with `{target, request_id, policy}`; status uses `task_policy_status` with `{target}`. The service must preserve existing usage and clock watermarks when extending a policy, bind usage to the stable target, and reject conflicting replay. Changing a request ID must not reset consumption. Limits do not grant spending or publishing permission, and do not change virtual accounting.

MCP `task_status` remains read-only and returns policy, usage, and the host's decision alongside native history. There is no unattended tool to increase limits. Model-authored completion is a claim until an independent result check confirms it. Native turn completion alone does not establish business success. Resume cannot reset limits, clear unknown results, or override quota protection.

Collection freshness is separately configurable with `alice collect ... --max-age-seconds 60`. The result keeps the raw observation and marks stale counts unknown. Omitting the option preserves the prior snapshot contract; it does not assert that remote data is current.

Local entry-point tests cover legacy configuration without mutation, invalid limits preserving the saved file, invalid CLI limits rejected before contacting a service, and a real localhost cached response that changes from a complete zero count to unknown under an explicit freshness limit. Durable admission, deadline interruption, restarts and waiting require the service integration and its final installed-artifact tests.
