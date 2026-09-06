# Host heartbeat source configuration

Alice restores explicitly configured collection sources when the Service starts. It reuses CollectionSpec, HostHeartbeatAdapter and the existing durable heartbeat receipts and admission checks. A persisted receipt alone does not authorize contacting a source again. No MCP or model operation accepts source configuration, trusted receipts or collection verdicts.

## Private operator configuration

The optional `heartbeat_sources` field in Alice's private `config.json` has this format:

```json
{
  "heartbeat_sources": {
    "version": 1,
    "sources": [
      {
        "target": "monitor",
        "wait_seconds": 120,
        "spec": {
          "source_id": "example-answers",
          "url": "https://example.invalid/answers?filter=published",
          "subject": "example-member",
          "collection": "answers",
          "auth_context_version": "account-v1",
          "max_age_seconds": 60,
          "required_metrics": ["voteup_count", "comment_count"],
          "header_env": [["Authorization", "EXAMPLE_AUTH_HEADER"]]
        }
      }
    ]
  }
}
```

This is a synthetic example, not an enabled task. Each stable target has at most one source. `main` is permitted; `new`, `summary:` and `scheduled:` targets are not. `wait_seconds` must be an explicit finite positive number, and is independent of TaskPolicy limits. Unknown versions or fields, duplicate targets, malformed containers and invalid values reject the entire configuration. Alice does not skip damaged entries and silently restore ordinary self-review.

Missing `heartbeat_sources` and JSON `null` both mean no configured sources. RuntimeConfig.save omits the field when its value is None, so saving an unconfigured runtime does not introduce an unsupported field for older code. An explicit `{ "version": 1, "sources": [] }` remains present when saved. Reading configuration does not rewrite it.

`spec` uses the existing CollectionSpec fields. Its required fields are source_id, url, subject, collection, auth_context_version and max_age_seconds. Optional required_metrics and header_env are JSON arrays; headers use two-element arrays, not an object. Parsing copies them into immutable tuples. Optional max_pages, max_items, request_timeout and total_seconds retain the collector's existing defaults and bounds. The current collector expects its supported `data`/`paging` response format and nonnegative integer metric counts; configuration does not add arbitrary API adapters.

The public configuration contract is `alice_codex.heartbeat.HEARTBEAT_SOURCES_CONFIG_VERSION = 1` and `parse_heartbeat_sources(raw) -> tuple[HeartbeatSourceBinding, ...]`. Each frozen binding contains target, a standard CollectionSpec and a floating-point wait_seconds. Parsing performs no I/O, does not resolve environment values, and does not modify its input. Config validation and direct Service construction both use it.

## Startup, observations and changes

Service validates the whole source configuration before opening business stores or starting native work. It then initializes the adapter and registers each immutable binding once during construction. Registration performs no collection, creates no schedules, enables no existing task and does not change pause or budget state. The normal watcher performs collection after Service startup, including read-only rechecks while autonomous dispatch is paused.

Configuration changes take effect through a controlled stop and restart; there is no hot reload or model-facing configuration writer. The operator must preserve the original configuration and unrelated fields while validating and atomically applying an edit. Maintenance views must not be saved over the raw JSON. Native tasks and ownership cleanup remain subject to the existing lifecycle guards.

On restart Alice restores the source and interval, then obtains a new host observation. It does not restore a previous process's monotonic timer or treat an old receipt as newly collected evidence. The stored receipt history, last_good and consumed records remain in place. A known observation with already-consumed scope/content does not repeat task input. A rejected or unknown latest observation cannot borrow an older known result to authorize input.

wait_seconds controls the process-local collection interval and the durable unchanged waiting record. max_age_seconds validates page/cache age at collection time; it is not a separate per-dispatch receipt TTL. Same-process waiting may reuse the latest receipt. Changing only waiting or pagination/request budgets does not reset the semantic baseline or consumed records.

The existing scope digest binds the full initial URL/query, source ID, subject, collection, required metrics, credential environment-variable names, auth_context_version and max_age_seconds. A change to these fields requires an actual collection under the new configuration before a known baseline can be considered by admission. It does not clear pause, task usage, quota protection or unresolved native work.

Deleting a source is an explicit withdrawal of that collection permission on the next restart. Alice stops contacting it and retains its old receipts and consumed evidence. Removing a source is not the same as pausing its heartbeat job: ordinary periodic self-review remains available when no external source is configured. Stop or disable the job explicitly if that is the intended action. Reintroducing an unchanged source does not erase its prior consumption.

## Credentials and unavailable sources

Authentication values belong in the actual Service process environment, referenced only by header_env names. They are read at collection time and are never saved into heartbeat receipts. Configuration does not accept literal headers, tokens, prefilled receipts, trusted/known flags, hashes, observation timestamps or model verdicts.

Use credential-free HTTPS business URLs and query filters; unencrypted HTTP is limited to loopback fixtures. CollectionSpec rejects URL user information and fragments, but a generic URL parser cannot recognize arbitrary secrets embedded in query values. This interface does not provide presigned-URL or URL-template authentication. Account or permission context changes require an explicit auth_context_version change; this collector cannot independently establish the credential owner's identity or account-wide coverage.

There is no new secret manager or launchd credential injector in this change. The integration owner must arrange the Service environment separately. A missing environment value, failed HTTP response, stale cache or incomplete collection leaves a valid source configured but unknown, with bounded rechecks. It cannot become unconfigured or an empty known snapshot. Structural configuration damage instead prevents Service startup and preserves the original files.

## Compatibility and evidence boundary

This adds no database schema, receipt format or persistent identifier migration. Rollback must preserve all new observations, consumption, task usage and pause records; restoring an older database snapshot is not a rollback mechanism.

An older strict RuntimeConfig parser cannot read the new field. The release/bootstrap owner must consume the integer configuration capability and reject unsupported candidates or independent bootstraps whenever the raw configuration contains heartbeat_sources, including null or empty sources. A missing capability means unsupported. The compatibility gate and environment deployment are separate integration dependencies; this change does not modify publishing or bootstrap modules. Do not remove source configuration automatically to make an incompatible rollback pass.

Validation must distinguish pure parser/configuration tests, real localhost collection with Service/Store persistence, and installed-candidate CLI/native tests. The production-entry acceptance requires the same candidate's isolated Python to start and restart the normal CLI Service from its saved configuration, without a test-only call to register_heartbeat_source. It must cover damaged configuration, unchanged consumption, preserved pause, source withdrawal/change and configured unknown. Local Responses fixtures exercise the real Codex public protocol without real model inference. No fixture result establishes production deployment, account identity or autonomous business success.
