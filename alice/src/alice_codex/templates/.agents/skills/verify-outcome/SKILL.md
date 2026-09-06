---
name: verify-outcome
description: Verify Alice task results when website observations may be incomplete, a draft may contain private notes, or a publication result is uncertain.
metadata:
  version: "1.2.0"
---

Treat tool success as evidence about the operation, then verify the requested result. Preserve the source, observation time, object ID, and uncertainty. A successful HTTP status or model self-report is insufficient for a publication claim.

Call Alice's `runtime_info` MCP tool for the serving package's command arrays. Append absolute input paths and execute the array directly, or quote each argument for a shell. Re-query after restart or reconnect. Do not substitute bare `python` or resolve the interpreter symlink out of its virtualenv.

For Zhihu observations, export raw collector responses before aggregation. Old `zhihu_daily_check.py` totals already turn missing fields into zero and only enumerate a first page; those totals cannot establish complete observation. Adapt the collector to this v1 JSON envelope without importing cookies or credentials into it:

```json
{"schema_version":1,"subject":"account-id","collection":"answers","required_metrics":["voteup_count","comment_count"],"pages":[{"source":"answers-api","observed_at":"2026-09-06T10:00:00Z","cursor":null,"next_cursor":null,"status":"ok","items":[{"id":"answer-id","voteup_count":0}]}]}
```

Preserve missing keys/nulls; never use `.get(field, 0)`. Map the API's actual pagination to `cursor`/`next_cursor`, retain failed pages as `status: "error"`, and use a final null only when the collector observed the end. `expected_count`, if available, must describe the same collection. A count for answers does not cover articles or notifications.

Retain v1 optional `truncated`, `http_status`, bounded `error`, `cache_age_seconds` and `cache_max_age_seconds` metadata. For a time-sensitive task, declare `freshness: {"as_of": "2026-09-06T10:00:30Z", "max_age_seconds": 120}` using the intended comparison time. Missing freshness policy is `not_checked`, not proof that a cached observation is current. Truncation, denied access and stale or unverified cache evidence keep the result unknown even with a terminal cursor. A repeated API source cannot establish independence.

An independent rendered-page observation may be attached as `independent` with `source`, `observed_at`, identical `subject`/`collection`, `coverage: "partial" | "complete"`, and `items`. Do not reuse the same API output as independent evidence. Compare scope, timestamps and permissions before interpreting differences. An API-only item is not a discrepancy when the browser view is partial.

Append the observation file's absolute path to `runtime_info.commands["summarize-observation"]` and run it. Unknown totals remain null; `observed_sum` covers only known retrieved items. The command is offline validation, not a collector, and does not certify the whole account.

Before publishing, produce the exact public payload separately from notes. Append its absolute path to the returned `check-draft` command and repeat on the final rendered payload. Findings block this static check; fix them explicitly rather than silently stripping text. Also inspect the actual preview, links and images; these are not checked by the offline command.

Preserve the authorized action's stable ID and exact final-payload hash before sending. Save any external object ID. For timeouts or uncertain results, retrieve the external object or search read-only evidence before further action. Append `--intent <absolute intent path> --evidence <absolute evidence path>` to the returned `reconcile-publication` command; it accepts an independently fetched `read_back` receipt. See `alice_codex.business.reconcile_publication` for the input fields. Identical content without an action/object binding is only a candidate. This module never sends a publication or authorizes an automatic retry.

Include the action's `sent_at` when known. Readback observed before that time, stale cache, truncation or denied access cannot confirm the action. Without an action time, `temporal_check=not_checked` limits the result to the supplied snapshot.
