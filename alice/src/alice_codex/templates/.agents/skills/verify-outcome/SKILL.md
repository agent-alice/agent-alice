---
name: verify-outcome
description: Verify Alice task results when website observations may be incomplete, a draft may contain private notes, or a publication result is uncertain.
metadata:
  version: "1.0.0"
---

Treat tool success as evidence about the operation, then verify the requested result. Preserve the source, observation time, object ID, and uncertainty. A successful HTTP status or model self-report is insufficient for a publication claim.

For Zhihu observations, export raw collector responses before aggregation. Old `zhihu_daily_check.py` totals already turn missing fields into zero and only enumerate a first page; those totals cannot establish complete observation. Adapt the collector to this v1 JSON envelope without importing cookies or credentials into it:

```json
{"schema_version":1,"subject":"account-id","collection":"answers","required_metrics":["voteup_count","comment_count"],"pages":[{"source":"answers-api","observed_at":"2026-09-06T10:00:00Z","cursor":null,"next_cursor":null,"status":"ok","items":[{"id":"answer-id","voteup_count":0}]}]}
```

Preserve missing keys/nulls; never use `.get(field, 0)`. Map the API's actual pagination to `cursor`/`next_cursor`, retain failed pages as `status: "error"`, and use a final null only when the collector observed the end. `expected_count`, if available, must describe the same collection. A count for answers does not cover articles or notifications.

An independent rendered-page observation may be attached as `independent` with `source`, `observed_at`, identical `subject`/`collection`, `coverage: "partial" | "complete"`, and `items`. Do not reuse the same API output as independent evidence. Compare scope, timestamps and permissions before interpreting differences. An API-only item is not a discrepancy when the browser view is partial.

Run `python -m alice_codex.business summarize-observation observation.json`. Unknown totals remain null; `observed_sum` covers only known retrieved items. The command is offline validation, not a collector, and does not certify the whole account.

Before publishing, produce the exact public payload separately from notes. Run `python -m alice_codex.business check-draft payload.md` and repeat on the final rendered payload. Findings block this static check; fix them explicitly rather than silently stripping text. Also inspect the actual preview, links and images; these are not checked by the offline command.

Preserve the authorized action's stable ID and exact final-payload hash before sending. Save any external object ID. For timeouts or uncertain results, retrieve the external object or search read-only evidence before further action. `reconcile-publication --intent intent.json --evidence evidence.json` accepts an independently fetched `read_back` receipt; see `alice_codex.business.reconcile_publication` for the input fields. Identical content without an action/object binding is only a candidate. This module never sends a publication or authorizes an automatic retry.
