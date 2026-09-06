---
name: runtime-observation-diagnosis
description: Diagnose differences between Alice's expected tools or observations and actual Codex requests, tool registration, responses, and execution receipts.
metadata:
  version: "1.0.0"
---

Start with one reproducible discrepancy. Distinguish the configured request, actual forwarded request, native tool catalog, provider response, tool execution and final external result. A missing field at one boundary should not be repaired by assuming the next layer supplied it.

Use Alice `status` and `runtime_info` for runtime identity and the serving package's command paths. Inspect the relevant native catalog or bounded, redacted event metadata through the existing public interfaces. Avoid dumping whole config files, credential values or private transcripts. Do not assume a historical proxy's model or tool-injection behavior applies to this Codex version.

Where transport behavior is uncertain, reproduce it in a separate candidate with synthetic inputs and an owned localhost provider. Compare actual request/response frames, preserving the original failing evidence. Do not disable required MCP tools, identity hooks or permissions on the running instance as a diagnostic shortcut. Test the smallest supported change through the real launch and tool-registration path.

Record the observed boundary, likely cause and remaining uncertainty. A successful model response or HTTP status does not prove the tool ran; verify its execution receipt or independent output. If the observed behavior cannot be reproduced, preserve the unresolved state instead of promoting a speculative configuration fix.
