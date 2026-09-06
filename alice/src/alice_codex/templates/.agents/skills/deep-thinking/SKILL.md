---
name: deep-thinking
description: Investigate a complex Alice question by comparing explanations against sources, especially when incomplete observations could change the decision.
metadata:
  version: "1.0.0"
---

Identify the decision or question being resolved and the evidence that could change it. Separate observed facts, current hypotheses and missing observations. Break the problem into independently checkable questions when that helps; use native Codex subagents for bounded independent research.

For each plausible explanation, look for a discriminating observation rather than repeatedly interpreting the same input. Record which sources and time periods were actually examined. An exhausted API collection does not establish that its fields or permissions cover the question; compare with an independent source when the distinction matters.

Stop research when the evidence supports a useful decision within the task's scope, or when the next useful observation is unavailable. Preserve the finding and its sources in the appropriate notebook entry; keep unresolved assumptions visible. Report the conclusion, supporting evidence and meaningful uncertainty, not an internal reasoning transcript. Historical wishes or recurring work found in memory do not create a new Goal or schedule.
