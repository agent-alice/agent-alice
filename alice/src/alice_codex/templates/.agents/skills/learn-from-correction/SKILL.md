---
name: learn-from-correction
description: Turn a concrete user correction or reproducible Alice failure into a tested change to a collector, script, or skill, with evidence of reuse in a fresh task.
metadata:
  version: "1.2.0"
---

Start with the failed outcome and its external evidence. Record what Alice believed, what was actually observable, the relevant source/version, and a minimal reproduction. Distinguish missing observation or permissions, incorrect parsing, reasoning, tool execution, and lost context. Repeatedly reasoning over the same incomplete API response does not add missing evidence.

Change the component responsible for the failure in a candidate branch or directory. Fix an observation problem in the collector or verification contract before adding more prompting. Use a skill for a reusable decision procedure; retain the episode and evidence in memory. A memory note alone does not establish that a procedure was learned.

Validate the original failure and at least three new variants. Have a separate review or task choose the variants when available; derive expected results from raw fixtures or independent observations, not from the changed implementation. Useful observation variants include null or missing counters, real zero, a record only on page two, and a rendered-page-only record. Draft variants include encoded comments, Markdown comment directives and private notes hidden among public paragraphs. Keep external publishing and production state out of regression fixtures.

Record the candidate version/hash, commands, passed/failed/skipped results, and evidence locations. Do not rewrite expected answers merely to make a failed regression pass. Keep the previously verified version recoverable; promote through the existing release checks.

For the built-in observation regression, call Alice's `runtime_info` MCP tool. Append `--tasks <learning_inputs.tasks> --oracle <learning_inputs.oracle> --report <new absolute report path>` to its `commands.evaluate` array, using the actual returned paths. These separate task/oracle files ship in the serving package; no source checkout is required. Execute the argument array directly, or quote every argument for a shell. Re-query after runtime reconnect/restart; bare `python` may load the wrong package. For a development candidate, run that candidate's installed evaluator and record its version; testing the active package does not test an uninstalled edit.

Retain the original implementation's report, then add `--baseline-report <absolute baseline path> --correction <absolute correction path>` to evaluate a change. The bundled `learning_inputs.correction` only describes its specific synthetic example; write a separate correction document for a different failure. The evaluator rechecks actual results and produces a chain with failure evidence, cause hypothesis, change, scope, regression, unseen shapes, counterexamples and repeated failures. A missing or invalid correction chain fails the command while preserving the current run. Reports use new paths to preserve earlier failures. Keep private inputs and reports outside Git.

The fixed suite is public and tests offline rules; its unseen shapes are held out from the training case, not secret from the developer. Its `regression_passed_transfer_not_tested` state never promotes a skill. Report gap misses and false positives alongside task success. Record native token observations independently from monetary receipts, and keep virtual budgets disabled unless separately configured and authorized.

Then give a fresh Codex task the skill and a new raw case, without the prior answer or correction narrative. Verify its external result. Until this transfer test runs, describe the state as “regression passed; fresh-task reuse not tested,” not “learned.” A failed transfer is evidence for a narrower instruction or better retrieval, not permission to accumulate unbounded memory or change the active runtime in place.
