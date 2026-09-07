# Verified command launcher (#32)

Status: implementation on `codex/startup-verification`, following the startup-setting change. Owns `launcher.py`, release resolution, the optional launcher entry point, their tests/docs and the opt-in command probe in the synthetic wheel fixture. Exact verification results are recorded in the associated PR. No running installation is modified here.

The deployment's outer wrapper calls `checked_current()` before every command. This repeats full database integrity scans even when the selected command only reads service status or stops owned processes. Command selection needs verified code and an authentic release pointer. Starting a business service or switching releases additionally needs data compatibility and integrity checks.

Add a code-only `resolve_current()` operation and a packaged launcher that delegates to that exact verified interpreter. Keep `checked_current()`, activation, rollback, CLI service startup and all store integrity checks unchanged. The new resolver must not return unverified or changed code, trust a raw pointer interpreter, claim data health, write business data, or cache validation across changes. The launcher preserves argument boundaries, removes Python import override environment variables and defaults to `chat`.

Acceptance: a real staged/verified synthetic candidate can be resolved while an unrelated business database is damaged, allowing diagnostics to remain reachable; the existing full startup guard still rejects that database and preserves its bytes. Changed code or a forged pointer is rejected on every resolution. Exercise literal arguments/environment isolation and the packaged module through installed-artifact coverage. Run synthetic latency measurements; production measurements and deployment remain separate gates.

This removes one outer scan from the intended deployment command path. Repeated supervisor/child startup scans remain tracked in #32 and are not claimed fixed by this change. Deployment must replace the old outer wrapper with the tested packaged entry after native release gates pass.
