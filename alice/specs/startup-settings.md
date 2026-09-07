# Startup settings (#32)

Status: configuration support implemented on `codex/startup-verification`, based on `54b46db`. Deployment and performance follow-up remain in #32; no running installation is changed by this worktree. Verification results belong to the associated PR and its exact candidate evidence.

The imported 4.84 GB source index requires about 19 seconds per full integrity scan. Repeated startup checks exceeded the supervisor's default 30-second readiness deadline. A local deployment currently uses the existing constructor's 120-second option; standard service installation must make that setting reproducible and retain it when reinstalling.

This change owns `startup.py`, `supervisor.py`, `launchd.py`, the service-install CLI arguments, related tests (including installed CLI rejection coverage) and deployment documentation. It adds a finite startup deadline through the standard supervisor command, persists it with the installed service, rejects invalid settings before changing installation, and preserves it on reinstall. A nondefault setting requires an installed bootstrap implementation that supports the command-line option. Compatible older application candidates may still run under the newer independent bootstrap.

Acceptance: install at 120 seconds, reinstall without an override and retain 120; explicitly select a new finite value; reject zero, negative, boolean, NaN, infinity and values above the documented maximum; reject an unsupported bootstrap before replacement; stop remains available when the timeout setting is invalid; preserve bounded failures, owned-process shutdown and data. Exercise the actual supervisor entry in isolated tests, then the exact installed artifact before deployment.

The ordinary regressions use synthetic launchd responses and owned subprocesses. The delayed-readiness test advances only the supervisor's monotonic clock past 30 seconds while a real child waits for an explicit readiness signal; it does not measure 31 seconds of disk I/O. Installed-artifact coverage must also verify invalid settings leave a running synthetic service untouched. Positive service installation through real macOS launchd and the native release/recovery gates remain required before replacing an active bootstrap.

This does not remove database checks or change archive previews, source IDs, database formats, model calls or the Codex core. Repeated integrity scans and CLI latency remain explicit work in #32; a passing configuration test is not a performance result.
