# Optional standalone browser runtime

Alice can install a pinned Playwright MCP through the public Codex App Server
interfaces. This is browser support; it does not supply Desktop Computer Use or
prove general autonomous learning or live website integration.

`browser.install(config, node=..., npm_cli=..., browser_executable=...)` is a
synchronous operation. Async CLI callers must use `asyncio.to_thread`. All three
paths are explicit; the installer does not search accounts, install Chrome,
connect to a running browser, or copy a personal browser profile.

Node must be version 20 or newer. The bundled npm package metadata and portable
lock file pin `@playwright/mcp` 0.0.80 and Playwright 1.63.0-alpha-2026-08-31.
Installation runs `node npm-cli.js ci` with a private home and cache, disabled
install scripts, no global installation and no browser download. Runtime uses
absolute Node and package paths, without `npx` or `latest`.
Playwright IPC uses a separate short private socket directory; its
`PWTEST_SOCKETS_DIR` setting avoids Unix socket-length failures when Alice's
data-directory path is long. It does not point at a Desktop browser bridge.

Installation requires a stopped Alice service. It holds the same lifetime lock
used by the service, probes a remaining control socket, and rejects both a live
response and an uncertain response. A remaining native socket also requires
diagnosis. It does not stop a running instance or treat a timeout as stopped.

The runtime is created outside the writable workspace. Before writing the MCP
configuration, an owned App Server with a fresh authentication-free Codex home
validates the final package path, executable, environment, namespace and tool
allowlist. This check starts a native thread but never starts a model turn. It
navigates a synthetic localhost page, checks actual browser snapshots containing
true zero and missing data, clicks an observed second-page link and checks a
second-page-only count. It then closes the browser and verifies owned processes
have exited. Failure retains private diagnostics and preserves the previous
Codex configuration.

The resulting `mcp_servers.alice_browser` stanza is optional, uses an independent
headless in-memory Chrome context and enables the browser sandbox. Only
navigation, snapshots, clicking, page-network inspection and browser close are
exposed. Tool approval defaults to `prompt`. The installer refuses to overwrite
an unrelated namespace or a concurrent configuration edit. Alice's existing
configuration merger preserves this separate MCP stanza.

`browser.status(config)` is read-only and reports `installed` separately from
`native_verified`. It checks the registered stanza, dependency hashes and
installed package files before trusting the last native verification. It does
not execute a browser or silently repair drift. Matching verified installations
are idempotent; changed dependencies require installation and verification again.
The browser executable hash detects changes but is not a full signed-app bundle
pin. Local manifests are regression checks, not protection against a process
with the user's filesystem permissions.

The localhost page log is not an OS network firewall. MCP browser processes do
not automatically inherit every restriction of native shell tools. Real-site
authentication, publishing and tool approvals remain separate integration work.
This single-thread check also does not establish cross-thread context isolation
or automatic browser-page cleanup on every task pause. Native task interruption
alone must not be reported as proof that a live MCP browser page has closed.

Artifacts, dependency paths, hashes and verification diagnostics stay in Alice's
private data directory. Production credentials, private browser profiles and
Desktop session metadata are not inputs to the verification.
