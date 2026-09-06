"""Host-collected heartbeat evidence, separate from model-authored observations.

Registration and collection are internal host operations, not MCP/control inputs.
Only snapshots returned by this adapter belong in its trusted receipt stream.
Neither a JSON receipt nor compare_receipts authenticates an arbitrary caller.
Known means a complete retrieved ID/metric snapshot under the configured local
and supplied cache freshness checks. It does not prove origin revalidation,
unrequested content, account-wide state, or completion of a business goal.
"""

from dataclasses import dataclass, field, fields
import hashlib
import json
import math
import re
from uuid import uuid4

from .business import MAX_ITEMS, summarize_observation
from .collector import _origin, collect_collection


VALIDATOR_VERSION = "collector-summary-v1"
HEARTBEAT_SOURCES_CONFIG_VERSION = 1


def _digest(value) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _text(value, name, *, maximum=2000):
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 for character in value)
    ):
        raise ValueError(f"{name} must be bounded nonempty text")
    return value


def _number(value, name, *, minimum=0, maximum=None):
    try:
        valid = (
            type(value) in (int, float)
            and math.isfinite(value)
            and value >= minimum
            and (maximum is None or value <= maximum)
        )
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError(f"{name} must be a finite number within its bounds")
    return value


@dataclass(frozen=True, kw_only=True)
class CollectionSpec:
    """One explicit host-owned collection; the initial URL includes its filters.

    The complete URL/query and credential *environment variable names* bind the
    scope digest. Credentials are read only by the collector and never saved in
    receipts. Hosts must change auth_context_version when the credential/account
    context changes; this collector cannot independently discover that identity.
    Changing page/time limits does not change semantic scope: a known receipt
    still requires complete pagination. No independent/model-supplied evidence
    or arbitrary observation documents are accepted here.
    """

    source_id: str
    url: str = field(repr=False)
    subject: str
    collection: str
    auth_context_version: str
    max_age_seconds: float
    required_metrics: tuple[str, ...] = ("voteup_count", "comment_count")
    header_env: tuple[tuple[str, str], ...] = ()
    max_pages: int = 20
    max_items: int = 2000
    request_timeout: float = 10
    total_seconds: float = 60

    def __post_init__(self):
        for name in ("source_id", "subject", "collection", "auth_context_version"):
            _text(getattr(self, name), name)
        _text(self.url, "url", maximum=8192)
        _origin(self.url)
        if (
            not isinstance(self.required_metrics, tuple)
            or not 1 <= len(self.required_metrics) <= 8
            or any(
                not isinstance(metric, str)
                or not re.fullmatch(r"[a-zA-Z_][a-zA-Z_0-9]{0,63}", metric)
                for metric in self.required_metrics
            )
            or len(set(self.required_metrics)) != len(self.required_metrics)
        ):
            raise ValueError("required_metrics must be a tuple of 1..8 distinct metric names")
        if not isinstance(self.header_env, tuple):
            raise ValueError("header_env must be an immutable tuple of name/environment pairs")
        seen = set()
        for entry in self.header_env:
            if not isinstance(entry, tuple) or len(entry) != 2:
                raise ValueError("header_env requires name/environment pairs")
            name, variable = entry
            if (
                not isinstance(name, str)
                or not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name)
                or name.lower() in {"host", "content-length", "transfer-encoding", "connection"}
                or name.lower() in seen
                or not isinstance(variable, str)
                or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", variable)
            ):
                raise ValueError(
                    "header_env must map unique permitted headers to environment names"
                )
            seen.add(name.lower())
        _number(self.max_age_seconds, "max_age_seconds")
        for name, maximum in (("max_pages", 1000), ("max_items", MAX_ITEMS)):
            if type(getattr(self, name)) is not int or not 1 <= getattr(self, name) <= maximum:
                raise ValueError(f"{name} is outside its supported bound")
        for name, maximum in (("request_timeout", 60), ("total_seconds", 600)):
            _number(getattr(self, name), name, maximum=maximum)
            if getattr(self, name) == 0:
                raise ValueError(f"{name} must be positive")

    @property
    def scope_sha256(self) -> str:
        # Hash the complete initial query without persisting signed parameters.
        return _digest(
            {
                "version": 1,
                "source_id": self.source_id,
                "url": self.url,
                "subject": self.subject,
                "collection": self.collection,
                "required_metrics": sorted(self.required_metrics),
                "header_env": sorted(
                    (name.lower(), variable) for name, variable in self.header_env
                ),
                "auth_context_version": self.auth_context_version,
                "max_age_seconds": float(self.max_age_seconds),
            }
        )


@dataclass(frozen=True)
class HeartbeatSourceBinding:
    """A validated host registration, separate from collected evidence."""

    target: str
    spec: CollectionSpec
    wait_seconds: float


def parse_heartbeat_sources(raw: object) -> tuple[HeartbeatSourceBinding, ...]:
    """Parse host configuration without reading credentials or collecting data.

    Absent configuration is distinct from a malformed configured source: the
    latter must fail rather than silently restore unconfigured self-review.
    JSON arrays are copied into immutable spec fields. URLs may contain business
    filters; authentication belongs in header_env, not embedded URL credentials
    or signed queries. A general URL parser cannot identify query secrets.
    """
    if raw is None:
        return ()
    if (
        not isinstance(raw, dict)
        or set(raw) != {"version", "sources"}
        or type(raw["version"]) is not int
        or raw["version"] != HEARTBEAT_SOURCES_CONFIG_VERSION
        or not isinstance(raw["sources"], list)
    ):
        raise ValueError("heartbeat_sources requires version 1 and a sources list")
    bindings, targets = [], set()
    spec_fields = {item.name for item in fields(CollectionSpec)}
    for entry in raw["sources"]:
        if not isinstance(entry, dict) or set(entry) != {"target", "wait_seconds", "spec"}:
            raise ValueError("heartbeat source requires target, wait_seconds and spec")
        target = _text(entry["target"], "heartbeat target", maximum=150)
        try:
            target.encode("utf-8")
        except UnicodeError:
            raise ValueError("heartbeat target must be valid UTF-8 text") from None
        if target == "new" or target.startswith(("summary:", "scheduled:")):
            raise ValueError("heartbeat source requires a stable named target")
        if target in targets:
            raise ValueError("heartbeat source targets must be unique")
        wait = _number(entry["wait_seconds"], "heartbeat wait_seconds")
        if wait == 0:
            raise ValueError("heartbeat wait_seconds must be positive")
        raw_spec = entry["spec"]
        if not isinstance(raw_spec, dict) or not set(raw_spec).issubset(spec_fields):
            raise ValueError("heartbeat source spec requires only CollectionSpec fields")
        values = dict(raw_spec)
        if "required_metrics" in values:
            if not isinstance(values["required_metrics"], list):
                raise ValueError("heartbeat required_metrics must be a JSON list")
            values["required_metrics"] = tuple(values["required_metrics"])
        if "header_env" in values:
            headers = values["header_env"]
            if not isinstance(headers, list) or any(
                not isinstance(pair, list) or len(pair) != 2 for pair in headers
            ):
                raise ValueError("heartbeat header_env must be a JSON list of two-item lists")
            values["header_env"] = tuple(tuple(pair) for pair in headers)
        try:
            spec = CollectionSpec(**values)
            # Registration hashes the scope after Service opens its stores.
            # Reject text that cannot be hashed here, before any host side effects.
            spec.scope_sha256
        except (TypeError, ValueError, OverflowError):
            # Never interpolate configuration values or expose constructor errors
            # that could contain a private URL or credential material.
            raise ValueError("heartbeat source has invalid CollectionSpec fields") from None
        targets.add(target)
        bindings.append(HeartbeatSourceBinding(target=target, spec=spec, wait_seconds=float(wait)))
    return tuple(bindings)


class HostHeartbeatAdapter:
    """Collect synchronously; async hosts should use asyncio.to_thread(observe, ...).

    No source is inferred from a prompt, HEARTBEAT.md, ledger or output file.
    Registration is process-local and must be restored by host code on restart;
    an absent registration yields unconfigured, never an empty known snapshot.
    Receipt persistence and comparison watermarks belong to the caller.
    """

    def __init__(self):
        self._sources: dict[str, CollectionSpec] = {}

    def register(self, target: str, spec: CollectionSpec) -> None:
        _text(target, "target", maximum=150)
        if not isinstance(spec, CollectionSpec):
            raise ValueError("a validated host CollectionSpec is required")
        self._sources[target] = spec

    def observe(self, target: str, *, now: float) -> dict:
        _text(target, "target", maximum=150)
        _number(now, "now")
        spec = self._sources.get(target)
        receipt = {
            "version": 1,
            "id": str(uuid4()),
            "target": target,
            "source_id": spec.source_id if spec else None,
            "scope_sha256": spec.scope_sha256 if spec else None,
            "validator_version": VALIDATOR_VERSION,
            "observed_at": now,
            "state": "unconfigured" if spec is None else "unknown",
            "content_sha256": None,
            "evidence_sha256": None,
            "reason": "source_unconfigured" if spec is None else "collection_unavailable",
        }
        if spec is None:
            receipt["evidence_sha256"] = _digest({"reason": receipt["reason"]})
            return receipt
        try:
            result = collect_collection(
                spec.url,
                subject=spec.subject,
                collection=spec.collection,
                required_metrics=spec.required_metrics,
                header_env=dict(spec.header_env),
                max_pages=spec.max_pages,
                max_items=spec.max_items,
                request_timeout=spec.request_timeout,
                total_seconds=spec.total_seconds,
                max_age_seconds=spec.max_age_seconds,
            )
            document, fetches = result["observation"], result["fetches"]
            # Recompute the independent structural verdict over our own fetch;
            # never import a caller/model's claimed summary or passed field.
            summary = summarize_observation(document)
            receipt["evidence_sha256"] = _digest(
                {
                    "observation": document,
                    "fetches": fetches,
                    "validator_version": VALIDATOR_VERSION,
                }
            )
            if (
                not summary["complete"]
                or summary["coverage"]["freshness"] != "fresh"
                or not fetches
                or any(fetch.get("http_status") != 200 or "error" in fetch for fetch in fetches)
            ):
                receipt["reason"] = "collection_incomplete_or_unfresh"
                return receipt
            observed = {}
            for page in document["pages"]:
                for item in page["items"]:
                    identity = str(item["id"])
                    observed[identity] = {
                        "id": identity,
                        "metrics": {metric: item[metric] for metric in spec.required_metrics},
                    }
            receipt["content_sha256"] = _digest(
                {
                    "scope_sha256": spec.scope_sha256,
                    "items": [observed[identity] for identity in sorted(observed)],
                }
            )
            receipt.update(state="known", reason=None)
        except (ValueError, OSError, TimeoutError):
            # Never store exception text, remote content or credential values.
            receipt["evidence_sha256"] = _digest({"reason": receipt["reason"]})
        return receipt


def _validate_receipt(receipt):
    if (
        not isinstance(receipt, dict)
        or type(receipt.get("version")) is not int
        or receipt["version"] != 1
    ):
        raise ValueError("unsupported heartbeat receipt")
    for name in ("id", "target", "validator_version"):
        _text(receipt.get(name), name)
    _number(receipt.get("observed_at"), "observed_at")
    state = receipt.get("state")
    if not isinstance(state, str) or state not in {"known", "unknown", "unconfigured"}:
        raise ValueError("invalid heartbeat receipt state")
    if state == "known":
        if receipt.get("reason") is not None:
            raise ValueError("known receipt cannot retain an unknown reason")
    else:
        _text(receipt.get("reason"), "reason", maximum=200)
    for name in ("evidence_sha256", "scope_sha256", "content_sha256"):
        value = receipt.get(name)
        absent = (
            name == "scope_sha256"
            and state == "unconfigured"
            or (name == "content_sha256" and state != "known")
        )
        if absent:
            if value is not None:
                raise ValueError(f"{name} must be absent for {state}")
        elif not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
            raise ValueError(f"{name} must be a SHA-256 digest")
    if state == "unconfigured":
        if receipt.get("source_id") is not None:
            raise ValueError("unconfigured receipt cannot claim a source")
    else:
        _text(receipt.get("source_id"), "source_id")


def compare_receipts(
    previous: dict | None, current: dict, *, last_good: dict | None = None
) -> dict:
    """Compare already trusted receipts; this function provides no authentication.

    previous is the latest accepted receipt, including unknown observations.
    Keep last_good separately when unknown observations advance that watermark.
    Duplicate IDs with different payloads are conflicts. Different IDs at equal
    or older timestamps are stale and cannot change a baseline or extend waits.
    Only same-scope fresh known observations can mean unchanged/new_evidence;
    new_evidence authorizes consideration of a wake, not business completion or
    clearing pause, quota, task budgets, or unresolved native work.
    """
    _validate_receipt(current)
    if previous is not None:
        _validate_receipt(previous)
        if previous["target"] != current["target"]:
            raise ValueError("heartbeat receipts must belong to the same target")
    result = {
        "state": current["state"],
        "accept": True,
        "update_last_good": False,
        "wake": False,
        "reason": current.get("reason"),
    }
    if previous is not None and previous["id"] == current["id"]:
        if previous != current:
            raise ValueError("heartbeat receipt ID identifies conflicting evidence")
        return {**result, "state": "duplicate", "accept": False, "reason": "same_receipt"}
    if previous is not None and current["observed_at"] <= previous["observed_at"]:
        return {**result, "state": "stale", "accept": False, "reason": "observation_not_newer"}
    if last_good is None and previous is not None and previous["state"] == "known":
        last_good = previous
    if last_good is not None:
        _validate_receipt(last_good)
        if last_good["state"] != "known" or last_good["target"] != current["target"]:
            raise ValueError("last_good must be a known receipt for the same target")
        if previous is None or last_good["observed_at"] > previous["observed_at"]:
            raise ValueError("last_good cannot exceed the accepted observation watermark")
    if current["state"] != "known":
        return result
    binding = ("source_id", "scope_sha256", "validator_version")
    if last_good is None or any(last_good[key] != current[key] for key in binding):
        return {**result, "state": "baseline", "update_last_good": True, "reason": "new_scope"}
    changed = last_good["content_sha256"] != current["content_sha256"]
    return {
        **result,
        "state": "new_evidence" if changed else "unchanged",
        "update_last_good": True,
        "wake": changed,
        "reason": None,
    }
