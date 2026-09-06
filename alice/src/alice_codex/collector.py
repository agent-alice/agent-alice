"""Bounded, read-only HTTP collection into business observation schema v1.

This implements the list shape used by the legacy Zhihu collector (data and
paging.is_end/next), not a guarantee that undocumented production endpoints or
authentication still work. It never publishes, logs response bodies, discovers
accounts, loads cookie files, or treats one collection as an entire account.
"""

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import http.client
import json
import math
import os
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from alice_codex.business import MAX_INPUT_BYTES, MAX_ITEMS, summarize_observation


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _origin(url: str) -> tuple:
    try:
        parsed = urlsplit(url)
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
    except (TypeError, ValueError):
        raise ValueError("collection URL is invalid") from None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or any(ord(c) < 33 for c in url)
    ):
        raise ValueError("collection URL must be HTTP(S), without user info or fragments")
    if parsed.scheme == "http" and parsed.hostname not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("unencrypted HTTP is allowed only for loopback test endpoints")
    return parsed.scheme, parsed.hostname.lower(), port


def _cursor(url: str) -> str:
    # API cursors and signed query parameters must not enter logs or summaries.
    return "page:" + hashlib.sha256(url.encode()).hexdigest()[:24]


def _source(url: str) -> str:
    parsed = urlsplit(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _headers(header_env: dict | None) -> dict:
    headers = {
        "Accept": "application/json",
        "Accept-Encoding": "identity",
        "User-Agent": "AliceReadOnlyCollector/1",
    }
    for name, variable in (header_env or {}).items():
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name)
            or name.lower() in {"host", "content-length", "transfer-encoding", "connection"}
            or not isinstance(variable, str)
            or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", variable)
        ):
            raise ValueError(
                "header_env must map permitted header names to environment variable names"
            )
        value = os.environ.get(variable)
        if value is None or not value or any(ord(c) < 32 or ord(c) == 127 for c in value):
            raise ValueError(
                "a required collector credential environment variable is absent or invalid"
            )
        headers[name] = value
    return headers


def _cache_evidence(headers) -> dict:
    """Keep bounded numeric cache evidence, never arbitrary header text."""
    result = {}
    age = headers.get("Age")
    if age is not None:
        if len(age) > 16 or not age.isascii() or not age.isdecimal():
            result["error"] = "cache_metadata_invalid"
        else:
            result["cache_age_seconds"] = int(age)
    directives = headers.get("Cache-Control", "").split(",")
    lifetimes = []
    for directive in directives:
        name, separator, value = directive.strip().partition("=")
        if name.lower() not in {"max-age", "s-maxage"}:
            continue
        match = re.fullmatch(r'(?:"([0-9]{1,16})"|([0-9]{1,16}))', value.strip())
        if not separator or match is None:
            result["error"] = "cache_metadata_invalid"
        else:
            lifetimes.append(int(match.group(1) or match.group(2)))
    if lifetimes:
        result["cache_max_age_seconds"] = min(lifetimes)
        if age is None:
            try:
                dated = parsedate_to_datetime(headers.get("Date", ""))
                if dated.tzinfo is None:
                    raise ValueError("cache date has no timezone")
                result["cache_age_seconds"] = max(
                    0, (datetime.now(timezone.utc) - dated).total_seconds()
                )
            except (TypeError, ValueError, OverflowError):
                result["error"] = "cache_metadata_invalid"
    if re.search(r"(?:^|,)\s*11[01]\s", headers.get("Warning", "")):
        result["error"] = "stale_cache"
    return result


def collect_collection(
    url: str,
    *,
    subject: str,
    collection: str,
    required_metrics: tuple[str, ...] = ("voteup_count", "comment_count"),
    header_env: dict | None = None,
    max_pages: int = 20,
    max_items: int = 2000,
    max_response_bytes: int = MAX_INPUT_BYTES,
    max_total_bytes: int = 8 * 1024 * 1024,
    request_timeout: float = 10,
    total_seconds: float = 60,
    expected_count: int | None = None,
    independent: dict | None = None,
    max_age_seconds: float | None = None,
) -> dict:
    """GET one explicit collection, following same-origin, same-path next links.

    Returns {observation, summary, fetches}. Observation pages contain only IDs
    and requested metrics, retaining absent/null fields. Invalid metric values
    are represented by their type, not arbitrary remote strings/objects. Failed requests become
    failed pages, not empty collections. No retry, redirect, ambient proxy, or
    publication occurs. Header values come exclusively from named environment
    variables and are never included in the returned diagnostics.

    total_seconds is checked before each request and response chunk; a blocking
    I/O operation can exceed that budget by at most request_timeout. Limits are
    conservative stops, never proof that pagination is complete. independent is
    a separately supplied browser observation in business schema v1.
    max_age_seconds optionally checks snapshot/cache age at collection completion.
    HTTP cache lifetime/Age/Warning evidence is retained when supplied; missing
    metadata cannot prove that an origin is current or that a cache revalidated.
    """
    origin = _origin(url)
    for value, maximum, name in (
        (max_pages, 1000, "max_pages"),
        (max_items, MAX_ITEMS, "max_items"),
        (max_response_bytes, MAX_INPUT_BYTES, "max_response_bytes"),
        (max_total_bytes, 16 * 1024 * 1024, "max_total_bytes"),
    ):
        if type(value) is not int or not 1 <= value <= maximum:
            raise ValueError(f"{name} is outside its supported bound")
    if not 0 < request_timeout <= 60 or not 0 < total_seconds <= 600:
        raise ValueError("timeouts must be positive and bounded")
    if max_age_seconds is not None:
        try:
            valid_age = (
                type(max_age_seconds) in (int, float)
                and math.isfinite(max_age_seconds)
                and max_age_seconds >= 0
            )
        except OverflowError:
            valid_age = False
        if not valid_age:
            raise ValueError("max_age_seconds must be a finite nonnegative number")
    document = {
        "schema_version": 1,
        "subject": subject,
        "collection": collection,
        "required_metrics": list(required_metrics),
        "pages": [],
    }
    if expected_count is not None:
        document["expected_count"] = expected_count
    if independent is not None:
        document["independent"] = independent
    if max_age_seconds is not None:
        document["freshness"] = {
            "as_of": datetime.now(timezone.utc).isoformat(),
            "max_age_seconds": max_age_seconds,
        }
    # Validate the caller contract before using credentials or doing any I/O.
    summarize_observation(document)
    if independent and len(independent["items"]) + max_items > MAX_ITEMS:
        raise ValueError("API and independent item bounds exceed the combined observation limit")
    headers = _headers(header_env)
    opener = build_opener(ProxyHandler({}), _NoRedirect())
    deadline, seen, diagnostics, item_count = time.monotonic() + total_seconds, set(), [], 0
    total_bytes = 0
    current, cursor = url, None
    for number in range(max_pages):
        page = {
            "source": _source(current),
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "cursor": cursor,
            "next_cursor": "unresolved",
            "status": "error",
            "items": [],
        }
        diagnostic = {"page": number + 1, "source": page["source"], "bytes_received": 0}
        document["pages"].append(page)
        diagnostics.append(diagnostic)
        if current in seen:
            diagnostic["error"] = "pagination_cycle"
            break
        seen.add(current)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            diagnostic["error"] = "time_budget"
            break
        try:
            request = Request(current, headers=headers, method="GET")
            with opener.open(request, timeout=min(request_timeout, remaining)) as response:
                diagnostic["http_status"] = response.status
                page.update(_cache_evidence(response.headers))
                if response.status != 200:
                    diagnostic["error"] = "unexpected_http_status"
                    break
                if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                    diagnostic["error"] = "unsupported_content_encoding"
                    break
                data = bytearray()
                while True:
                    if time.monotonic() >= deadline:
                        diagnostic["error"] = "time_budget"
                        break
                    chunk = response.read1(
                        min(
                            65536,
                            max_response_bytes + 1 - len(data),
                            max_total_bytes + 1 - total_bytes,
                        )
                    )
                    if not chunk:
                        break
                    data.extend(chunk)
                    total_bytes += len(chunk)
                    diagnostic["bytes_received"] += len(chunk)
                    if len(data) > max_response_bytes:
                        diagnostic["error"] = "response_too_large"
                        break
                    if total_bytes > max_total_bytes:
                        diagnostic["error"] = "total_byte_limit"
                        break
                if "error" in diagnostic:
                    break
                length = response.headers.get("Content-Length")
                if length is not None and (not length.isdecimal() or int(length) != len(data)):
                    diagnostic["error"] = "truncated_response"
                    break
            body = json.loads(data)
        except HTTPError as exc:
            diagnostic.update(
                http_status=exc.code,
                error="redirect_rejected" if 300 <= exc.code < 400 else "http_error",
            )
            exc.close()
            break
        except (TimeoutError, URLError, OSError, http.client.HTTPException):
            diagnostic["error"] = "transport_error"
            break
        except (ValueError, UnicodeError, RecursionError):
            diagnostic["error"] = "invalid_json"
            break
        if not isinstance(body, dict) or not isinstance(body.get("data"), list):
            diagnostic["error"] = "invalid_collection_shape"
            break
        rows = body["data"]
        page["status"] = "ok"
        for row in rows[: max_items - item_count]:
            item = {}
            if isinstance(row, dict):
                identity = row.get("id")
                if type(identity) is int or (
                    isinstance(identity, str) and 0 < len(identity) <= 256
                ):
                    item["id"] = identity
                for metric in required_metrics:
                    if metric in row:
                        value = row[metric]
                        item[metric] = (
                            value
                            if value is None or type(value) is int and value >= 0
                            else {"invalid_type": type(value).__name__}
                        )
            page["items"].append(item)
        item_count += len(page["items"])
        if len(page["items"]) != len(rows):
            diagnostic["error"] = "item_limit"
            break
        paging = body.get("paging")
        if not isinstance(paging, dict) or type(paging.get("is_end")) is not bool:
            diagnostic["error"] = "pagination_metadata_missing"
            break
        if paging["is_end"]:
            page["next_cursor"] = None
            break
        next_link = paging.get("next")
        if not isinstance(next_link, str) or not next_link:
            diagnostic["error"] = "next_page_missing"
            break
        next_url = urljoin(current, next_link)
        try:
            if _origin(next_url) != origin:
                raise ValueError("origin changed")
        except ValueError:
            diagnostic["error"] = "next_page_origin_rejected"
            break
        if urlsplit(next_url).path != urlsplit(url).path:
            diagnostic["error"] = "next_page_scope_rejected"
            break
        page["next_cursor"] = _cursor(next_url)
        if item_count >= max_items:
            diagnostic["error"] = "item_limit"
            break
        if number + 1 == max_pages:
            diagnostic["error"] = "page_limit"
            break
        current, cursor = next_url, page["next_cursor"]
    for page, diagnostic in zip(document["pages"], diagnostics):
        for field in ("http_status", "error"):
            if field in diagnostic:
                page[field] = diagnostic[field]
        if diagnostic.get("error") in {
            "truncated_response",
            "response_too_large",
            "total_byte_limit",
            "item_limit",
        }:
            page["truncated"] = True
    if "freshness" in document:
        document["freshness"]["as_of"] = datetime.now(timezone.utc).isoformat()
    summary = summarize_observation(document)
    return {"observation": document, "summary": summary, "fetches": diagnostics}


def collect_zhihu_answers(
    subject: str, *, base_url: str = "https://www.zhihu.com", page_size: int = 20, **options
) -> dict:
    """Collect answers for an explicit member ID/url_token, never infer /me.

    Compatibility wrapper for the old collector's members/{subject}/answers
    endpoint. Missing counters remain unknown; a detail endpoint or independent
    observation may be needed. It does not claim articles, notifications, all
    account activity, or current production API compatibility were checked.
    """
    if not isinstance(subject, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", subject):
        raise ValueError("subject must be an explicit member ID or url_token")
    if type(page_size) is not int or not 1 <= page_size <= 100:
        raise ValueError("page_size must be between 1 and 100")
    _origin(base_url)
    parsed = urlsplit(base_url)
    if parsed.path not in {"", "/"} or parsed.query:
        raise ValueError("base_url must be an origin without a path or query")
    url = (
        base_url.rstrip("/")
        + f"/api/v4/members/{quote(subject, safe='')}/answers?"
        + urlencode({"offset": 0, "limit": page_size})
    )
    return collect_collection(url, subject=subject, collection="answers", **options)
