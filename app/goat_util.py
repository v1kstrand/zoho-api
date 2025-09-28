from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Callable, Iterator
from urllib.parse import parse_qs, urlsplit

import requests

DEFAULT_BASE = "https://v1kstrand.goatcounter.com/api/v0"
DEFAULT_LIMIT = 100
DEFAULT_TIMEOUT = 30


def goat_headers(api_key: str | None = None) -> dict[str, str]:
    """Return GoatCounter request headers, loading the API key when needed."""
    key = api_key or os.getenv("GOAT_API_KEY")
    if not key:
        raise RuntimeError("GOAT_API_KEY is required for GoatCounter calls")
    return {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}


def goat_get(
    path: str,
    *,
    params: dict[str, Any] | None = None,
    base: str = DEFAULT_BASE,
    api_key: str | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Any:
    """Perform a GoatCounter GET request and return the parsed JSON payload."""
    response = requests.get(
        f"{base}{path}",
        headers=goat_headers(api_key),
        params=params,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def _default_end_date() -> str:
    return (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")


def iterate_hits(
    *,
    start: str | None = None,
    end: str | None = None,
    limit: int = DEFAULT_LIMIT,
    base: str = DEFAULT_BASE,
    api_key: str | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Iterator[dict[str, Any]]:
    """Yield paginated hit rows from GoatCounter's /stats/hits endpoint."""
    params: dict[str, Any] = {"limit": min(limit, DEFAULT_LIMIT)}
    if start:
        params["start"] = start
    if start and not end:
        end = _default_end_date()
    if end:
        params["end"] = end

    exclude: list[str] = []
    while True:
        if exclude:
            params["exclude_paths"] = exclude
        elif "exclude_paths" in params:
            params.pop("exclude_paths")

        payload = goat_get(
            "/stats/hits",
            params=params,
            base=base,
            api_key=api_key,
            timeout=timeout,
        )
        rows = payload.get("hits", payload) or []

        next_exclude: list[str] = []
        for row in rows:
            yield row
            rid = row.get("path_id") or row.get("id")
            if rid:
                next_exclude.append(rid)

        if not payload.get("more"):
            break
        exclude = next_exclude


def hit_counts_by_query(
    key: str = "id",
    *,
    coerce: Callable[[str], Any] | None = int,
    start: str | None = None,
    end: str | None = None,
    limit: int = DEFAULT_LIMIT,
    base: str = DEFAULT_BASE,
    api_key: str | None = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[Any, int]:
    """Aggregate hit counts by a query-string key (e.g. id) from GoatCounter."""
    counts: defaultdict[Any, int] = defaultdict(int)

    for row in iterate_hits(
        start=start,
        end=end,
        limit=limit,
        base=base,
        api_key=api_key,
        timeout=timeout,
    ):
        path = row.get("path") or ""
        values = parse_qs(urlsplit(path).query).get(key)
        if not values:
            continue

        raw = str(values[0] or "").lstrip("$")
        if not raw:
            continue

        if coerce is None:
            label = raw
        else:
            try:
                label = coerce(raw)
            except (TypeError, ValueError):
                continue

        views = int(row.get("views") or row.get("count") or 0)
        counts[label] += views

    return dict(counts)


__all__ = [
    "DEFAULT_BASE",
    "DEFAULT_LIMIT",
    "DEFAULT_TIMEOUT",
    "goat_headers",
    "goat_get",
    "iterate_hits",
    "hit_counts_by_query",
]