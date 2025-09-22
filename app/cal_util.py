import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Iterable, Optional
import os

CAL_BASE = "https://api.cal.com/v2"
CAL_API_VERSION = "2024-08-13"  # required
CAL_API_KEY = os.environ["CAL_API_KEY"]
CAL_CREATED_WITHIN = int(os.environ["CAL_CREATED_WITHIN"])

def _iso_to_dt(s: str) -> datetime:
    # "2025-09-22T12:00:00Z" → aware UTC dt
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

def cancel_all_bookings(
    reason: str = "Host-initiated cancellation",
    only_future: bool = True,
    statuses: Optional[Iterable[str]] = None,   # e.g. {"accepted","confirmed","pending"}
    cancel_subsequent_bookings: bool = True,    # for recurring (non-seated) series
    dry_run: bool = True,
    verbose: bool = True,
) -> dict:
    """
    Cancel Cal.com bookings for the authenticated user/account.

    Returns a summary dict {scanned:int, attempted:int, cancelled:int, skipped:int, errors:int}.
    """
    headers = {
        "Authorization": f"Bearer {CAL_API_KEY}",       # token must start with cal_
        "cal-api-version": CAL_API_VERSION,
        "Content-Type": "application/json",
    }

    take = 100
    skip = 0
    scanned = attempted = cancelled = skipped = errors = 0

    while True:
        # GET /v2/bookings with pagination
        r = requests.get(
            f"{CAL_BASE}/bookings",
            headers=headers,
            params={"take": take, "skip": skip},
            timeout=30,
        )
        r.raise_for_status()
        payload = r.json()
        data = payload.get("data", [])
        scanned += len(data)

        for bk in data:
            uid   = bk.get("uid")
            stat  = bk.get("status")  # e.g. "accepted"
            start = bk.get("start")   # ISO string

            # Skip already canceled
            if (stat or "").lower() in {"canceled", "cancelled"}:
                skipped += 1
                if verbose:
                    print(f"skip  uid={uid} status={stat}")
                continue

            # Optional status filter
            if statuses and str(stat).lower() not in {s.lower() for s in statuses}:
                skipped += 1
                if verbose:
                    print(f"skip  uid={uid} status={stat} (not in filter)")
                continue

            # Only future?
            if only_future and isinstance(start, str):
                try:
                    if _iso_to_dt(start) <= datetime.now(timezone.utc):
                        skipped += 1
                        if verbose:
                            print(f"skip  uid={uid} start={start} (past)")
                        continue
                except Exception:
                    pass  # if we can't parse, fall through and attempt

            attempted += 1
            if dry_run:
                if verbose:
                    print(f"DRY   uid={uid} would cancel (status={stat}, start={start})")
                continue

            # POST /v2/bookings/{uid}/cancel
            body = {
                "cancellationReason": reason,
                "cancelSubsequentBookings": bool(cancel_subsequent_bookings),
            }
            try:
                cr = requests.post(
                    f"{CAL_BASE}/bookings/{uid}/cancel",
                    headers=headers,
                    json=body,
                    timeout=30,
                )
                if cr.status_code // 100 == 2:
                    cancelled += 1
                    if verbose:
                        print(f"OK    uid={uid} cancelled")
                else:
                    errors += 1
                    if verbose:
                        print(f"ERR   uid={uid} {cr.status_code} {cr.text}")
            except Exception as e:
                errors += 1
                if verbose:
                    print(f"ERR   uid={uid} exception: {e}")

        # Pagination
        pag = payload.get("pagination") or {}
        if not pag.get("hasNextPage"):
            break
        skip += pag.get("itemsPerPage", take)

    return {
        "scanned": scanned,
        "attempted": attempted,
        "cancelled": cancelled,
        "skipped": skipped,
        "errors": errors,
        "dry_run": dry_run,
    }
    
    
def get_bookings_created_within(
    hours: float = CAL_CREATED_WITHIN,
    take: int = 100,
    timeout_s: int = 60,
) -> List[Dict[str, Any]]:
    since_utc = datetime.now(timezone.utc) - timedelta(hours=hours)
    since_iso = since_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    headers = {
        "Authorization": f"Bearer {CAL_API_KEY}",
        "cal-api-version": CAL_API_VERSION,
    }

    out: List[Dict[str, Any]] = []
    skip = 0
    while True:
        params: Dict[str, str] = {
            "take": str(take),
            "afterCreatedAt": since_iso,     # server-side filter by created time
            "sortCreated": "desc",           # newest first
            "skip" : str(skip)
        }

        r = requests.get(f"{CAL_BASE}/bookings", headers=headers, params=params, timeout=timeout_s)
        r.raise_for_status()
        payload = r.json()
        data = payload.get("data", [])
        out.extend(data)
        pag = payload.get("pagination") or {}
        if not pag.get("hasNextPage"):
            break
        skip += pag.get("itemsPerPage", take)

    return out