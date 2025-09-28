import os
from urllib.parse import urlsplit, parse_qs
from collections import defaultdict
from datetime import datetime, timedelta
import requests
from app import api_client

GOAT_API_KEY = os.environ['GOAT_API_KEY']
BASE = "https://v1kstrand.goatcounter.com/api/v0"
HEAD = {"Authorization": f"Bearer {GOAT_API_KEY}", "Content-Type": "application/json"}

def _gc_get(path, **params):
    r = requests.get(f"{BASE}{path}", headers=HEAD, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def id_counts(start: str | None = None, end: str | None = None, *, key="id", limit=100) -> dict[str, int]:
    if limit > 100:  # API caps hits limit at 100
        limit = 100
    params = {"limit": limit}
    if end is None:
        end = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    
    if start and end:
        params["start"] = start     # e.g. "2025-09-01T00:00:00Z" or 
        params["end"] = end

    out = defaultdict(int)
    exclude = []  # list of path IDs we've already processed

    while True:
        if exclude:
            params["exclude_paths"] = exclude
        data = _gc_get("/stats/hits", **params)
        rows = data.get("hits", data)

        for row in rows:
            path = row.get("path") or ""
            q = parse_qs(urlsplit(path).query)
            if key in q and q[key]:
                k = q[key][0].lstrip("$")
                # docs call this field “count” = visitors for the range
                out[k] += int(row.get("views") or row.get("count") or 0)

        if not data.get("more"):
            break
        exclude = [r.get("path_id") or r.get("id") for r in rows if (r.get("path_id") or r.get("id"))]
    return {int(k): v for k, v in dict(out).items()} 

def update_df():
    df = api_client.get_df()
    counts = id_counts(start="2025-09-01")
    df["num_visits"] = df["auto_number"].map(counts).fillna(0).astype(int)
    api_client.save_df()
    
if __name__ == "__main__":
    update_df()
    