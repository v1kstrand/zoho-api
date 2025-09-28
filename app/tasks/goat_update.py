from __future__ import annotations

from app import api_client
from app.goat_util import hit_counts_by_query


def update_df(start: str = "2025-09-01", end: str | None = None) -> None:
    """Refresh the num_visits column using GoatCounter hit stats."""
    df = api_client.get_df()
    counts = hit_counts_by_query(start=start, end=end)
    df["num_visits"] = df["auto_number"].map(counts).fillna(0).astype(int)
    api_client.save_df()


if __name__ == "__main__":
    update_df()