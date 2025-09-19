from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pandas as pd

JSON = Dict[str, Any]

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = Path(os.environ.get("CONTACTS_CSV_PATH") or (BASE_DIR / "data" / "contacts" / "contacts.csv"))

COLUMNS: List[str] = [
    "id",
    "contact_name",
    "company_name",
    "email",
    "first_name",
    "last_name",
    "contact_type",
    "created_time",
    "outreach_idx",
    "auto_number",
    "last_touch_date",
    "stage",
    "notes",
]
ID_COLUMN = "id"
EMAIL_COLUMN = "email"
NOTES_COLUMN = "notes"


def _empty_frame() -> pd.DataFrame:
    """Return an empty contacts DataFrame with the canonical schema."""
    return pd.DataFrame(columns=COLUMNS).astype(str)


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure the DataFrame contains all expected columns in order."""
    for column in COLUMNS:
        if column not in df.columns:
            df[column] = ""
    return df[COLUMNS]


def _load_contacts_df() -> pd.DataFrame:
    """Load the contacts CSV, returning an empty frame when missing."""
    if not CSV_PATH.exists():
        return _empty_frame()
    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False)
    if df.empty:
        return _empty_frame()
    df = df.fillna("")
    return _ensure_columns(df).astype(str)


def _save_contacts_df(df: pd.DataFrame) -> None:
    """Persist the contacts DataFrame back to disk."""
    df = _ensure_columns(df.copy())
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CSV_PATH, index=False)


def _coerce(value: Any) -> str:
    """Convert values into string form for storage."""
    if value is None:
        return ""
    return str(value)


def _row_dict(series: pd.Series, columns: Optional[List[str]] = None) -> JSON:
    """Serialize a pandas row into a plain dict keyed by columns."""
    cols = columns or COLUMNS
    return {col: _coerce(series.get(col, "")) for col in cols}


def _normalize_email(value: str) -> str:
    """Lowercase and trim an email value for comparison."""
    return (value or "").strip().lower()


def _row_index_by_email(df: pd.DataFrame, email: str) -> Optional[int]:
    """Return the index of the row matching the given email if present."""
    if not email:
        return None
    matches = df[EMAIL_COLUMN].astype(str).str.strip().str.lower() == _normalize_email(email)
    idx = matches[matches].index
    return int(idx[0]) if len(idx) else None


def _row_index_by_id(df: pd.DataFrame, contact_id: str) -> Optional[int]:
    """Return the index of the row matching the given contact id if present."""
    if not contact_id:
        return None
    matches = df[ID_COLUMN].astype(str) == str(contact_id)
    idx = matches[matches].index
    return int(idx[0]) if len(idx) else None


def iter_contacts(*, fields: Optional[List[str]] = None) -> Iterator[JSON]:
    """Yield contact rows as dicts, optionally limited to a subset of columns."""
    df = _load_contacts_df()
    if fields:
        for name in fields:
            if name not in COLUMNS:
                raise KeyError(f"Unknown column {name!r}; expected one of {COLUMNS}")
        subset = df[fields]
    else:
        subset = df
    for _, row in subset.iterrows():
        yield _row_dict(row, list(subset.columns))


def list_contacts(*, fields: Optional[List[str]] = None) -> List[JSON]:
    """Return all contacts as a list of dicts."""
    return list(iter_contacts(fields=fields))


def find_contact_by_email(email: str) -> Optional[JSON]:
    """Fetch a contact row by email, returning None when absent."""
    df = _load_contacts_df()
    idx = _row_index_by_email(df, email)
    if idx is None:
        return None
    return _row_dict(df.loc[idx])


def get_contact_by_id(contact_id: str) -> Optional[JSON]:
    """Fetch a contact row by contact id, returning None when absent."""
    df = _load_contacts_df()
    idx = _row_index_by_id(df, contact_id)
    if idx is None:
        return None
    return _row_dict(df.loc[idx])


def _assign_row(df: pd.DataFrame, idx: int, data: JSON) -> None:
    """Apply the provided fields to an existing row."""
    for key, value in data.items():
        if key in COLUMNS:
            df.at[idx, key] = _coerce(value)


def upsert_contact(data: JSON) -> JSON:
    """Insert or update a contact based on provided fields and email/id."""
    if not isinstance(data, dict) or not data:
        raise ValueError("Provide contact fields as a non-empty dict")

    filtered = {key: _coerce(value) for key, value in data.items() if key in COLUMNS}
    if not filtered:
        raise ValueError("No recognised fields supplied")

    df = _load_contacts_df()
    df = df.copy()

    email_value = _normalize_email(filtered.get(EMAIL_COLUMN, ""))
    contact_id = filtered.get(ID_COLUMN, "")

    idx = _row_index_by_email(df, email_value) if email_value else None
    if idx is None and contact_id:
        idx = _row_index_by_id(df, contact_id)

    if idx is not None:
        _assign_row(df, idx, filtered)
        _save_contacts_df(df)
        return _row_dict(df.loc[idx])

    if not contact_id:
        contact_id = f"csv_{uuid.uuid4().hex}"
        filtered[ID_COLUMN] = contact_id
    if email_value and EMAIL_COLUMN not in filtered:
        filtered[EMAIL_COLUMN] = email_value

    row = {col: "" for col in COLUMNS}
    row.update(filtered)
    df.loc[len(df)] = row
    _save_contacts_df(df)
    return _row_dict(df.loc[len(df) - 1])


def update_contact(contact_id: str, fields: JSON) -> JSON:
    """Update an existing contact by id with the supplied fields."""
    if not (contact_id and isinstance(fields, dict) and fields):
        raise ValueError("Provide contact_id and non-empty fields to update")

    payload = {key: _coerce(value) for key, value in fields.items() if key in COLUMNS}
    if not payload:
        raise ValueError("No recognised fields supplied")

    df = _load_contacts_df()
    df = df.copy()
    idx = _row_index_by_id(df, contact_id)
    if idx is None:
        raise ValueError(f"Contact with id={contact_id!r} not found")

    _assign_row(df, idx, payload)
    _save_contacts_df(df)
    return _row_dict(df.loc[idx])



def get_contact_field(contact_id: str, field: str) -> str:
    """Return a single column value for the given contact id ("" when unknown)."""
    if field not in COLUMNS:
        raise KeyError(f"Unknown column {field!r}; expected one of {COLUMNS}")
    df = _load_contacts_df()
    idx = _row_index_by_id(df, contact_id)
    if idx is None:
        return ""
    return _coerce(df.at[idx, field])


def append_contact_note(contact_id: str, note: str) -> JSON:
    """Append a textual note to the contact's notes column."""
    note = note.strip()
    if not note:
        raise ValueError("Note content must be non-empty")

    df = _load_contacts_df()
    df = df.copy()
    idx = _row_index_by_id(df, contact_id)
    if idx is None:
        raise ValueError(f"Contact with id={contact_id!r} not found")

    existing = _coerce(df.at[idx, NOTES_COLUMN])
    df.at[idx, NOTES_COLUMN] = f"{existing}\n{note}".strip() if existing else note
    _save_contacts_df(df)
    return _row_dict(df.loc[idx])


# Compatibility aliases
search_contact_by_email = find_contact_by_email
update_contact_fields = update_contact

__all__ = [
    "append_contact_note",
    "find_contact_by_email",
    "get_contact_by_id",
    "iter_contacts",
    "list_contacts",
    "search_contact_by_email",
    "update_contact",
    "update_contact_fields",
    "upsert_contact",
]
