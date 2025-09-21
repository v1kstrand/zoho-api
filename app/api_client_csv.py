from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from dotenv import load_dotenv
load_dotenv()

import pandas as pd

JSON = Dict[str, Any]

BASE_DIR = Path.cwd()
CSV_PATH = Path(os.environ.get("CONTACTS_CSV_PATH"))

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
    "unsub",
    "into_date",
    "dfu1_date",
    "dfu2_date",
]
EMAIL_COLUMN = "email"
NOTES_COLUMN = "notes"
CASE_INS = True


class ContactStore:
    """In-memory contact store backed by a CSV file (email is the key)."""

    def __init__(self, csv_path: Path = CSV_PATH) -> None:
        self.path = Path(csv_path)
        self._df: pd.DataFrame | None = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _empty_frame() -> pd.DataFrame:
        """Return an empty contacts DataFrame with the canonical schema."""
        return pd.DataFrame(columns=COLUMNS).astype(str)

    @staticmethod
    def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Ensure the DataFrame contains all expected columns in order."""
        for column in COLUMNS:
            if column not in df.columns:
                df[column] = ""
        return df[COLUMNS]

    def _load_from_disk(self) -> pd.DataFrame:
        """Read the CSV from disk (or build an empty frame when missing)."""
        if not self.path.exists():
            return self._empty_frame()
        df = pd.read_csv(self.path, dtype=str, keep_default_na=False)
        if df.empty:
            return self._empty_frame()
        df = df.fillna("")
        return self._ensure_columns(df).astype(str)

    def _ensure_loaded(self) -> pd.DataFrame:
        """Guarantee that the DataFrame is loaded into memory."""
        if self._df is None:
            self._df = self._load_from_disk()
        return self._df

    def refresh(self) -> pd.DataFrame:
        """Reload the CSV from disk and return the live DataFrame."""
        self._df = self._load_from_disk()
        return self._df

    def _save(self) -> None:
        """Persist the in-memory DataFrame back to the CSV."""
        if self._df is None:
            return
        df = self._ensure_columns(self._df.copy())
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.path, index=False)

    @staticmethod
    def _coerce(value: Any) -> str:
        """Convert arbitrary values into string form for storage."""
        return "" if value is None else str(value)

    def _row_dict(self, series: pd.Series, columns: Optional[List[str]] = None) -> JSON:
        """Serialize a pandas row into a plain dict keyed by columns."""
        cols = columns or COLUMNS
        return {col: self._coerce(series.get(col, "")) for col in cols}

    @staticmethod
    def _normalize_email(value: str) -> str:
        """Lowercase and trim an email value for comparison."""
        return (value or "").strip().lower()
    
    def _get_now(self):
        return pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

    def _row_index_by_email(self, email: str) -> Optional[int]:
        """Return the index of the row matching the given email if present."""
        if not email:
            return None
        df = self._ensure_loaded()
        matches = df[EMAIL_COLUMN].astype(str).str.strip().str.lower() == self._normalize_email(email)
        idx = matches[matches].index
        return int(idx[0]) if len(idx) else None

    def _next_auto_number(self) -> str:
        """Return the next sequential auto number as a string."""
        df = self._ensure_loaded()
        if df.empty or "auto_number" not in df.columns:
            return "1"
        numbers = pd.to_numeric(df["auto_number"], errors="coerce")
        numbers = numbers[numbers.notna()]
        current = numbers.max() if not numbers.empty else 0
        return str(int(current) + 1)

    def _update_row(self, idx: int, data: JSON) -> None:
        """Apply the provided fields to an existing row."""
        df = self._ensure_loaded()
        for key, value in data.items():
            if key in COLUMNS:
                df.at[idx, key] = self._coerce(value)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def iter_contacts(self, *, fields: Optional[List[str]] = None) -> Iterator[JSON]:
        """Yield contact rows as dicts, optionally limited to a subset of columns."""
        df = self._ensure_loaded()
        subset: pd.DataFrame
        if fields:
            for name in fields:
                if name not in COLUMNS:
                    raise KeyError(f"Unknown column {name!r}; expected one of {COLUMNS}")
            subset = df[fields]
        else:
            subset = df
        for _, row in subset.iterrows():
            yield self._row_dict(row, list(subset.columns))

    def list_contacts(self, *, fields: Optional[List[str]] = None) -> List[JSON]:
        """Return all contacts as a list of dicts."""
        return list(self.iter_contacts(fields=fields))

    def find_contact_by_email(self, email: str) -> Optional[JSON]:
        """Fetch a contact row by email, returning None when absent."""
        idx = self._row_index_by_email(email)
        if idx is None:
            return None
        df = self._ensure_loaded()
        return self._row_dict(df.loc[idx])

    def add_contact(self, data: JSON) -> JSON:
        """Append a new contact row, assigning id/auto number when missing."""
        if not isinstance(data, dict) or not data:
            raise ValueError("Provide contact fields as a non-empty dict")

        unknown = [key for key in data.keys() if key not in COLUMNS]
        if unknown:
            raise ValueError(f"Unknown column(s) {unknown!r}; expected one of {COLUMNS}")

        df = self._ensure_loaded()
        payload = {key: self._coerce(value) for key, value in data.items()}

        email_value = self._normalize_email(payload.get(EMAIL_COLUMN, ""))
        if not email_value:
            raise ValueError("Email is required to add a contact")
        payload[EMAIL_COLUMN] = email_value

        payload["id"] = uuid.uuid4().hex
        payload["auto_number"] = self._next_auto_number()
        payload["created_time"] = self._get_now()

        idx = self._row_index_by_email(email_value)
        if idx is not None:
            raise ValueError(f"Contact with email {payload['email']} already exists")

        row = {col: "" for col in COLUMNS}
        row.update(payload)
        df.loc[len(df)] = row
        self._save()
        return self._row_dict(df.loc[len(df) - 1])

    def update_contact_by_email(self, email: str, fields: JSON) -> JSON:
        """Update an existing contact by email with the supplied fields."""
        if not (email and isinstance(fields, dict) and fields):
            raise ValueError("Provide email and non-empty fields to update")

        payload = {key: self._coerce(value) for key, value in fields.items() if key in COLUMNS}
        if not payload:
            raise ValueError("No recognised fields supplied")

        idx = self._row_index_by_email(email)
        if idx is None:
            raise ValueError(f"Contact with email={email!r} not found")

        self._update_row(idx, payload)
        self._save()
        df = self._ensure_loaded()
        return self._row_dict(df.loc[idx])

    def filter_contacts(self, criteria: Dict[str, Any]) -> List[JSON]:
        """Return rows whose columns match the given criteria dict."""
        if not isinstance(criteria, dict):
            raise ValueError("criteria must be a dict of column:value pairs")
        if not criteria:
            return list(self.iter_contacts())
        df = self._ensure_loaded()
        mask = pd.Series(True, index=df.index)
        for column, value in criteria.items():
            if column not in COLUMNS:
                raise KeyError(f"Unknown column {column!r}; expected one of {COLUMNS}")
            series = df[column].astype(str)
            target = self._coerce(value)

            if CASE_INS:
                series = series.str.casefold()
                target = target.casefold()

            mask &= series == target
        subset = df[mask]
        return [self._row_dict(row) for _, row in subset.iterrows()]

    def append_contact_note(self, email: str, note: str) -> JSON:
        """Append a textual note to the contact's notes column."""
        note = (note or "").strip()
        if not note:
            raise ValueError("Note content must be non-empty")

        idx = self._row_index_by_email(email)
        if idx is None:
            raise ValueError(f"Contact with email={email!r} not found")

        df = self._ensure_loaded()
        existing = self._coerce(df.at[idx, NOTES_COLUMN])
        df.at[idx, NOTES_COLUMN] = f"{existing};{note}".strip() if existing else note
        self._save()
        return self._row_dict(df.loc[idx])



# Global store instance -------------------------------------------------
_store = ContactStore()


def find_contact_by_email(email: str) -> Optional[JSON]:
    """Fetch a contact row by email, returning None when absent."""
    return _store.find_contact_by_email(email)


def add_contact(data: JSON) -> JSON:
    """Append a new contact row with email as the unique key."""
    return _store.add_contact(data)


def update_contact(email: str, fields: JSON) -> JSON:
    """Update an existing contact by email with the supplied fields."""
    return _store.update_contact_by_email(email, fields)


def get_contact_field(email: str, field: str) -> str:
    """Return a single column value for the given email."""
    contact = _store.find_contact_by_email(email)
    assert contact is not None, f"Contact with email={email} not found"
    assert field in contact, f"Unknown field: {field}"
    return contact[field]

def filter_contacts(criteria: Dict[str, Any]) -> List[JSON]:
    """Return rows whose columns match the given criteria dict."""
    return _store.filter_contacts(criteria)

def append_contact_note(email: str, note: str) -> JSON:
    """Append a textual note to the contact's notes column."""
    return _store.append_contact_note(email, note)


# Compatibility aliases (email-only API)
search_contact_by_email = find_contact_by_email
update_contact_fields = update_contact

__all__ = [
    "append_contact_note",
    "find_contact_by_email",
    "search_contact_by_email",
    "update_contact",
    "update_contact_fields",
    "get_contact_field",
    "ContactStore",
]


