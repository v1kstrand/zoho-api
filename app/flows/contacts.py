# app/flows/contacts.py
import argparse, sys, json
from typing import Dict
from ..api_client import search_contact_by_email, update_contact_fields

def update_contact_by_email(email: str, fields: Dict[str, str]) -> Dict:
    """
    Given an email -> find first matching Contact -> update fields (workflows disabled).
    Returns the raw API response (dict). Raises if contact not found.
    """
    row = search_contact_by_email(email)
    if not row:
        raise RuntimeError(f"CONTACT_NOT_FOUND for email: {email}")
    return update_contact_fields(row["id"], fields)

# ---- CLI entrypoint (so you can run: py -m app.flows.contacts --email ... --set Field=Val) ----
def _parse_sets(pairs):
    out = {}
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f'--set expects Field=Value, got: {pair}')
        k, v = pair.split("=", 1)
        out[k.strip()] = v
    return out

def main():
    ap = argparse.ArgumentParser(description="Update a Bigin Contact by email.")
    ap.add_argument("--email", required=True, help="Contact email to search")
    ap.add_argument("--set", action="append", required=True,
                    help='Field=Value (repeatable). Example: --set "Description=Updated by flows"')
    args = ap.parse_args()

    try:
        fields = _parse_sets(args.set)
        res = update_contact_by_email(args.email, fields)
        print(json.dumps({"ok": True, "email": args.email, "updated": fields, "api": res}, indent=2))
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e), "email": args.email}), file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()

