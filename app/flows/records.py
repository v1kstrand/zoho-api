# app/flows/records.py
import argparse, json, sys
from typing import Optional, Dict, Any, List

# Uses the generic helpers you added in app/api_client.py
from ..api_client import (
    get_record_by_id,
    search_record_by_email,
    search_record_first,
)

ALLOWED_OPERATORS = {"equals", "starts_with", "contains", "ends_with"}

def _subset(rec: Dict[str, Any], fields_csv: Optional[str]) -> Dict[str, Any]:
    if not fields_csv:
        return rec
    picks: List[str] = [f.strip() for f in fields_csv.split(",") if f.strip()]
    return {k: rec.get(k) for k in picks}

def main():
    ap = argparse.ArgumentParser(
        description="Fetch a Bigin record by id, email, or field/value."
    )
    ap.add_argument(
        "--module", required=True,
        help="Module API name (e.g., Contacts, Accounts, Pipelines)"
    )

    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--id", help="Record ID")
    g.add_argument("--email", help="Email value (defaults to Email field)")
    g.add_argument("--field", help="Field API name for generic search")

    ap.add_argument("--value", help="Value to search for (required with --field)")
    ap.add_argument(
        "--operator", default="equals", choices=sorted(ALLOWED_OPERATORS),
        help="Search operator for --field (default: equals)"
    )
    ap.add_argument(
        "--email-field", default="Email",
        help="Email field API name to use with --email (default: Email)"
    )
    ap.add_argument(
        "--fields",
        help="Comma-separated list of fields to print (optional). "
             "If omitted, prints the full record."
    )

    args = ap.parse_args()

    try:
        if args.id:
            rec = get_record_by_id(args.module, args.id)
        elif args.email:
            rec = search_record_by_email(args.module, args.email, args.email_field)
        else:
            if not args.value:
                raise SystemExit("--field requires --value")
            if args.operator not in ALLOWED_OPERATORS:
                raise SystemExit(f"Unsupported operator: {args.operator}")
            rec = search_record_first(args.module, args.field, args.value, args.operator)

        if not rec:
            print(json.dumps({"ok": False, "error": "RECORD_NOT_FOUND"}), file=sys.stderr)
            sys.exit(2)

        out = _subset(rec, args.fields)
        print(json.dumps({"ok": True, "module": args.module, "record": out}, indent=2, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}), file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
