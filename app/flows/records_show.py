import argparse, json, sys
from ..api_client import (
    get_record_by_id,
    get_full_records_by_contact_email,
    get_full_records_by_contact_id,
)

def main():
    ap = argparse.ArgumentParser(description="Print full pipeline record JSON.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--record-id", help="Pipeline record id (prints that record)")
    g.add_argument("--email", help="Contact email (prints linked record(s))")
    g.add_argument("--contact-id", help="Contact id (prints linked record(s))")
    ap.add_argument("--first", action="store_true", help="Print only the first linked record")
    ap.add_argument("--module", default="Pipelines", help='Module API name (default: "Pipelines")')
    args = ap.parse_args()

    try:
        if args.record_id:
            rec = get_record_by_id(args.record_id, module_api=args.module)
            print(json.dumps(rec or {}, indent=2, ensure_ascii=False))
            return

        if args.email:
            out = get_full_records_by_contact_email(args.email, first_only=args.first, module_api=args.module)
        else:
            out = get_full_records_by_contact_id(args.contact_id, first_only=args.first, module_api=args.module)

        print(json.dumps(out, indent=2, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}), file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
