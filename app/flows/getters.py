# app/flows/getters.py
import argparse, json, sys
from ..api_client import get_contact_by_id, get_contact_by_email

def main():
    ap = argparse.ArgumentParser(description="Fetch a Bigin Contact by id or email.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--id", help="Bigin Contact record ID")
    g.add_argument("--email", help="Contact email")
    args = ap.parse_args()

    try:
        if args.id:
            rec = get_contact_by_id(args.id)
        else:
            rec = get_contact_by_email(args.email)

        if not rec:
            print(json.dumps({"ok": False, "error": "CONTACT_NOT_FOUND"}), file=sys.stderr)
            sys.exit(2)

        print(json.dumps({"ok": True, "contact": rec}, indent=2, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}), file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
