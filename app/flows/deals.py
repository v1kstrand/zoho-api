# app/flows/deals_find.py
import argparse, json, sys
from ..api_client import (
    list_deals_by_contact_email,
    list_deals_by_contact_id,
    first_deal_by_contact_email,
    first_deal_by_contact_id,
)

def main():
    ap = argparse.ArgumentParser(description="Find Pipelines/Deals for a contact (by email or id).")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--email", help="Contact email")
    g.add_argument("--id", help="Contact ID")
    ap.add_argument("--first", action="store_true", help="Return only the first matching deal")
    ap.add_argument("--fields", help="Comma-separated fields to print (optional)")
    args = ap.parse_args()

    try:
        if args.first:
            deal = first_deal_by_contact_email(args.email) if args.email else first_deal_by_contact_id(args.id)
            if not deal:
                print(json.dumps({"ok": True, "count": 0, "deal": None}))
                return
            if args.fields:
                picks = [f.strip() for f in args.fields.split(",") if f.strip()]
                deal = {k: deal.get(k) for k in picks}
            print(json.dumps({"ok": True, "count": 1, "deal": deal}, indent=2, ensure_ascii=False))
        else:
            deals = list_deals_by_contact_email(args.email) if args.email else list_deals_by_contact_id(args.id)
            if args.fields:
                picks = [f.strip() for f in args.fields.split(",") if f.strip()]
                deals = [{k: d.get(k) for k in picks} for d in deals]
            print(json.dumps({"ok": True, "count": len(deals), "deals": deals}, indent=2, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}), file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
