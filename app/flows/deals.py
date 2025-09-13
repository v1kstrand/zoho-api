# app/flows/deals.py
import argparse, json, sys
from ..api_client import get_deals_for_contact_id, get_deals_for_contact_email

def main():
    ap = argparse.ArgumentParser(description="List Deals (pipeline records) for a Contact.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--id", help="Contact ID")
    g.add_argument("--email", help="Contact email")
    ap.add_argument("--full", action="store_true", help="Print full records (default prints a compact view)")
    args = ap.parse_args()

    try:
        deals = get_deals_for_contact_id(args.id) if args.id else get_deals_for_contact_email(args.email)
        if not args.full:
            slim = []
            for d in deals or []:
                slim.append({
                    "id": d.get("id"),
                    "Deal_Name": d.get("Deal_Name") or d.get("Deal_Name__s") or d.get("Name"),
                    "Stage": d.get("Stage"),
                    "Pipeline": d.get("Pipeline"),
                    "Amount": d.get("Amount"),
                    "Closing_Date": d.get("Closing_Date"),
                })
            print(json.dumps({"ok": True, "count": len(deals or []), "deals": slim}, indent=2, ensure_ascii=False))
        else:
            print(json.dumps({"ok": True, "count": len(deals or []), "deals": deals}, indent=2, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}), file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
