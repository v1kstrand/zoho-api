import argparse, json, sys
from ..api_client import update_records_by_contact_email, update_records_by_contact_id

def _parse_sets(pairs):
    out = {}
    for raw in pairs:
        if "=" not in raw:
            raise ValueError(f"Bad --set pair (expected Field=Value): {raw!r}")
        k, v = raw.split("=", 1)
        out[k.strip()] = v.strip()
    return out

def main():
    ap = argparse.ArgumentParser(description="Update pipeline records linked to a Contact.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--email", help="Contact email")
    g.add_argument("--id", help="Contact ID")
    ap.add_argument("--set", action="append", required=True, help='Field=Value (repeatable). Example: --set "Stage=Qualification"')
    ap.add_argument("--first", action="store_true", help="Update only the first linked record")
    ap.add_argument("--module", default="Pipelines", help='Module API name (default: "Pipelines")')
    args = ap.parse_args()

    try:
        patch = _parse_sets(args.set)
        if args.email:
            res = update_records_by_contact_email(args.email, patch, module_api=args.module, first_only=args.first)
        else:
            res = update_records_by_contact_id(args.id, patch, module_api=args.module, first_only=args.first)
        print(json.dumps({"ok": True, "updated": patch, "result": res}, indent=2, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}), file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
