# app/vds_bigin.py
import json
from .api_client import bigin_get

def main():
    data = bigin_get("settings/modules")
    mods = [{"api_name": m["api_name"], "module_name": m["module_name"]} for m in data["modules"]]
    print(json.dumps(mods, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
