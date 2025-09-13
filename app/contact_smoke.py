# app/contact_smoke.py
import json
from datetime import datetime
from .api_client import bigin_get, bigin_post, bigin_delete

def main():
    # 1) Create a unique test contact (workflows disabled via trigger = [])
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    email = f"apitest+{ts}@vdsai.se"
    payload = {
        "data": [{
            "Last_Name": "VDS API Test (DELETE ME)",
            "Email": email,
            "Description": f"Created by local test at {ts}Z"
        }],
        "trigger": []  # prevents workflows/emails
    }
    created = bigin_post("Contacts", payload)
    cid = created["data"][0]["details"]["id"]
    print("Created Contact ID:", cid)

    # 2) Fetch it back to verify
    got = bigin_get(f"Contacts/{cid}")
    row = got["data"][0]
    print("Fetched:", json.dumps({
        "id": row["id"],
        "Last_Name": row.get("Last_Name"),
        "Email": row.get("Email"),
        "Created_Time": row.get("Created_Time"),
    }, indent=2, ensure_ascii=False))

    # 3) Delete it (cleanup)
    deleted = bigin_delete(f"Contacts/{cid}")
    print("Delete status:", deleted["data"][0]["status"])

if __name__ == "__main__":
    main()
