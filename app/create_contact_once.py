# app/create_contact_once.py
from datetime import datetime
from .api_client import bigin_post

def main():
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    email = f"webhooktest+{ts}@vdsai.se"
    payload = {
        "data": [{
            "Last_Name": f"Webhook Test {ts}",
            "Email": email,
            "Description": f"watch trigger at {ts}Z"
        }],
        "trigger": []  # disables workflows/emails
    }
    res = bigin_post("Contacts", payload)
    cid = res["data"][0]["details"]["id"]
    print("Created Contact ID:", cid)

if __name__ == "__main__":
    main()
