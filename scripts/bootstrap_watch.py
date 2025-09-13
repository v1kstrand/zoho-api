# scripts/bootstrap_watch.py
import os, json, requests
from dotenv import load_dotenv
from app.api_client import get_access_token

load_dotenv()
VERIFY_TOKEN = os.environ["VERIFY_TOKEN"]
WEBHOOK_URL  = os.environ["WEBHOOK_URL"]
CHANNEL_ID   = int(os.environ["CHANNEL_ID"])  # <-- numeric big integer

def main():
    at, api = get_access_token()
    url = f"{api}/bigin/v1/actions/watch"
    payload = {
        "watch": [{
            "channel_id": CHANNEL_ID,  # numeric, not string
            "token": VERIFY_TOKEN,
            "notify_url": f"{WEBHOOK_URL.rstrip('/')}/bigin-webhook",
            "events": ["Contacts.create", "Contacts.edit"]
        }]
    }
    r = requests.post(url, json=payload,
                      headers={"Authorization": f"Zoho-oauthtoken {at}"},
                      timeout=20)
    if not r.ok:
        print("Status:", r.status_code, "Body:", r.text)
    r.raise_for_status()
    print(json.dumps(r.json(), indent=2))

if __name__ == "__main__":
    main()
