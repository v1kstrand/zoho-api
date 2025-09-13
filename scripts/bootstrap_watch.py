# scripts/bootstrap_watch.py
import os, json, requests
from dotenv import load_dotenv
from app.api_client import get_access_token

load_dotenv()

# --- Required env vars ---
VERIFY_TOKEN = os.environ["VERIFY_TOKEN"].strip()         # keep ≤ ~50 chars
WEBHOOK_URL  = os.environ["WEBHOOK_URL"].strip()          # e.g. "https://abcd.ngrok.io"
CHANNEL_ID   = int(os.environ["CHANNEL_ID"])              # numeric big integer

# --- Optional: comma-separated events (defaults to Contacts create/edit) ---
WATCH_EVENTS = os.getenv("WATCH_EVENTS", "Contacts.create,Contacts.edit")
EVENTS = [e.strip() for e in WATCH_EVENTS.split(",") if e.strip()]

def main():
    # basic sanity: require https
    if not WEBHOOK_URL.lower().startswith("https://"):
        raise SystemExit("WEBHOOK_URL must be an HTTPS URL")

    access_token, api_base = get_access_token()
    url = f"{api_base.rstrip('/')}/bigin/v2/actions/watch"

    payload = {
        "watch": [{
            "channel_id": CHANNEL_ID,
            "token": VERIFY_TOKEN,
            "notify_url": f"{WEBHOOK_URL.rstrip('/')}/bigin-webhook",
            "events": EVENTS
        }]
    }

    r = requests.post(
        url,
        json=payload,
        headers={"Authorization": f"Zoho-oauthtoken {access_token}"},
        timeout=20,
    )

    if not r.ok:
        print("Status:", r.status_code, "Body:", r.text)
    r.raise_for_status()
    print(json.dumps(r.json(), indent=2))

if __name__ == "__main__":
    main()
