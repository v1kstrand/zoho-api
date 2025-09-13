# app/server.py
import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv  # pip install python-dotenv
from app.api_client import bigin_post

load_dotenv()

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "")

app = FastAPI()

@app.get("/healthz")
def health():
    return {"ok": True}

@app.post("/bigin-webhook")
async def bigin_webhook(req: Request):
    try:
        body = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if not VERIFY_TOKEN or body.get("token") != VERIFY_TOKEN:
        raise HTTPException(status_code=401, detail="Bad verify token")

    ids = body.get("ids") or body.get("payload", {}).get("ids") or []
    created = []

    for rid in ids:
        # Add a Note on the Contact
        note_payload = {
            "data": [{
                "Note_Title": "VDS Webhook",
                "Note_Content": "Auto note: webhook received and processed."
            }]
        }
        # Use the per-record Notes endpoint; simpler payload.
        # Endpoint: POST /{module}/{record_id}/Notes (Bigin Notes API). :contentReference[oaicite:0]{index=0}
        bigin_post(f"Contacts/{rid}/Notes", note_payload)
        created.append(rid)

    return JSONResponse({"ok": True, "received": ids, "noted": created})
