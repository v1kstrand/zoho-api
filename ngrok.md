Here’s a tight “what we set up & how to run it” you can paste into another chat.

# Ngrok + Bigin webhook setup (current project)

## 1) What’s running locally

* **FastAPI server** on `http://127.0.0.1:8001` with:

  * `GET /healthz` → simple health check
  * `POST /bigin-webhook` → receives Zoho Bigin notifications. It validates a shared `VERIFY_TOKEN` from `.env`.
* **Zoho client** code handles OAuth token refresh from `tokens.json` and calls Bigin v2 APIs.

## 2) Required local files & env

* `.env` (not committed):

  ```
  Z_CLIENT_ID=...
  Z_CLIENT_SECRET=...
  VERIFY_TOKEN=<random-long-string>
  WEBHOOK_URL=<filled after you start ngrok>
  CHANNEL_ID=<big unique integer, e.g. 20250913150001>
  ```

  Tip to generate values:

  ```powershell
  python - <<'PY'
  import secrets, time, random
  print("VERIFY_TOKEN:", secrets.token_urlsafe(32))
  print("CHANNEL_ID:", int(time.time()*1000)*1000 + random.randint(0,999))
  PY
  ```
* `tokens.json` (not committed):

  ```json
  {
    "refresh_token": "1000.xxxxx...", 
    "api_domain": "https://www.zohoapis.eu"
  }
  ```

  (We’re using the **EU** DC: `accounts.zoho.eu` and `www.zohoapis.eu`.)

## 3) Start the local server

```powershell
python -m uvicorn app.server:app --reload --port 8001
# expect: "Uvicorn running on http://127.0.0.1:8001"
```

Quick local checks:

```powershell
Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:8001/healthz"
$body = @{ token = $env:VERIFY_TOKEN; ids = @("TEST") } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8001/bigin-webhook" -ContentType "application/json" -Body $body
```

## 4) Expose it with ngrok

1. Install & add your authtoken once:

   ```powershell
   ngrok config add-authtoken <YOUR_NGROK_AUTHTOKEN>
   ```
2. Start the tunnel:

   ```powershell
   ngrok http 8001
   ```
3. Copy the **HTTPS** forwarding URL (e.g., `https://abc123.ngrok-free.app`) and put it in `.env`:

   ```
   WEBHOOK_URL=https://abc123.ngrok-free.app
   ```

   Restart uvicorn so it reads the new `.env`.

**Note (scripts/automation calls):** ngrok shows a browser warning page on first visit. For programmatic calls, add this header:

* PowerShell: `-Headers @{ "ngrok-skip-browser-warning" = "true" }`
* curl: `-H 'ngrok-skip-browser-warning: true'`

Example through ngrok:

```powershell
Invoke-RestMethod -Method Get `
  -Headers @{ "ngrok-skip-browser-warning" = "true" } `
  -Uri "$env:WEBHOOK_URL/healthz"
```

## 5) Subscribe a Bigin “Watch” to your webhook

Run our bootstrap script (uses `.env` + tokens):

```powershell
python -m scripts.bootstrap_watch
```

It registers a watch for `Contacts.create` & `Contacts.edit` with:

* `notify_url` = `WEBHOOK_URL + /bigin-webhook`
* `token`      = `VERIFY_TOKEN` (Bigin echoes this back; our server checks it)
* `channel_id` = `CHANNEL_ID` (must be a big integer)

If your refresh token misses scopes you’ll get 401. Use a grant that includes at least:

```
ZohoBigin.notifications.ALL,ZohoBigin.modules.ALL,ZohoBigin.settings.ALL,ZohoBigin.users.READ,aaaserver.profile.READ,offline_access
```

## 6) Trigger a real event

Use our tiny helper to create a Contact (workflows disabled):

```powershell
python -m app.create_contact_once
```

You should see in the uvicorn log (and in ngrok’s inspector at [http://127.0.0.1:4040](http://127.0.0.1:4040)):

```
POST /bigin-webhook 200
```

(What the webhook does is your logic; earlier we tested by adding a Note. You can swap this to your flow functions.)

## 7) Common gotchas (and fixes)

* **401 “Bad verify token” at webhook**
  The JSON `token` from Bigin must match `VERIFY_TOKEN` in `.env`. Update `.env` & restart server; re-run the watch script if you changed tokens.
* **INVALID\_DATA “channel\_id”**
  Must be **numeric** (not a string). Use a 10–20 digit int.
* **401 on `/actions/watch`**
  Your refresh token lacks **notifications** scope. Mint a new grant with scopes above; update `tokens.json`.
* **ngrok “ERR\_NGROK\_6024” page**
  Add header `ngrok-skip-browser-warning: true` in scripts (see above).
* **URL changed after restarting ngrok**
  Update `WEBHOOK_URL` in `.env`, restart uvicorn, and re-run `scripts.bootstrap_watch` to resubscribe.
* **Token refresh flakiness**
  We cache `access_token` + `expires_at` in `tokens.json` and auto-refresh when needed. Ensure `.env` has `Z_CLIENT_ID` and `Z_CLIENT_SECRET` and that `tokens.json` contains a valid `refresh_token`.

## 8) Minimal test checklist to share

1. Start uvicorn: `python -m uvicorn app.server:app --port 8001`
2. Start ngrok: `ngrok http 8001` → copy HTTPS URL → set `WEBHOOK_URL=` in `.env` → restart uvicorn
3. Health check via ngrok (with skip-header)
4. `python -m scripts.bootstrap_watch`
5. `python -m app.create_contact_once` → watch webhook log hit

That’s the full setup we’re using right now—portable and easy to replay anywhere.
