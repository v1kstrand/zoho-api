a# minimal bits added on top of your existing IMAP code
BOOKING_FOLDER = os.getenv("BOOKING_FOLDER", "Bookings/Incoming")
MOVE_BOOKING_TO = os.getenv("BOOKING_MOVE_TO", "Processed/Bookings")
BOOKING_SUBJECT_HINT = r"\b(booking|appointment).*(confirmed|scheduled|new)\b"
BOOKING_ID_RE = re.compile(r"\bBooking\s*ID\s*[:#]\s*([A-Za-z0-9\-_]+)", re.I)

def _is_booking(msg, body: str) -> bool:
    subj = str(make_header(decode_header(msg.get("Subject","")))).lower()
    if re.search(BOOKING_SUBJECT_HINT, subj):
        return True
    return bool(BOOKING_ID_RE.search(body))

def _extract_booking_id(msg, body: str) -> Optional[str]:
    m = BOOKING_ID_RE.search(body)
    return m.group(1).strip() if m else None

# simple Bookings "getappointment" call (1 call per booking)
def get_appointment(booking_id: str) -> Dict[str, Any]:
    at, api = get_access_token()
    url = api.rstrip("/") + "/bookings/v1/json/getappointment"
    r = requests.post(url, headers=_auth_headers(at), data={"booking_id": booking_id}, timeout=REQ_TIMEOUT)
    r.raise_for_status()
    return r.json().get("response", {}).get("returnvalue", {}).get("response", {}) or {}

def process_bookings_once(verbose=True):
    assert IMAP_USER and IMAP_PASS
    imap = _imap_connect_with_retry(IMAP_HOST, IMAP_USER, IMAP_PASS, BOOKING_FOLDER, ensure_mailbox=MOVE_BOOKING_TO, verbose=verbose)
    try:
        typ, data = imap.uid("SEARCH", None, "(UNSEEN)")
        uids = (data[0].decode().split() if typ == "OK" and data and data[0] else [])
        for uid in uids:
            typ, fetch = imap.uid("FETCH", uid, "(RFC822)")
            if typ != "OK" or not fetch or not fetch[0]:
                continue
            msg = email.message_from_bytes(fetch[0][1])
            body = _get_text(msg)
            if not _is_booking(msg, body):
                continue

            bid = _extract_booking_id(msg, body)
            appt = get_appointment(bid) if bid else None

            # fallback: tight window if no id in mail
            if not appt:
                now = dt.datetime.now(dt.timezone.utc)
                for row in fetch_appointments(start=now-dt.timedelta(minutes=60), end=now+dt.timedelta(minutes=60), status=None, per_page=25):
                    # crude match by sender email + near start time
                    # (you can refine as needed)
                    appt = row; break

            if appt:
                c = _ensure_contact(appt)
                if c:
                    _upsert_pipeline_record(c, appt)  # your existing upsert (by Booking_Id)
            _move_message(imap, uid, MOVE_BOOKING_TO)
    finally:
        try: imap.close()
        except Exception: pass
        try: imap.logout()
        except Exception: pass
