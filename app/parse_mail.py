import re


def _normalize_name(raw):
    if not raw:
        return None
    cleaned = re.sub(r"[._]+", " ", str(raw))
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned.title() if cleaned else None

NAME_PHONE_SPLIT = re.compile(r"\s*[-\u2013\u2014]\s*")
BETWEEN_RE = re.compile(r"between\s+.+?\s+and\s+(.+)", re.I)

def parse_mail(body):
    body = body.strip()

    EMAIL = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
    PHONE = re.compile(r"\+?\d[\d\s\-()]{6,}")
    URL   = re.compile(r"https?://\S+")

    lines = [l.strip() for l in body.splitlines()]
    L = {i: s for i, s in enumerate(lines)}

    # find section indices
    idx_what = next((i for i,s in L.items() if s.lower().startswith("what:")), -1)
    idx_tz   = next((i for i,s in L.items() if s.lower().startswith("invitee time zone:")), -1)
    idx_who  = next((i for i,s in L.items() if s.lower().startswith("who:")), -1)
    idx_where= next((i for i,s in L.items() if s.lower().startswith("where:")), -1)
    idx_co   = next((i for i,s in L.items() if s.lower().startswith("company:")), -1)
    idx_sms  = next((i for i,s in L.items() if s.lower().startswith("phone number (text notifications):")), -1)

    # helpers to get the next non-empty line after a header
    def next_line(after_idx):
        j = after_idx + 1
        while 0 <= after_idx < len(lines) and j < len(lines):
            if lines[j]: return lines[j]
            j += 1
        return None

    what_line = next_line(idx_what)
    timezone = next_line(idx_tz)
    where    = next_line(idx_where)
    company  = next_line(idx_co)
    sms_raw  = next_line(idx_sms)

    # parse Who block (from its header until a blank line or next header)
    headers = {idx_tz, idx_who, idx_where, idx_co, idx_sms}
    end_who = next((i for i in range(idx_who+1, len(lines))
                    if not lines[i] or i in headers), len(lines))
    who_lines = [l for l in lines[idx_who+1:end_who] if l]

    # organizer/customer picking
    organizer_hint = "organizer"
    organizer_email = None
    cust_email = None
    cust_name = None
    cust_phone = None

    for l in who_lines:
        lower = l.lower()

        # try to split "Name - phone" variants (regular hyphen, en dash, em dash)
        if organizer_hint not in lower:
            parts = NAME_PHONE_SPLIT.split(l, 1)
            if len(parts) == 2:
                left, right = (parts[0].strip(), parts[1].strip())
                normalized = _normalize_name(left)
                if normalized:
                    cust_name = normalized
                if right and not cust_phone:
                    m = PHONE.search(right)
                    if m:
                        cust_phone = m.group().replace(" ", "")

        # fallback: look for phone anywhere in the line
        if not cust_phone:
            m = PHONE.search(l)
            if m:
                cust_phone = m.group().replace(" ", "")

        # emails on any line
        for e in EMAIL.findall(l):
            if organizer_hint in lower or "info@vdsai.se" in e.lower():
                organizer_email = organizer_email or e.lower()
            else:
                cust_email = cust_email or e.lower()

    # fallbacks
    if not cust_phone:
        m = PHONE.search(" ".join(who_lines))
        cust_phone = (m.group().replace(" ", "") if m else None)

    # URL
    m = URL.search(where or "")
    where_url = m.group(0) if m else None

    if not cust_name and what_line:
        match = BETWEEN_RE.search(what_line)
        if match:
            fallback_name = _normalize_name(match.group(1).strip().strip('.'))
            if fallback_name:
                cust_name = fallback_name

    if not cust_name and cust_email:
        local = cust_email.split("@", 1)[0]
        guess = _normalize_name(local)
        if guess:
            cust_name = guess

    # SMS "undefined" -> None
    first, last = "", ""
    if cust_name:
        parts = cust_name.split()
        first = parts[0]
        last = " ".join(parts[1:]) if len(parts) > 1 else ""

    sms_opt_phone = None
    sms_value = (sms_raw or "").strip()
    if sms_value and sms_value.lower() != "undefined":
        sms_match = PHONE.search(sms_value)
        if sms_match:
            sms_opt_phone = sms_match.group().replace(" ", "")

    result = {
        "timezone": timezone,
        "customer_name": cust_name,
        "customer_first_name": first,
        "customer_last_name": last,
        "customer_email": cust_email,
        "customer_phone": cust_phone,
        "organizer_email": organizer_email,
        "location_url": where_url,
        "company": company,
        "sms_opt_phone": sms_opt_phone,
    }

    return result

