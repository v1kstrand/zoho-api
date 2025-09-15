import re

def parse_mail(body):

    body = body.strip()

    EMAIL = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)
    PHONE = re.compile(r"\+?\d[\d\s\-()]{6,}")
    URL   = re.compile(r"https?://\S+")

    lines = [l.strip() for l in body.splitlines()]
    L = {i: s for i, s in enumerate(lines)}

    # find section indices
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
        # name + phone like: "First_name Last_name - +46723866428"
        if "-" in l and organizer_hint not in l.lower():
            left, right = [x.strip() for x in l.split("-", 1)]
            cust_name = left.replace("_", " ")
            m = PHONE.search(right)
            if m: cust_phone = m.group().replace(" ", "")
        # emails on any line
        for e in EMAIL.findall(l):
            if organizer_hint in l.lower() or "info@vdsai.se" in e.lower():
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

    # SMS “undefined” -> None
    first, last = cust_name.split(" ") if cust_name else ["", ""]

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
        "sms_opt_phone": None if (sms_raw or "").lower()=="undefined" else (PHONE.search(sms_raw or "") and PHONE.search(sms_raw or "").group().replace(" ","")),
    }

    return result

