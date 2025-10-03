import sys
import os
import email
import shutil
import subprocess
import csv
import json
import glob
from datetime import datetime

import urllib.parse
from collections import defaultdict
import pandas as pd

from app import api_client

def _optional_path(base, name):
    if not base or not name:
        return None
    return os.path.join(base, name)

MAIL_DATA_DIR = os.environ["MAIL_UTIL_DATADIR"]
MAIL_UTIL_BATCH = os.environ["MAIL_UTIL_BATCH"]
MAIL_UTIL_EMAIL = os.environ["MAIL_UTIL_EMAIL"]
BATCH_STATS_PATH = _optional_path(MAIL_DATA_DIR, MAIL_UTIL_BATCH)
EMAIL_STATS_PATH = _optional_path(MAIL_DATA_DIR, MAIL_UTIL_EMAIL)

ORACLE_HOST = os.environ["ORACLE_HOST"]
ORACLE_USER = os.environ["ORACLE_USER"]
FORM_DATA_DIR = os.environ["FORM_DATA_DIR"]

def extract_html(eml_path):
    with open(eml_path, 'rb') as f:
        msg = email.message_from_binary_file(f)

    html = None
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype == 'text/html':
                html = part.get_payload(decode=True)  # decoded bytes
                charset = part.get_content_charset() or 'utf-8'
                html = html.decode(charset, errors='replace')
                break
    else:
        if msg.get_content_type() == 'text/html':
            html = msg.get_payload(decode=True).decode(msg.get_content_charset() or 'utf-8', errors='replace')

    if html:
        sys.stdout.write(html)
        
        
def check_and_update_smtp_errors():
    email_df = pd.read_csv(EMAIL_STATS_PATH)
    updates = []
    for _, row in email_df[email_df["smtp_message"] != "OK"].iterrows():
        if api_client.get_contact_field(row["recipient"], "stage") == "invalid":
            continue
        
        smtp_message = row["smtp_message"]
        recipient = row["recipient"]
        updates.append((recipient, smtp_message))
        
        api_client.update_contact(recipient, {"stage": "invalid", "unsub" : "True"})
        api_client.append_contact_note(recipient, f"SMTP error: {smtp_message[:20]}")
        
    if not updates:
        print("no updates")
    for recipient, smtp_message in updates:
        print(f"Update: {recipient} {smtp_message}")
        

def get_site_trafic(is_after_date = "2025-10-02T09:19:31+00:00"):
    df = api_client.get_df()
    auto_number = set(map(str, df["auto_number"].tolist()))
    LOG  = "/var/log/nginx/vdsai-events.log"

    def run(cmd):
        return subprocess.check_output(cmd, text=True, errors="ignore")

    def is_after(date: str, is_after: str = is_after_date) -> bool:
        return datetime.fromisoformat(date) > datetime.fromisoformat(is_after)

    print("__")
    ls_out = run(["ssh", f"{ORACLE_USER}@{ORACLE_HOST}", f"ls -1t {LOG}* 2>/dev/null || true"])
    print("start")
    files = list(reversed([p for p in ls_out.splitlines() if p]))  # oldest → newest

    rows = [["ts_utc","token","event","path","user_agent"]]
    visits = defaultdict(lambda: defaultdict(int))
    for path in files:
        cat = "zcat" if path.endswith(".gz") else "cat"
        data = run(["ssh", f"{ORACLE_USER}@{ORACLE_HOST}", f"{cat} {path}"])
        for line in data.splitlines():
            line = line.strip()
            if not line: 
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            if d.get("tok","") not in auto_number or not d.get("ts","") or not is_after(d.get("ts","")):
                continue
            if d.get("ev","") == "ping":
                continue
            
            visits[d.get("tok","")][d.get("ev","")] += 1
            
            rows.append([
                d.get("ts",""),
                d.get("tok",""),
                d.get("ev",""),
                urllib.parse.unquote(d.get("u","")),
                d.get("ua",""),
            ])
            
    visits = dict(visits)
    for tok in list(visits.keys()):
        visits[tok] = dict(visits[tok])
        
    df_traffic = pd.DataFrame(rows[1:], columns=rows[0])
    df_visits = pd.DataFrame.from_dict(visits, orient="index").fillna(0)
    return df_traffic, df_visits


def pull_disc_form_submissions(
    remote_dir: str = "/var/lib/formrelay/submissions",
    local_dir: str = FORM_DATA_DIR,
) -> None:
    os.makedirs(local_dir, exist_ok=True)
    cmd = ["scp", "-r", f"{ORACLE_USER}@{ORACLE_HOST}:{remote_dir}/", f"{os.path.abspath(local_dir)}/"]

    if shutil.which("rsync"):
        cmd = ["rsync", "-avz", f"{ORACLE_USER}@{ORACLE_HOST}:{remote_dir}/", f"{os.path.abspath(local_dir)}/"]
    else:
        cmd = ["scp", "-r", f"{ORACLE_USER}@{ORACLE_HOST}:{remote_dir}/", f"{os.path.abspath(local_dir)}/"]

    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Sync failed: {e}") from e


def set_disc_form_csv(
    save_path: str = os.path.join(FORM_DATA_DIR, "discovery_form.csv")
):
    fields = ["submitted_at","email","goal","role","availability","data_sources","outcome","company_website"]
    rows = []
    json_sub_dir = os.path.join(FORM_DATA_DIR, "submissions", "*.json")
    for path in sorted(glob.glob(json_sub_dir)):
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        rows.append([d.get(k,"") for k in fields])

    def parse(ts):
        try: 
            return datetime.fromisoformat(ts.replace('Z','+00:00'))
        except: 
            return datetime.min
    rows.sort(key=lambda r: parse(r[0][0]) if r and r[0][0] else datetime.min)

    os.makedirs("data", exist_ok=True)
    with open(save_path,"w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, quoting=csv.QUOTE_ALL)
        w.writerow(fields)
        w.writerows(rows)
