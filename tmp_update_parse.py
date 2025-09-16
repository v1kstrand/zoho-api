from pathlib import Path
from textwrap import dedent

path = Path("app/parse_mail.py")
text = path.read_text(encoding="utf-8")

if "def _normalize_name(" not in text:
    helper = dedent('''
\n\ndef _normalize_name(raw):
    if not raw:
        return None
    cleaned = re.sub(r"[._]+", " ", str(raw))
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned.title() if cleaned else None
''')
    text = text.replace("import re\n\n", "import re\n\n" + helper)

text = text.replace(
    '            left, right = [x.strip() for x in l.split("-", 1)]\n            cust_name = left.replace("_", " ")\n',
    '            left, right = [x.strip() for x in l.split("-", 1)]\n            normalized = _normalize_name(left)\n            if normalized:\n                cust_name = normalized\n')

text = text.replace(
    '    if not cust_name and what_line:\n        match = BETWEEN_RE.search(what_line)\n        if match:\n            cust_name = match.group(1).strip().strip(\'.\')\n\n    if not cust_name and cust_email:\n        local = cust_email.split("@", 1)[0]\n        guess = re.sub(r"[._]+", " ", local)\n        guess = re.sub(r"\\s+", " ", guess).strip()\n        if guess:\n            cust_name = guess.title()\n\n',
    '    if not cust_name and what_line:\n        match = BETWEEN_RE.search(what_line)\n        if match:\n            fallback_name = _normalize_name(match.group(1).strip().strip(\'.\'))\n            if fallback_name:
                cust_name = fallback_name\n\n    if not cust_name and cust_email:\n        local = cust_email.split("@", 1)[0]\n        guess = _normalize_name(local)\n        if guess:\n            cust_name = guess\n\n')

path.write_text(text, encoding="utf-8")
