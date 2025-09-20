from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from typing import Iterable

from ..api_client_csv import find_contact_by_email
from ..mail_utils import MailgunEventsClient, MailgunPerRecipient, send_mailgun_message


__all__ = [
    "build_argument_parser",
    "main",
    "send_campaign",
]


def _parse_key_value_pairs(entries: Iterable[str], *, option: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for raw in entries:
        if "=" not in raw:
            raise ValueError(f"{option} expects KEY=VALUE entries; got '{raw}'")
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            raise ValueError(f"{option} entries must provide both key and value; got '{raw}'")
        if key in mapping:
            raise ValueError(f"Duplicate {option} key '{key}'")
        mapping[key] = value
    return mapping


def _build_template_params(
    contact: dict[str, str] | None,
    *,
    column_map: dict[str, str],
    static_params: dict[str, str],
    email: str,
) -> dict[str, str]:
    if contact is None:
        raise ValueError(f"Contact not found for {email}")

    params: dict[str, str] = dict(static_params)
    for template_key, column_name in column_map.items():
        if column_name not in contact:
            raise ValueError(
                f"Contact column '{column_name}' required for template variable '{template_key}'"
            )
        params[template_key] = contact.get(column_name, "")
    return params


def send_campaign(
    *,
    template: str,
    emails: list[str],
    column_map: dict[str, str],
    static_params: dict[str, str],
    tag_label: str | None,
    dry_run: bool = False,
    contact_lookup=find_contact_by_email,
    events_client_factory=MailgunEventsClient,
    per_recipient_factory=MailgunPerRecipient,
    send_message=send_mailgun_message,
) -> None:
    mailgun_errors: list[str] = []
    lower_recipients = {email.lower() for email in emails}
    delivered_to = 0

    for email in emails:
        contact = contact_lookup(email)
        try:
            params = _build_template_params(
                contact,
                column_map=column_map,
                static_params=static_params,
                email=email,
            )
        except ValueError as exc:
            print(f"[skip] {exc}")
            mailgun_errors.append(str(exc))
            continue

        if dry_run:
            print(f"[dry-run] would send template '{template}' to {email} with params {params}")
            delivered_to += 1
            continue

        try:
            send_message([email], (template, params))
            delivered_to += 1
            print(f"[mailgun] sent template '{template}' to {email}")
        except Exception as exc:  # pragma: no cover - best effort logging only
            print(f"[error] failed to send to {email}: {exc}")
            mailgun_errors.append(f"{email}: {exc}")

    if dry_run or not delivered_to:
        if mailgun_errors:
            print("[warn] one or more deliveries encountered issues:")
            for msg in mailgun_errors:
                print(f"    - {msg}")
        return

    events_client = events_client_factory()
    per_recipient = per_recipient_factory(events_client)
    today = datetime.now(timezone.utc)
    rows = per_recipient.compute_rows_for_day(today, tag_label=tag_label)
    filtered = [row for row in rows if row["recipient"].lower() in lower_recipients]

    if filtered:
        per_recipient.upsert_csv(filtered)
        print(f"[mailgun] updated delivery log for {len(filtered)} recipient(s)")
    else:
        print("[mailgun] no delivery events found for specified recipients yet")

    if mailgun_errors:
        print("[warn] one or more deliveries encountered issues:")
        for msg in mailgun_errors:
            print(f"    - {msg}")


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send a Mailgun template to contacts and refresh delivery logs."
    )
    parser.add_argument(
        "template",
        help="Mailgun template name to send.",
    )
    parser.add_argument(
        "emails",
        nargs="+",
        help="Email addresses that must exist in the contacts CSV.",
    )
    parser.add_argument(
        "--param-from-column",
        dest="column_params",
        action="append",
        default=[],
        help=(
            "Template variable sourced from a contact column. Specify multiple times, e.g. "
            "--param-from-column first_name=first_name"
        ),
    )
    parser.add_argument(
        "--param",
        dest="static_params",
        action="append",
        default=[],
        help="Static template variable applied to every recipient (KEY=VALUE).",
    )
    parser.add_argument(
        "--tag-label",
        dest="tag_label",
        default=None,
        help="Optional tag label to associate with delivery log rows (defaults to template name).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not call Mailgun; only print what would happen.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        column_map = _parse_key_value_pairs(args.column_params, option="--param-from-column")
        static_params = _parse_key_value_pairs(args.static_params, option="--param")
    except ValueError as exc:
        parser.error(str(exc))

    if not column_map and not static_params:
        parser.error("Provide at least one --param or --param-from-column entry")

    tag_label = args.tag_label or args.template
    send_campaign(
        template=args.template,
        emails=args.emails,
        column_map=column_map,
        static_params=static_params,
        tag_label=tag_label,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
