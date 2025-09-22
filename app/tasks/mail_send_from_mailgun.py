# app/tasks/mail_send_from_mailgun.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Iterable
from datetime import datetime, timedelta

from dotenv import load_dotenv
import os

from ..api_client import find_contact_by_email, update_contact
from ..mailgun_util import send_mailgun_message

DFU1_DELTA = int(os.environ["DFU1_DELTA"])
DFU2_DELTA = int(os.environ["DFU2_DELTA"])

load_dotenv()

__all__ = [
    "StageConfig",
    "STAGE_CONFIGS",
    "build_argument_parser",
    "main",
    "resolve_stage",
    "_verify_contact_rules",
    "send_campaign_pipeline",
    "get_now_with_delta",
]

def get_now_with_delta(days: int = 0) -> str:
    return (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d_%H:%M:%S")
    

@dataclass(frozen=True)
class StageConfig:
    template: str
    column_map: dict[str, str]
    static_params: dict[str, str]
    tag: str | None = None
    contact_rules: list[tuple[str, str, str]] | None = None
    contact_update: dict[str, str] | None = None


STAGE_CONFIGS: dict[str, StageConfig] = {
    "intro": StageConfig(
        template="intro_v1",
        column_map={"first_name": "first_name", "auto_number": "auto_number"},
        static_params={},
        tag=f"intro_{get_now_with_delta()}",
        contact_rules=[("stage", "not_in", ["intro", "booked", "dropped"])],
        contact_update={"stage": "intro", "intro_date": get_now_with_delta(), "dfu1_date": get_now_with_delta(DFU1_DELTA)},
    ),
    "dfu1": StageConfig(
        template="dfu1_v1",
        column_map={"first_name": "first_name", "auto_number": "auto_number"},
        static_params={},
        tag=f"dfu1_{get_now_with_delta()}",
        contact_rules=[("stage", "is", "intro")],
        contact_update={"stage": "dfu1", "dfu2_date": get_now_with_delta(DFU2_DELTA)},
    ),
    "dfu2": StageConfig(
        template="dfu2_v1",
        column_map={"first_name": "first_name", "auto_number": "auto_number"},
        static_params={},
        tag=f"dfu2_{get_now_with_delta()}",
        contact_rules=[("stage", "is", "dfu1")],
        contact_update={"stage": "dfu2"},
    ),
}

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


def resolve_stage(stage: str) -> StageConfig:
    try:
        return STAGE_CONFIGS[stage]
    except KeyError as exc:
        available = ", ".join(sorted(STAGE_CONFIGS)) or "<none>"
        raise ValueError(f"Unknown stage '{stage}'. Available stages: {available}") from exc

def _verify_contact_rules(contact: dict[str, str], contact_rules: dict[str, str]):
    for key, comp, value in contact_rules:
        if comp == "is" and contact[key] == value:
            continue
        if comp == "is_not" and contact[key] != value:
            continue
        if comp == "in" and contact[key] in value:
            continue
        if comp == "not_in" and contact[key] not in value:
            continue
        print(f"[skip] {key} {comp} {value}, got: {contact[key]}")
        return False
    return True


def send_campaign_pipeline(
    *,
    stage: str,
    emails: list[str],
    config: StageConfig,
    dry_run: bool = False,
    contact_lookup=find_contact_by_email,
    send_message=send_mailgun_message,
) -> None:
    mailgun_errors: list[str] = []
    delivered_to = 0
    message_kwargs = {"tag": config.tag} if config.tag else {}
    if config.tag:
        print(f"[info] using Mailgun tag '{config.tag}'")

    for email in emails:
        contact = contact_lookup(email)
        if not contact:
            print(f"[skip] Contact not found for {email}")
            continue
        
        if not _verify_contact_rules(contact, config.contact_rules):
            continue
        
        if not dry_run:
            update_contact(contact["email"], config.contact_update)
        else:
            print(f"[dry-run] stage '{stage}' would update {email} with {config.contact_update}")
        
        try:
            params = _build_template_params(
                contact,
                column_map=config.column_map,
                static_params=config.static_params,
                email=email,
            )
        except ValueError as exc:
            print(f"[skip] {exc}")
            mailgun_errors.append(str(exc))
            continue

        if dry_run:
            print(
                f"[dry-run] stage '{stage}' would send template '{config.template}' "
                f"to {email} with params {params}"
            )
            delivered_to += 1
            continue

        try:
            send_message([email], (config.template, params), **message_kwargs)
            delivered_to += 1
            print(f"[mailgun] stage '{stage}' sent template '{config.template}' to {email}")
        except Exception as exc:  # pragma: no cover - best effort logging only
            print(f"[error] failed to send to {email}: {exc}")
            mailgun_errors.append(f"{email}: {exc}")

    if not dry_run and delivered_to == 0:
        print("[info] no messages sent")

    if mailgun_errors:
        print("[warn] one or more deliveries encountered issues:")
        for msg in mailgun_errors:
            print(f"    - {msg}")


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send a staged Mailgun template to contacts."
    )
    parser.add_argument(
        "stage",
        choices=sorted(STAGE_CONFIGS.keys()),
        help="Stage name that determines template and merge parameters.",
    )
    parser.add_argument(
        "emails",
        nargs="+",
        help="Email addresses that must exist in the contacts CSV.",
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

    config = resolve_stage(args.stage)
    send_campaign_pipeline(
        stage=args.stage,
        emails=args.emails,
        config=config,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    main()
