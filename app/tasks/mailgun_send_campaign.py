# app/tasks/mail_send_from_mailgun.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import os

from dotenv import load_dotenv

from ..api_client import find_contact_by_email, update_contact, get_contact_field
from ..mailgun_util import send_mailgun_message

DFU1_DELTA = int(os.environ["DFU1_DELTA"])
DFU2_DELTA = int(os.environ["DFU2_DELTA"])

load_dotenv()

__all__ = [
    "StageConfig",
    "STAGE_CONFIGS",
    "resolve_stage",
    "_verify_contact_rules",
    "send_campaign_pipeline",
    "get_now_with_delta",
]

def get_now_with_delta(days: int = 0) -> str:
    return (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d_%H:%M:%S")
    

@dataclass()
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
        contact_rules=[("stage", "is", "new")],
        contact_update={"stage": "intro", "intro_date": get_now_with_delta()},
    ),
    "dfu1": StageConfig(
        template="dfu1",
        column_map={"first_name": "first_name", "auto_number": "auto_number"},
        static_params={},
        tag=f"dfu1_{get_now_with_delta()}",
        contact_rules=[("stage", "is", "intro")],
        contact_update={"stage": "dfu1", "dfu1_date": get_now_with_delta()},
    ),
    "dfu2": StageConfig(
        template="dfu2",
        column_map={"first_name": "first_name", "auto_number": "auto_number"},
        static_params={},
        tag=f"dfu2_{get_now_with_delta()}",
        contact_rules=[("stage", "is", "dfu1")],
        contact_update={"stage": "dfu2", "dfu2_date": get_now_with_delta()},
    ),
}

def _build_template_params(
    contact: dict[str, str],
    column_map: dict[str, str],
    static_params: dict[str, str],
) -> dict[str, str]:
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
        print(f"[skip] Contact rules not met for {contact['email']}, Rule: ({key} {comp} {value}), got: {contact[key]}")
        return False
    return True


def send_campaign_pipeline(
    stage: str,
    emails: list[str],
    dry_run: bool,
    personal_mail: bool,
    contact_lookup=find_contact_by_email,
    send_message=send_mailgun_message,
    custom_tag=None,
    verbose=True
) -> None:
    config = resolve_stage(stage)
    if custom_tag is not None:
        config.tag = custom_tag
    if not personal_mail:
        config.template += "_generic"
    
    if config.tag:
        print(f"[info] using Mailgun tag '{config.tag}'")

    valid_contacts = {}
    for email in emails:
        contact = contact_lookup(email)
        if not contact:
            print(f"[skip] Contact not found for {email}")
            continue
        
        if get_contact_field(email, "unsub").lower() != "false":
            print(f"[skip] Contact unsubscribed for {email}")
            continue
        
        if personal_mail and get_contact_field(email, "contact_type").lower() != "personal":
            print(f"[skip] Contact opted out for generic mail for {email}")
            continue
        
        if not _verify_contact_rules(contact, config.contact_rules):
            continue
        
        params = _build_template_params(
            contact,
            column_map=config.column_map,
            static_params=config.static_params,
        )
        valid_contacts[email] = params
        if verbose:
            print(f"[ok] Contact found for {email} with params: {params}")
    
    if not dry_run:
        receivers = send_message(valid_contacts, config.template, tag=config.tag)
        for email in receivers:
            update_contact(email, config.contact_update)
            
        print(f"[info] {len(receivers)} messages sent")
    else:
        print(f"[info] {len(valid_contacts)} messages would be sent")

