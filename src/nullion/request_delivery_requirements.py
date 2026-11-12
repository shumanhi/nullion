"""Request-bound output requirements and grounded partial replies.

Only runtime-owned ToolResult metadata carries these decisions.
Provider payloads, filenames and reply prose cannot introduce this contract.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

from nullion.artifacts import normalize_artifact_extensions
from nullion.attachment_format_graph import VALID_ATTACHMENT_EXTENSIONS

_KEY = "response_requirements"


def _value(result, key):
    return result.get(key) if isinstance(result, Mapping) else getattr(result, key, None)


def _binding(results):
    rows = [(_value(r, 'invocation_id'), _value(r, 'tool_name'), _value(r, 'status')) for r in results]
    return hashlib.sha256(json.dumps(rows, ensure_ascii=False).encode()).hexdigest()


def _request_digest(user_message):
    return hashlib.sha256(str(user_message).encode()).hexdigest()


def _action_presentation_binding(results):
    from nullion.workflow_reasoning import outcome_binding
    return outcome_binding('', {}, None, results, ())


def required_action_receipt_delivery(results):
    for row in results or ():
        output = _value(row, 'output')
        receipt = output.get('action_receipt') if isinstance(output, Mapping) else None
        if (_value(row, 'status') == 'completed' and isinstance(receipt, Mapping)
                and receipt.get('type') == 'action_receipt' and receipt.get('delivery_required') is True):
            return True
    return False


def record_reviewed_action_reply(results, user_message, reply):
    """Called only after the owner's grounded completion review approves copy."""
    rows = list(results or ())
    for row in reversed(rows):
        output = _value(row, 'output')
        receipt = output.get('action_receipt') if isinstance(output, Mapping) else None
        if (_value(row, 'status') == 'completed' and isinstance(receipt, Mapping)
                and receipt.get('type') == 'action_receipt'
                and hasattr(row, 'response_requirements')):
            metadata = row.response_requirements if isinstance(row.response_requirements, Mapping) else {}
            row.response_requirements = {**metadata, '_reviewed_action_reply': {
                'version': 1, 'request_digest': _request_digest(user_message),
                'reply_digest': _request_digest(reply), 'evidence_binding': _action_presentation_binding(rows)}}
            return True
    return False


def reviewed_action_reply_matches(results, user_message, reply):
    rows = list(results or ())
    for row in reversed(rows):
        metadata = _value(row, 'response_requirements')
        record = metadata.get('_reviewed_action_reply') if isinstance(metadata, Mapping) else None
        if (isinstance(record, Mapping) and record.get('version') == 1
                and record.get('request_digest') == _request_digest(user_message)
                and record.get('reply_digest') == _request_digest(reply)
                and record.get('evidence_binding') == _action_presentation_binding(rows)):
            return True
    return False


def record_request_delivery_requirements(results, user_message, extensions, *, partial_reply=None, eligible_tool_names=None):
    """Persist the existing validated requirement planner's decision, not a new model call."""
    rows = list(results or ())
    if not isinstance(user_message, str) or not user_message.strip():
        return False
    extensions = tuple(extensions)
    normalized = normalize_artifact_extensions(extensions)
    if (any(not isinstance(value, str) or not value.startswith('.') for value in extensions)
            or not set(normalized).issubset(VALID_ATTACHMENT_EXTENSIONS)):
        raise ValueError('Invalid reviewed attachment formats')
    for result in reversed(rows):
        if hasattr(result, 'response_requirements'):
            prior = reviewed_request_delivery_requirements(rows, user_message)
            if partial_reply is None and prior and list(normalized) == prior['required_attachment_extensions']:
                partial_reply = prior.get('partial_reply')
            if eligible_tool_names is None and prior:
                eligible_tool_names = prior.get('eligible_evidence_tool_names')
            record = {'version': 1, 'request_digest': _request_digest(user_message),
                'evidence_binding': _binding(rows), 'required_attachment_extensions': list(normalized),
                **({'eligible_evidence_tool_names': list(dict.fromkeys(eligible_tool_names))} if eligible_tool_names is not None else {}),
                **({'partial_reply': partial_reply} if isinstance(partial_reply, str) and partial_reply.strip() else {})}
            native = result.response_requirements.get("_native_artifact_media") if isinstance(result.response_requirements, dict) else None
            if isinstance(native, dict):
                record["_native_artifact_media"] = dict(native)
            reviewed = result.response_requirements.get('_reviewed_action_reply') if isinstance(result.response_requirements, dict) else None
            if isinstance(reviewed, dict):
                record['_reviewed_action_reply'] = dict(reviewed)
            result.response_requirements = record
            return True
    return False


def reviewed_request_delivery_requirements(results, user_message):
    if not isinstance(user_message, str) or not user_message.strip():
        return None
    rows = list(results or ())
    for result in reversed(rows):
        record = _value(result, _KEY)
        if not isinstance(record, Mapping):
            continue
        extensions = record.get('required_attachment_extensions')
        tools = record.get('eligible_evidence_tool_names')
        if (record.get('version') == 1 and record.get('request_digest') == _request_digest(user_message)
                and record.get('evidence_binding') == _binding(rows)
                and isinstance(extensions, list) and len(extensions) <= 16
                and all(isinstance(value, str) and value in VALID_ATTACHMENT_EXTENSIONS for value in extensions)
                and (tools is None or isinstance(tools, list) and len(tools) <= 256
                     and all(isinstance(name, str) and 0 < len(name) <= 120 for name in tools))):
            return record
        return None
    return None
