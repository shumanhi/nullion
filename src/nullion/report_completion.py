"""Request-bound report completion receipts, separate from file delivery.

The producer supplies structured metadata in inert HTML, so streamed document
continuations remain supported without an additional model or source call.
"""
from __future__ import annotations

import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path

MARKER = "NULLION_REPORT_OUTCOME_V1:"


def report_delivery_gate_contract(context: dict, evidence_refs: list[str]) -> dict:
    """Bind a conditional no-delivery decision to this run and its evidence.

    This is an alternative response from the existing composer, not another
    classifier. Missing data or a failed check must still produce useful partial
    work; only an explicit instruction to withhold can select this outcome.
    """
    digest = hashlib.sha256(json.dumps(context, sort_keys=True, ensure_ascii=False,
        default=str).encode()).hexdigest()
    return {"contract_id": digest, "response_schema": {
        "type": "object", "additionalProperties": False,
        "required": ["contract_id", "delivery_decision", "instruction_condition", "evidence_refs"],
        "properties": {
            "contract_id": {"const": digest},
            "delivery_decision": {"const": "withhold"},
            "instruction_condition": {"type": "string", "minLength": 1, "maxLength": 1000},
            "evidence_refs": {"type": "array", "minItems": 1, "uniqueItems": True,
                "items": {"enum": evidence_refs}},
        }}}


def validated_report_delivery_gate(decision: object, contract: dict, *, instructions: str) -> dict | None:
    from jsonschema import validate, ValidationError
    try:
        validate(decision, contract["response_schema"])
    except (ValidationError, KeyError, TypeError):
        return None
    condition = decision['instruction_condition'].strip()
    if not condition or condition not in instructions:
        return None
    return dict(decision)


def report_outcome_contract(context: dict, *, recovery_tool_names=()) -> dict:
    contract_id = hashlib.sha256(json.dumps(context, sort_keys=True, ensure_ascii=False,
        default=str).encode()).hexdigest()
    return {"marker": MARKER, "contract_id": contract_id, "response_schema": {
        "type": "object", "additionalProperties": False,
        "required": ["contract_id", "status", "all_material_requirements_enumerated", "remaining_work"],
        "properties": {
            "contract_id": {"const": contract_id},
            "status": {"enum": ["complete", "partial"]},
            "all_material_requirements_enumerated": {"type": "boolean"},
            "retry_tool_names": {"type": "array", "uniqueItems": True, "maxItems": 32,
                "description": "For partial research, select exact available tools that can finish remaining work. This is not permission to execute them.",
                "items": {"enum": sorted(set(recovery_tool_names))} if recovery_tool_names else False},
            "remaining_work": {"type": "array", "maxItems": 12,
                "items": {"type": "string", "minLength": 1, "maxLength": 500}}},
        "allOf": [{"if": {"properties": {"status": {"const": "complete"}}},
            "then": {"properties": {"all_material_requirements_enumerated": {"const": True},
                "retry_tool_names": {"maxItems": 0},
                "remaining_work": {"maxItems": 0}}},
            "else": {"properties": {"remaining_work": {"minItems": 1}}}}]}}


def generated_report_outcome(document: str, contract: dict) -> dict:
    from jsonschema import validate, ValidationError
    class Metadata(HTMLParser):
        def __init__(self):
            super().__init__(); self.records = []
        def handle_comment(self, data):
            if data.strip().startswith(MARKER):
                self.records.append(data.strip()[len(MARKER):].strip())
    parser = Metadata()
    try:
        parser.feed(document)
        if len(parser.records) != 1 or len(parser.records[0]) > 12000:
            raise ValueError("Missing or ambiguous report metadata")
        value = json.loads(parser.records[0])
        validate(value, contract["response_schema"])
        return {"status": value["status"], "remaining_work": value["remaining_work"],
            **({"retry_tool_names": value["retry_tool_names"]} if "retry_tool_names" in value else {})}
    except (ValueError, KeyError, TypeError, ValidationError):
        # Invalid metadata never establishes fulfillment or discards a file.
        pass
    return {"status": "unknown", "remaining_work": [
        "Completion of the report's required research was not established."]}


def bind_report_outcome(request: str, outcome: dict, paths: list[str]) -> dict:
    return {**outcome, "request_digest": hashlib.sha256(request.encode()).hexdigest(),
        "files": {str(Path(p).resolve()): hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in paths}}


def partial_report_receipt(request: str, receipt: object, paths: list[str]) -> bool:
    """Allow only the selected unchanged files from this request's producer."""
    if not isinstance(receipt, dict) or receipt.get("status") not in {"partial", "unknown"}:
        return False
    return report_receipt_matches(request, receipt, paths)


def report_receipt_matches(request: str, receipt: object, paths: list[str]) -> bool:
    if not isinstance(receipt, dict):
        return False
    if receipt.get("request_digest") != hashlib.sha256(request.encode()).hexdigest():
        return False
    try:
        actual = {str(Path(p).resolve()): hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in paths}
        return bool(actual) and actual == receipt.get("files")
    except (OSError, TypeError, ValueError):
        return False
