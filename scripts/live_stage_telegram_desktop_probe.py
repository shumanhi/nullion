#!/usr/bin/env python3
"""Run live stage chat contracts through the logged-in Telegram Desktop app."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import time
from typing import Any

try:
    from .live_platform_delivery_probe import _read_env_file
    from .live_stage_chat_contract_probe import (
        DEFAULT_LIVE_STAGE_SCENARIOS,
        DEEP_LIVE_STAGE_SCENARIOS,
        SCENARIOS,
        _artifact_extensions,
        _assert_contract,
        _download_and_check_artifacts,
        _get_json,
        _materialized_prompt,
        _normalize_event_payload,
        _scenario_for_config,
    )
except ImportError:
    from live_platform_delivery_probe import _read_env_file
    from live_stage_chat_contract_probe import (
        DEFAULT_LIVE_STAGE_SCENARIOS,
        DEEP_LIVE_STAGE_SCENARIOS,
        SCENARIOS,
        _artifact_extensions,
        _assert_contract,
        _download_and_check_artifacts,
        _get_json,
        _materialized_prompt,
        _normalize_event_payload,
        _scenario_for_config,
    )


_MEDIA_PATH_RE = re.compile(r"MEDIA:\s*([^\n\r]+)")


def _send_desktop_message(text: str, *, bot_username: str) -> None:
    subprocess.run(
        ["open", "-a", "Telegram", f"tg://resolve?domain={bot_username}"],
        check=True,
        capture_output=True,
        text=True,
    )
    # Telegram briefly clears its chat view while resolving a deep link. Wait
    # for that transition before placing text in the composer.
    time.sleep(3.0)
    script = [
        "on run argv",
        'tell application "Telegram" to activate',
        "delay 0.5",
        'tell application "System Events" to tell process "Telegram"',
        "set frontmost to true",
        "set the clipboard to item 1 of argv",
        'keystroke "v" using command down',
        # Long prompts can still be populating Telegram's rich-text composer
        # after the paste shortcut returns. Wait before Enter so the live UI
        # probe does not silently leave the request unsent in the composer.
        "delay 0.8",
        "set frontmost to true",
        "key code 36",
    ]
    if text.strip().startswith("/"):
        script.extend([
            "delay 0.35",
            "set frontmost to true",
            "key code 36",
        ])
    script.extend(["end tell", "end run"])
    command = ["osascript"]
    for line in script:
        command.extend(["-e", line])
    command.append(text)
    subprocess.run(command, check=True, capture_output=True, text=True)


def _created_at(payload: dict[str, Any]) -> str:
    return str(payload.get("created_at") or "").strip()


def _recent_event_payloads(
    runtime_db: Path,
    *,
    table: str,
    conversation_id: str,
    since: str,
    event_type: str | None = None,
    row_limit: int = 1000,
) -> list[dict[str, Any]]:
    if table not in {"conversation_events", "runtime_events"}:
        raise ValueError(f"unsupported runtime event table: {table}")
    conversation_path = "$.conversation_id" if table == "conversation_events" else "$.payload.conversation_id"
    scan_limit = max(2000, min(row_limit * 100, 10000))
    query = (
        "SELECT payload FROM ("
        f"SELECT rowid, payload FROM {table} ORDER BY rowid DESC LIMIT ?"
        ") "
        f"WHERE json_extract(payload, '{conversation_path}') = ? "
        "AND json_extract(payload, '$.created_at') >= ?"
    )
    params: list[Any] = [scan_limit, conversation_id, since]
    if event_type:
        query += " AND json_extract(payload, '$.event_type') = ?"
        params.append(event_type)
    query += " ORDER BY rowid ASC LIMIT ?"
    params.append(row_limit)
    with sqlite3.connect(str(runtime_db)) as connection:
        rows = connection.execute(query, params).fetchall()
    payloads: list[dict[str, Any]] = []
    for (raw_payload,) in rows:
        try:
            payload = json.loads(raw_payload)
        except (TypeError, json.JSONDecodeError):
            continue
        normalized = _normalize_event_payload(payload)
        if normalized:
            payloads.append(normalized)
    return payloads


def _events_since(runtime_db: Path, conversation_id: str, since: str) -> list[dict[str, Any]]:
    return [
        *_recent_event_payloads(
            runtime_db,
            table="conversation_events",
            conversation_id=conversation_id,
            since=since,
        ),
        *_recent_event_payloads(
            runtime_db,
            table="runtime_events",
            conversation_id=conversation_id,
            since=since,
        ),
    ]


def _matching_event(
    runtime_db: Path,
    conversation_id: str,
    *,
    event_type: str,
    user_message: str,
    since: str,
) -> dict[str, Any] | None:
    recent = _recent_event_payloads(
        runtime_db,
        table="conversation_events",
        conversation_id=conversation_id,
        since=since,
        event_type=event_type,
        row_limit=20,
    )
    exact_matches = [event for event in recent if event.get("user_message") == user_message]
    if exact_matches:
        return exact_matches[-1]
    # Telegram Desktop can turn pasted Markdown delimiters into rich-text
    # entities before sending. There is only one in-flight probe per chat, so
    # the latest typed event after the send timestamp is the same turn.
    return recent[-1] if recent else None


def _wait_for_turn(
    runtime_db: Path,
    conversation_id: str,
    *,
    user_message: str,
    since: str,
    timeout_seconds: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    deadline = time.perf_counter() + timeout_seconds
    while time.perf_counter() < deadline:
        turn = _matching_event(
            runtime_db,
            conversation_id,
            event_type="conversation.chat_turn",
            user_message=user_message,
            since=since,
        )
        if turn is not None:
            turn_id = str(turn.get("turn_id") or "").strip()
            received_matches = [
                event
                for event in _recent_event_payloads(
                    runtime_db,
                    table="conversation_events",
                    conversation_id=conversation_id,
                    since=since,
                    event_type="conversation.message_received",
                    row_limit=20,
                )
                if event.get("turn_id") == turn_id
            ]
            if received_matches:
                return received_matches[-1], turn
        time.sleep(0.4)
    raise AssertionError(
        f"no completed Telegram chat turn for {user_message[:120]!r} within {timeout_seconds:.0f}s"
    )


def _receipt_for_request(path: Path, request_id: str) -> dict[str, Any] | None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return None
    for line in reversed(lines):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and record.get("request_id") == request_id:
            return record
    return None


def _wait_for_receipt(path: Path, request_id: str, timeout_seconds: float) -> dict[str, Any]:
    deadline = time.perf_counter() + timeout_seconds
    while time.perf_counter() < deadline:
        receipt = _receipt_for_request(path, request_id)
        if receipt is not None:
            return receipt
        time.sleep(0.3)
    raise AssertionError(f"no Telegram delivery receipt for {request_id}")


def _tool_evidence(events: list[dict[str, Any]]) -> tuple[set[str], list[str]]:
    names: set[str] = set()
    output_texts: list[str] = []
    for event in events:
        normalized = _normalize_event_payload(event)
        tool_name = str(normalized.get("tool_name") or "").strip()
        if tool_name:
            names.add(tool_name)
            output_texts.append(json.dumps(normalized, ensure_ascii=False, sort_keys=True))
        for result in normalized.get("tool_results") or ():
            if not isinstance(result, dict):
                continue
            name = str(result.get("tool_name") or result.get("name") or "").strip()
            if name:
                names.add(name)
            output_texts.append(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return names, output_texts


def _artifact_candidates(value: Any) -> list[str]:
    candidates: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"artifact_path", "path"} and isinstance(item, str):
                candidates.append(item)
            elif key in {"artifact_paths", "attachment_paths"} and isinstance(item, list):
                candidates.extend(str(path) for path in item if isinstance(path, str))
            elif key == "artifact_descriptors" and isinstance(item, list):
                for descriptor in item:
                    if isinstance(descriptor, dict) and isinstance(descriptor.get("path"), str):
                        candidates.append(descriptor["path"])
            else:
                candidates.extend(_artifact_candidates(item))
    elif isinstance(value, list):
        for item in value:
            candidates.extend(_artifact_candidates(item))
    return candidates


def _turn_artifacts(turn: dict[str, Any], *, since_epoch: float) -> list[dict[str, str]]:
    candidates = [match.group(1).strip().strip("`'\"") for match in _MEDIA_PATH_RE.finditer(str(turn.get("assistant_reply") or ""))]
    selected_paths = turn.get("artifact_paths")
    if isinstance(selected_paths, list) and selected_paths:
        candidates.extend(str(path) for path in selected_paths if isinstance(path, str))
    else:
        candidates.extend(_artifact_candidates(turn.get("tool_results") or []))
    artifacts: list[dict[str, str]] = []
    seen: set[str] = set()
    for candidate in candidates:
        path = Path(candidate).expanduser()
        try:
            stat = path.stat()
        except OSError:
            continue
        if stat.st_mtime + 5 < since_epoch:
            continue
        resolved = str(path.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        artifacts.append({"name": path.name, "path": resolved})
    return artifacts


def _delivered_artifacts(
    artifacts: list[dict[str, str]],
    receipts: list[dict[str, Any]],
) -> list[dict[str, str]]:
    delivered_names = {
        str(name)
        for receipt in receipts
        for name in (receipt.get("attachment_names") or [])
        if isinstance(name, str) and name
    }
    return [artifact for artifact in artifacts if artifact.get("name") in delivered_names]


def _receipt_artifacts(
    receipt: dict[str, Any],
    *,
    runtime_home: Path,
    workspace_id: str,
) -> list[dict[str, str]]:
    artifacts: list[dict[str, str]] = []
    for attachment_name in receipt.get("attachment_names") or ():
        safe_name = Path(str(attachment_name or "")).name
        if not safe_name:
            continue
        for candidate in (
            runtime_home / "workspaces" / workspace_id / "artifacts" / safe_name,
            runtime_home / safe_name,
        ):
            if candidate.is_file():
                artifacts.append({"name": candidate.name, "path": str(candidate.resolve())})
                break
    return artifacts


def _elapsed_ms(received: dict[str, Any], turn: dict[str, Any]) -> float:
    try:
        start = datetime.fromisoformat(_created_at(received))
        end = datetime.fromisoformat(_created_at(turn))
    except ValueError:
        return 0.0
    return max(0.0, (end - start).total_seconds() * 1000)


def _run_scenario(
    scenario_name: str,
    *,
    runtime_db: Path,
    receipt_path: Path,
    conversation_id: str,
    config: dict[str, Any],
    bot_username: str,
    timeout_seconds: float,
    reset_settle_seconds: float,
) -> dict[str, Any]:
    scenario = _scenario_for_config(SCENARIOS[scenario_name], config)
    assert scenario.runner == "http", f"{scenario.name}: custom runner must use live_stage_chat_contract_probe.py"
    assert not scenario.requires_email_send_flag, f"{scenario.name}: live email send requires separate approval"

    _send_desktop_message("/new", bot_username=bot_username)
    time.sleep(reset_settle_seconds)

    tool_names: set[str] = set()
    tool_output_texts: list[str] = []
    artifacts_by_path: dict[str, dict[str, str]] = {}
    receipts: list[dict[str, Any]] = []
    turn_elapsed: list[float] = []
    final_turn: dict[str, Any] | None = None

    prompts = (scenario.prompt, *scenario.followups)
    for prompt_index, raw_prompt in enumerate(prompts):
        prompt = _materialized_prompt(raw_prompt, scenario_name=scenario.name, index=prompt_index)
        since_dt = datetime.now(UTC)
        since = since_dt.isoformat()
        _send_desktop_message(prompt, bot_username=bot_username)
        received, turn = _wait_for_turn(
            runtime_db,
            conversation_id,
            user_message=prompt,
            since=since,
            timeout_seconds=timeout_seconds,
        )
        final_turn = turn
        request_id = str(received.get("request_id") or "").strip()
        assert request_id, f"{scenario.name}: message_received event missing request_id"
        # The platform turn can be terminal in runtime.db before Telegram finishes
        # uploading a generated artifact and records its delivery receipt.  Keep
        # the receipt boundary under the same scenario budget instead of
        # misreporting slow-but-successful artifact delivery as a product failure.
        receipt = _wait_for_receipt(receipt_path, request_id, timeout_seconds=timeout_seconds)
        assert receipt.get("status") in {"succeeded", "partial"}, f"{scenario.name}: failed receipt {receipt!r}"
        receipts.append(receipt)
        workspace_id = str(turn.get("workspace_id") or "workspace_admin").strip() or "workspace_admin"
        for artifact in _receipt_artifacts(
            receipt,
            runtime_home=runtime_db.parent,
            workspace_id=workspace_id,
        ):
            artifacts_by_path[artifact["path"]] = artifact
        events = _events_since(runtime_db, conversation_id, since)
        names, outputs = _tool_evidence(events)
        tool_names.update(names)
        tool_output_texts.extend(outputs)
        for artifact in _turn_artifacts(turn, since_epoch=since_dt.timestamp()):
            artifacts_by_path[artifact["path"]] = artifact
        turn_elapsed.append(_elapsed_ms(received, turn))

    assert final_turn is not None
    delivered_text = str(receipts[-1].get("delivered_text") or "") if receipts else ""
    response = {
        "text": delivered_text or str(final_turn.get("assistant_reply") or ""),
        "artifacts": _delivered_artifacts(list(artifacts_by_path.values()), receipts),
    }
    _assert_contract(
        scenario,
        response=response,
        elapsed_ms=max(turn_elapsed or [0.0]),
        tool_names=tool_names,
        runtime_db_available=True,
        tool_output_texts=tool_output_texts,
    )
    if response["artifacts"]:
        _download_and_check_artifacts(
            "",
            response["artifacts"],
            scenario=scenario,
            timeout=max(30, int(timeout_seconds)),
        )
    if scenario.required_artifact_extensions:
        missing = set(scenario.required_artifact_extensions).difference(_artifact_extensions(response["artifacts"]))
        assert not missing, f"{scenario.name}: missing Telegram artifact files {sorted(missing)!r}"
        assert any(int(receipt.get("attachment_count") or 0) > 0 for receipt in receipts), (
            f"{scenario.name}: no Telegram receipt proved attachment delivery"
        )
    return {
        "scenario": scenario.name,
        "ok": True,
        "elapsed_ms": round(max(turn_elapsed or [0.0]), 1),
        "tools": sorted(tool_names),
        "artifacts": sorted(_artifact_extensions(response["artifacts"])),
        "receipts": len(receipts),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", action="append", choices=sorted(SCENARIOS))
    parser.add_argument("--deep", action="store_true")
    parser.add_argument("--base-url", default="http://127.0.0.1:8753")
    parser.add_argument("--runtime-db", type=Path, default=Path.home() / ".nullion-stage" / "runtime.db")
    parser.add_argument("--env-file", type=Path, default=Path.home() / ".nullion-stage" / ".env")
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--reset-settle-seconds", type=float, default=2.0)
    parser.add_argument("--bot-username", default="nullion_stage_bot")
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument("--yes-send-live-messages", action="store_true")
    args = parser.parse_args(argv)
    if not args.yes_send_live_messages:
        raise SystemExit("pass --yes-send-live-messages to send the real Telegram Desktop test batch")

    env_values = _read_env_file(args.env_file.expanduser())
    chat_id = str(env_values.get("NULLION_TELEGRAM_OPERATOR_CHAT_ID") or "").strip()
    if not chat_id:
        raise SystemExit("stage Telegram operator chat id is not configured")
    conversation_id = f"telegram:{chat_id}"
    receipt_path = args.env_file.expanduser().parent / "delivery_receipts.jsonl"
    config = _get_json(f"{args.base_url}/api/config", timeout=30)

    selected = args.scenario or list(DEFAULT_LIVE_STAGE_SCENARIOS)
    if args.deep and args.scenario is None:
        selected.extend(DEEP_LIVE_STAGE_SCENARIOS)
    selected = list(dict.fromkeys(selected))
    failures: list[str] = []
    summary: list[dict[str, Any]] = []
    for scenario_name in selected:
        scenario = SCENARIOS[scenario_name]
        if scenario.runner != "http":
            summary.append({"scenario": scenario_name, "ok": True, "skipped": "custom_runner"})
            continue
        if scenario.requires_email_send_flag:
            summary.append({"scenario": scenario_name, "ok": True, "skipped": "email_send_requires_approval"})
            continue
        print(json.dumps({"event": "scenario_start", "scenario": scenario_name}), flush=True)
        try:
            result = _run_scenario(
                scenario_name,
                runtime_db=args.runtime_db.expanduser(),
                receipt_path=receipt_path,
                conversation_id=conversation_id,
                config=config,
                bot_username=args.bot_username,
                timeout_seconds=args.timeout,
                reset_settle_seconds=args.reset_settle_seconds,
            )
            summary.append(result)
            print(json.dumps({"event": "scenario_pass", **result}), flush=True)
        except Exception as exc:
            failure = f"{scenario_name}: {exc}"
            failures.append(failure)
            summary.append({"scenario": scenario_name, "ok": False, "error": str(exc)})
            print(json.dumps({"event": "scenario_failure", "scenario": scenario_name, "error": str(exc)}), flush=True)
            if not args.continue_on_failure:
                break

    print(json.dumps({"ok": not failures, "summary": summary, "failures": failures}, indent=2, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
