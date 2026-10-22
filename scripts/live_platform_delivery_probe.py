#!/usr/bin/env python3
"""Probe real chat-platform delivery with platform receipts.

This script is intentionally opt-in because it sends a real message to the
configured chat platform. It is for validating the last delivery hop after
local/unit tests pass.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import zipfile
from xml.sax.saxutils import escape


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def _install_app_repo(app_repo: Path) -> None:
    source_root = app_repo.expanduser().resolve() / "src"
    if not source_root.exists():
        raise AssertionError(f"app repo source root does not exist: {source_root}")
    sys.path.insert(0, str(source_root))


def _write_minimal_xlsx(path: Path, *, marker: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cell_text = escape(f"Nullion live platform delivery probe {marker}")
    created_at = escape(datetime.now(UTC).isoformat())
    files = {
        "[Content_Types].xml": """<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>
""",
        "_rels/.rels": """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>
""",
        "xl/_rels/workbook.xml.rels": """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>
""",
        "xl/workbook.xml": """<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"
          xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Delivery Probe" sheetId="1" r:id="rId1"/>
  </sheets>
</workbook>
""",
        "xl/worksheets/sheet1.xml": f"""<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1">
      <c r="A1" t="inlineStr"><is><t>{cell_text}</t></is></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>{created_at}</t></is></c>
    </row>
  </sheetData>
</worksheet>
""",
    }
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        for name, contents in files.items():
            workbook.writestr(name, contents)


def _latest_receipt(receipt_path: Path, *, request_id: str, timeout_seconds: float) -> dict[str, object]:
    deadline = time.perf_counter() + timeout_seconds
    while True:
        try:
            lines = receipt_path.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError:
            lines = []
        for line in reversed(lines):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(record, dict) and record.get("request_id") == request_id:
                return record
        if time.perf_counter() >= deadline:
            raise AssertionError(f"no delivery receipt recorded for request_id={request_id}")
        time.sleep(0.5)


def _desktop_screenshot(path: Path) -> dict[str, object]:
    path.parent.mkdir(parents=True, exist_ok=True)
    state = {"attempted": True, "window_count": None, "screenshot": str(path)}
    subprocess.run(["open", "-b", "ru.keepcoder.Telegram"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.0)
    script = (
        'tell application id "ru.keepcoder.Telegram" to activate\n'
        "delay 0.5\n"
        'tell application "System Events" to tell process "Telegram" to get count of windows\n'
    )
    completed = subprocess.run(["osascript", "-e", script], text=True, capture_output=True, check=False)
    try:
        state["window_count"] = int((completed.stdout or "").strip())
    except ValueError:
        state["window_count"] = None
    subprocess.run(["screencapture", "-x", str(path)], check=False)
    return state


async def _run_telegram(args: argparse.Namespace) -> dict[str, object]:
    app_repo = Path(args.app_repo or os.environ.get("NULLION_APP_REPO") or Path.cwd()).expanduser()
    _install_app_repo(app_repo)

    env_file = Path(args.env_file).expanduser().resolve()
    env_values = _read_env_file(env_file)
    nullion_home = Path(args.nullion_home or env_values.get("NULLION_HOME") or env_file.parent).expanduser().resolve()
    for key, value in env_values.items():
        if key.startswith("NULLION_"):
            os.environ[key] = value
    os.environ["NULLION_HOME"] = str(nullion_home)
    if "NULLION_DATA_DIR" not in env_values:
        os.environ.pop("NULLION_DATA_DIR", None)

    bot_token = os.environ.get("NULLION_TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = str(args.telegram_chat_id or os.environ.get("NULLION_TELEGRAM_OPERATOR_CHAT_ID") or "").strip()
    if not bot_token:
        raise AssertionError("stage Telegram bot token is not configured")
    if not chat_id:
        raise AssertionError("stage Telegram operator chat id is not configured")

    from nullion.telegram_entrypoint import _send_operator_telegram_delivery

    marker = args.marker or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    request_id = args.request_id or f"live-platform-telegram-{marker}"
    artifact_root = Path(args.artifact_root or nullion_home / "workspaces" / "workspace_admin" / "artifacts")
    artifact_path = artifact_root / f"{request_id}.xlsx"
    _write_minimal_xlsx(artifact_path, marker=request_id)

    ok = await _send_operator_telegram_delivery(
        bot_token,
        chat_id,
        f"Telegram live delivery probe {request_id}.\n\nMEDIA:{artifact_path}",
        principal_id=f"telegram:{chat_id}",
        suppress_link_preview=True,
        request_id=request_id,
    )
    if not ok:
        raise AssertionError("Telegram delivery helper returned False")

    receipt = _latest_receipt(
        nullion_home / "delivery_receipts.jsonl",
        request_id=request_id,
        timeout_seconds=args.receipt_timeout,
    )
    failures: list[str] = []
    if receipt.get("channel") != "telegram":
        failures.append(f"channel={receipt.get('channel')!r}")
    if str(receipt.get("target_id") or "") != chat_id:
        failures.append(f"target_id={receipt.get('target_id')!r}")
    if receipt.get("status") not in {"succeeded", "partial"}:
        failures.append(f"status={receipt.get('status')!r}")
    if int(receipt.get("attachment_count") or 0) < 1:
        failures.append(f"attachment_count={receipt.get('attachment_count')!r}")
    if not receipt.get("attachment_required"):
        failures.append("attachment_required=false")
    if not str(receipt.get("message_id") or "").strip():
        failures.append("missing message_id")
    if receipt.get("error"):
        failures.append(f"error={receipt.get('error')!r}")
    if failures:
        raise AssertionError(f"Telegram delivery receipt did not prove attachment delivery: {', '.join(failures)}")

    desktop = None
    if args.telegram_desktop_screenshot:
        desktop = _desktop_screenshot(Path(args.telegram_desktop_screenshot).expanduser().resolve())

    return {
        "ok": True,
        "platform": "telegram",
        "request_id": request_id,
        "artifact": {
            "name": artifact_path.name,
            "path": str(artifact_path),
            "size": artifact_path.stat().st_size,
        },
        "receipt": {
            "status": receipt.get("status"),
            "target_id": receipt.get("target_id"),
            "attachment_count": receipt.get("attachment_count"),
            "message_id": receipt.get("message_id"),
            "created_at": receipt.get("created_at"),
        },
        "desktop": desktop,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", choices=("telegram",), default="telegram")
    parser.add_argument("--app-repo", type=Path, help="Nullion app checkout to import")
    parser.add_argument("--env-file", type=Path, default=Path.home() / ".nullion-stage" / ".env")
    parser.add_argument("--nullion-home", type=Path, help="Runtime home for receipts and artifacts")
    parser.add_argument("--telegram-chat-id", help="Telegram chat id to receive the live probe")
    parser.add_argument("--artifact-root", type=Path, help="Directory for the generated probe XLSX")
    parser.add_argument("--request-id", help="Unique delivery request id to assert in receipts")
    parser.add_argument("--marker", help="Human-visible marker included in the probe workbook")
    parser.add_argument("--receipt-timeout", type=float, default=10.0)
    parser.add_argument("--telegram-desktop-screenshot", type=Path, help="Optional screenshot path after sending")
    parser.add_argument(
        "--yes-send-live-message",
        action="store_true",
        help="Required safety flag: this sends a real platform message.",
    )
    args = parser.parse_args(argv)

    if not args.yes_send_live_message and os.environ.get("NULLION_LIVE_PLATFORM_DELIVERY") != "1":
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "set NULLION_LIVE_PLATFORM_DELIVERY=1 or pass --yes-send-live-message",
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2

    try:
        if args.platform == "telegram":
            result = asyncio.run(_run_telegram(args))
        else:  # pragma: no cover - argparse prevents this.
            raise AssertionError(f"unsupported platform: {args.platform}")
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, indent=2, sort_keys=True))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
