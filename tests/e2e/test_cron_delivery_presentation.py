"""Exercise cron results through the delivery graph and Telegram Bot API wire."""

import asyncio
from contextlib import contextmanager
import json
import os
import threading

import pytest

from tests.e2e.test_generated_artifact_protocol_recovery import upload_boundary
from tests.e2e.test_scheduled_report_completion import setup_runtime


pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Container E2E only")


def _job(*formats):
    from nullion.crons import CronJob

    return CronJob(
        "presentation-fixture", "Informe de viaje", "0 * * * *", "旅行の結果を届ける。",
        delivery_channel="telegram", delivery_target="42",
        artifact_delivery_options={
            "required_artifact_extensions": list(formats),
            "deliver_required_extensions_only": bool(formats),
        },
    )


@contextmanager
def _delivery_boundary(tmp_path, monkeypatch, produce):
    from nullion import telegram_entrypoint
    from nullion.cron_delivery import CronRunDeliveryCallbacks, cron_delivery_block_reason
    from nullion.events import make_event

    runtime = setup_runtime(tmp_path, monkeypatch)
    wire = []

    def record(event, job, channel, target, conversation_id, **metadata):
        runtime.store.add_event(make_event(event, "cron_scheduler", {"cron_id": job.id, **metadata}))
        runtime.checkpoint()

    def send(job, channel, target, body, **options):
        assert channel == "telegram"
        return asyncio.run(telegram_entrypoint._send_operator_telegram_delivery(
            "123456:synthetic", target, body, principal_id=f"telegram:{target}", **options,
        ))

    with upload_boundary(wire) as (endpoint, uploads):
        monkeypatch.setattr(telegram_entrypoint, "build_telegram_bot", lambda bot, token: bot(token, base_url=endpoint))
        callbacks = CronRunDeliveryCallbacks(
            effective_channel=lambda job: job.delivery_channel,
            delivery_target=lambda job, channel: job.delivery_target,
            run_agent_turn=produce,
            record_event=record,
            block_reason=cron_delivery_block_reason,
            save_web_delivery=lambda *args: False,
            send_platform_delivery=send,
            health_store_path=runtime.checkpoint_path,
        )
        yield callbacks, wire, uploads


def _wire_responses(wire):
    return [payload for method, payload in wire if method in {"sendMessage", "sendDocument"}]


def _recorded_delivery_events(checkpoint):
    from nullion.runtime_persistence import load_runtime_store

    return [event.event_type for event in load_runtime_store(checkpoint).list_events()
            if event.event_type.startswith("cron.delivery.")]


@pytest.mark.parametrize("manual_status", [None, True, False], ids=["scheduled", "manual-card", "manual-fallback"])
def test_cron_provider_failure_delivers_one_safe_notice(tmp_path, monkeypatch, manual_status):
    from nullion.cron_delivery import run_cron_delivery_workflow, start_manual_cron_background_delivery

    job = _job()
    diagnostic = 'provider response {"diagnostic_field":"private-fixture"}'

    def produce(*args):
        raise ValueError(diagnostic)

    with _delivery_boundary(tmp_path, monkeypatch, produce) as (callbacks, wire, uploads):
        if manual_status is not None:
            done = threading.Event()

            def status_update(group_id, text, terminal):
                if not manual_status or not terminal:
                    return False
                return callbacks.send_platform_delivery(job, "telegram", "42", text, exact_artifact_paths=())

            receipt = start_manual_cron_background_delivery(
                job, label="Manual scheduled task run", callbacks=callbacks, origin_conversation_id="telegram:42",
                status_update_callback=status_update, status_update_interval_seconds=0,
                background_start_grace_seconds=0, before_run=lambda: done.set,
            )
            assert receipt["cron_delivery_status"] == "deferred"
            assert done.wait(timeout=5)
        else:
            result = run_cron_delivery_workflow(job, label="Scheduled task", callbacks=callbacks)
            assert result["reason"] == "cron_run_exception"
            assert result["cron_failure_notice_sent"] is True
            assert result["execution_outcome"]["execution_succeeded"] is False
        assert _recorded_delivery_events(callbacks.health_store_path) == ["cron.delivery.started", "cron.delivery.failed"]

    assert uploads == []
    payload, = _wire_responses(wire)
    assert "Informe de viaje" in payload["text"]
    assert "Try running it again." in payload["text"]
    assert "private-fixture" not in payload["text"]
    assert "diagnostic_field" not in payload["text"]


def test_cron_renders_report_envelope_before_adding_the_scheduled_heading(tmp_path, monkeypatch):
    from nullion.chat_text import REPORT_MESSAGE_FORMAT
    from nullion.cron_delivery import run_cron_delivery_workflow

    summary = "Asunción → Cusco · 旅程"
    envelope = json.dumps({"format": REPORT_MESSAGE_FORMAT, "message": {"summary": summary}}, ensure_ascii=False)
    with _delivery_boundary(tmp_path, monkeypatch, lambda *args: {
        "text": envelope, "response_fulfilled": True,
    }) as (callbacks, wire, uploads):
        result = run_cron_delivery_workflow(_job(), label="Scheduled task", callbacks=callbacks)
        assert _recorded_delivery_events(callbacks.health_store_path)[-1] == "cron.delivery.sent"

    assert result["cron_delivery_status"] == "sent"
    assert uploads == []
    payload, = _wire_responses(wire)
    assert "SCHEDULED TASK: Informe de viaje" in payload["text"]
    assert summary in payload["text"]
    assert REPORT_MESSAGE_FORMAT not in payload["text"]


def _reviewed_artifacts(paths, *, formats):
    from nullion.artifact_lifecycle import record_delivery_manifest
    from nullion.tools import ToolResult

    selected = list(map(str, paths))
    results = [ToolResult("producer", "terminal_exec", "completed", {"artifact_paths": selected})]
    record_delivery_manifest(results, selected, review_status="completed", required_extensions=formats)
    return {
        "text": "Informe preparado.",
        "response_fulfilled": True,
        "artifact_delivery_satisfied": True,
        "artifacts": selected,
        "tool_results": results,
    }


@pytest.mark.parametrize("suffix", [".htm", ".html"])
def test_cron_sends_reviewed_html_alias_for_html_only_job(tmp_path, monkeypatch, suffix):
    from nullion.artifacts import artifact_root_for_principal
    from nullion.cron_delivery import run_cron_delivery_workflow

    def produce(*args):
        return _reviewed_artifacts([report], formats=[".html"])

    with _delivery_boundary(tmp_path, monkeypatch, produce) as (callbacks, wire, uploads):
        root = artifact_root_for_principal("telegram:42")
        root.mkdir(parents=True, exist_ok=True)
        report = root / ("viaje" + suffix)
        document = '<!doctype html><html lang="es"><head><meta charset="utf-8"></head><body>Asunción → Cusco</body></html>'
        report.write_text(document, encoding="utf-8")
        result = run_cron_delivery_workflow(_job(".html"), label="Scheduled task", callbacks=callbacks)
        assert _recorded_delivery_events(callbacks.health_store_path)[-1] == "cron.delivery.sent"

    assert result["cron_delivery_status"] == "sent"
    assert [str(report)] == result["execution_outcome"]["artifacts_delivered"]
    assert uploads == [document.encode("utf-8")]
    assert [method for method, _ in wire if method.startswith("send")] == ["sendDocument"]


def test_cron_delivers_verified_file_with_candid_caption_after_model_failure(tmp_path, monkeypatch):
    from nullion.artifacts import artifact_root_for_principal
    from nullion.cron_delivery import run_cron_delivery_workflow

    def produce(*args):
        return {**_reviewed_artifacts([report], formats=[".html"]), "model_response_failed": True,
                "text": "Synthetic provider error after producing the requested file"}

    with _delivery_boundary(tmp_path, monkeypatch, produce) as (callbacks, wire, uploads):
        root = artifact_root_for_principal("telegram:42")
        root.mkdir(parents=True, exist_ok=True)
        report = root / "viaje.html"
        document = '<!doctype html><html><head><meta charset="utf-8"></head><body>確認済みの旅行データ</body></html>'
        report.write_text(document, encoding="utf-8")
        result = run_cron_delivery_workflow(_job(".html"), label="Scheduled task", callbacks=callbacks)

    assert result["cron_delivery_status"] == "sent"
    assert uploads == [document.encode("utf-8")]
    payload, = _wire_responses(wire)
    assert "Report attached" in payload["caption"]
    assert "data and limitations" in payload["caption"]
    assert "Synthetic provider error" not in payload["caption"]


def test_cron_rechecks_required_formats_after_manifest_excludes_revised_file(tmp_path, monkeypatch):
    from nullion.artifacts import artifact_root_for_principal
    from nullion.cron_delivery import run_cron_delivery_workflow
    from nullion.tools import ToolResult

    def produce(*args):
        root = artifact_root_for_principal("telegram:42")
        root.mkdir(parents=True, exist_ok=True)
        html = root / "viaje.html"
        text = root / "viaje.txt"
        html.write_text("<!doctype html><html><body>First review version</body></html>", encoding="utf-8")
        text.write_text("Travel report\n", encoding="utf-8")
        result = _reviewed_artifacts([html, text], formats=[".html", ".txt"])
        html.write_text("<!doctype html><html><body>Unreviewed revision</body></html>", encoding="utf-8")
        result["tool_results"].append(ToolResult("new-producer", "terminal_exec", "completed", {"artifact_path": str(html)}))
        return result

    with _delivery_boundary(tmp_path, monkeypatch, produce) as (callbacks, wire, uploads):
        result = run_cron_delivery_workflow(_job(".html", ".txt"), label="Scheduled task", callbacks=callbacks)
        assert _recorded_delivery_events(callbacks.health_store_path)[-1] == "cron.delivery.failed"

    assert result["reason"] == "cron_required_artifact_missing"
    assert result["execution_outcome"]["execution_succeeded"] is False
    assert result["execution_outcome"]["artifacts_delivered"] == []
    assert uploads == []
    assert _wire_responses(wire) == []
