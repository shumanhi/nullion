from __future__ import annotations

import json
import sqlite3
import threading
import time
import zipfile
from io import BytesIO
from datetime import UTC, datetime, timedelta

import pytest

from scripts import live_stage_chat_contract_probe
from scripts.live_stage_chat_contract_probe import Scenario, _event_tool_names
from scripts.live_stage_telegram_desktop_probe import _delivered_artifacts, _receipt_artifacts, _turn_artifacts


@pytest.mark.parametrize("persist_capture", [True, False])
@pytest.mark.parametrize("tool_name", ["browser_screenshot", "weather_forecast"])
def test_capture_probe_waits_for_full_sqlite_receipt_not_timing(tmp_path, persist_capture, tool_name):
    db_path = tmp_path / "runtime.db"
    conversation_id = "web:captured-page-receipt"

    def persist(payload):
        payload.update(conversation_id=conversation_id, created_at=datetime.now(UTC).isoformat())
        with sqlite3.connect(db_path) as conn:
            conn.execute("INSERT INTO conversation_events VALUES (?, ?)",
                         (json.dumps(payload), payload["created_at"]))

    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE conversation_events (payload TEXT, updated_at TEXT)")
    persist({"event_type": "conversation.chat_turn", "tool_results": [{
        "tool_name": "browser_navigate", "status": "completed", "output": {"session_id": "page"},
    }]})
    persist({"event_type": "conversation.tool_timing", "tool_name": tool_name, "status": "completed"})

    def write_capture():
        time.sleep(0.05)
        if persist_capture:
            persist({"event_type": "conversation.chat_turn", "tool_results": [{
                "tool_name": tool_name, "status": "completed",
                "output": {"session_id": "page", "page_url": "https://example.com/"},
            }]})

    worker = threading.Thread(target=write_capture)
    worker.start()
    try:
        outputs = live_stage_chat_contract_probe._event_tool_output_texts_with_poll(
            db_path, conversation_id, required_output_tools=(tool_name,), timeout_seconds=0.3,
        )
    finally:
        worker.join(timeout=1)
    scenario = Scenario("capture", "", None, None, required_tool_all=(tool_name,), require_loaded_browser_capture=tool_name == "browser_screenshot")
    if persist_capture:
        live_stage_chat_contract_probe._assert_contract(
            scenario, response={}, elapsed_ms=1, tool_names={tool_name},
            runtime_db_available=True, tool_output_texts=outputs,
        )
    else:
        with pytest.raises(AssertionError, match="loaded-page capture|expected completed results"):
            live_stage_chat_contract_probe._assert_contract(
                scenario, response={}, elapsed_ms=1, tool_names={tool_name},
                runtime_db_available=True, tool_output_texts=outputs,
            )


def test_probe_without_receipt_requirement_does_not_wait_for_output(tmp_path):
    assert live_stage_chat_contract_probe._event_tool_output_texts_with_poll(
        tmp_path / "missing.db", "web:plain-turn", timeout_seconds=30,
    ) == []


def test_live_stage_probe_scans_enough_runtime_rows_for_tool_timings(tmp_path) -> None:
    db_path = tmp_path / "runtime.db"
    conversation_id = "web:live-stage-contract:spreadsheet_visual_workbook_pack:unit:0"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE runtime_items (collection TEXT, payload TEXT, updated_at TEXT)"
        )
        now = datetime.now(UTC)
        for index in range(20):
            payload = {
                "event_type": "conversation.message_received",
                "conversation_id": conversation_id,
            }
            conn.execute(
                "INSERT INTO runtime_items VALUES (?, ?, ?)",
                (
                    "conversation_events",
                    json.dumps(payload),
                    (now + timedelta(seconds=index)).isoformat(),
                ),
            )
        for tool_name in ("browser_image_collect", "spreadsheet_create", "pdf_create", "file_write"):
            payload = {
                "event_type": "conversation.tool_timing",
                "conversation_id": conversation_id,
                "tool_name": tool_name,
                "status": "completed",
            }
            conn.execute(
                "INSERT INTO runtime_items VALUES (?, ?, ?)",
                (
                    "conversation_events",
                    json.dumps(payload),
                    (now - timedelta(seconds=10)).isoformat(),
                ),
            )

    assert {
        "browser_image_collect",
        "spreadsheet_create",
        "pdf_create",
        "file_write",
    }.issubset(_event_tool_names(db_path, conversation_id))


def test_telegram_desktop_probe_prefers_selected_turn_artifacts_over_intermediate_tools(tmp_path) -> None:
    html = tmp_path / "selected-report.html"
    svg = tmp_path / "intermediate-cover.svg"
    html.write_text("<html><body>Selected</body></html>", encoding="utf-8")
    svg.write_text("<svg></svg>", encoding="utf-8")
    turn = {
        "assistant_reply": f"Created `{html.name}`.",
        "artifact_paths": [str(html)],
        "tool_results": [
            {"tool_name": "file_write", "output": {"artifact_path": str(html)}},
            {"tool_name": "file_write", "output": {"artifact_path": str(svg)}},
        ],
    }

    artifacts = _turn_artifacts(turn, since_epoch=0)

    assert artifacts == [{"name": html.name, "path": str(html.resolve())}]


def test_telegram_desktop_probe_uses_actual_delivery_receipt_artifacts(tmp_path) -> None:
    html = {"name": "final.html", "path": str(tmp_path / "final.html")}
    svg = {"name": "helper.svg", "path": str(tmp_path / "helper.svg")}

    assert _delivered_artifacts(
        [html, svg],
        [{"attachment_names": ["final.html"]}],
    ) == [html]


@pytest.mark.parametrize("receipts", [[], [{"attachment_count": 0, "attachment_names": []}],
    [{"status": "failed", "attachment_count": 0, "attachment_names": []}]])
def test_telegram_probe_does_not_count_internal_capture_as_delivered(tmp_path, receipts):
    capture = {"name": "browser-evidence.png", "path": str(tmp_path / "browser-evidence.png")}
    assert _delivered_artifacts([capture], receipts) == []


def test_telegram_desktop_probe_resolves_receipt_attachment_missing_from_turn_metadata(tmp_path) -> None:
    workbook = tmp_path / "workspaces" / "workspace_admin" / "artifacts" / "final.xlsx"
    workbook.parent.mkdir(parents=True)
    workbook.write_bytes(b"PK\x03\x04 workbook")

    assert _receipt_artifacts(
        {"attachment_names": ["final.xlsx"]},
        runtime_home=tmp_path,
        workspace_id="workspace_admin",
    ) == [{"name": "final.xlsx", "path": str(workbook.resolve())}]


def test_live_stage_probe_poll_waits_for_required_all_tools(monkeypatch) -> None:
    calls = [
        {"browser_image_collect"},
        {"browser_image_collect", "spreadsheet_create", "pdf_create", "file_write"},
    ]

    def fake_event_tool_names(_runtime_db, _conversation_id, **_kwargs):
        return calls.pop(0) if calls else {"browser_image_collect", "spreadsheet_create", "pdf_create", "file_write"}

    monkeypatch.setattr(live_stage_chat_contract_probe, "_event_tool_names", fake_event_tool_names)

    assert live_stage_chat_contract_probe._event_tool_names_with_poll(
        None,
        "web:live-stage-contract:spreadsheet_visual_workbook_pack:unit:0",
        required_tool_any=("browser_image_collect",),
        required_tool_all=("spreadsheet_create", "pdf_create", "file_write"),
        timeout_seconds=1,
    ) == {"browser_image_collect", "spreadsheet_create", "pdf_create", "file_write"}


def test_live_stage_probe_missing_tool_poll_uses_short_post_turn_grace(monkeypatch) -> None:
    ticks = iter((10.0, 12.0, 16.0))
    sleeps: list[float] = []

    monkeypatch.setattr(live_stage_chat_contract_probe, "_event_tool_names", lambda *_args, **_kwargs: {"market_quote"})
    monkeypatch.setattr(live_stage_chat_contract_probe.time, "perf_counter", lambda: next(ticks))
    monkeypatch.setattr(live_stage_chat_contract_probe.time, "sleep", sleeps.append)

    assert live_stage_chat_contract_probe._event_tool_names_with_poll(
        None,
        "web:live-stage-contract:missing-tools:unit:0",
        required_tool_all=("calendar_list", "weather_forecast"),
        timeout_seconds=5,
    ) == {"market_quote"}
    assert sleeps == [0.2]


def test_live_stage_probe_waits_for_receipts_persisted_after_five_seconds(monkeypatch) -> None:
    ticks = iter((0.0, 6.0))
    receipts = iter((set(), {"weather_forecast"}))
    monkeypatch.setattr(live_stage_chat_contract_probe.time, "perf_counter", lambda: next(ticks))
    monkeypatch.setattr(live_stage_chat_contract_probe.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(live_stage_chat_contract_probe, "_event_tool_names", lambda *_args, **_kwargs: next(receipts))
    assert live_stage_chat_contract_probe._event_tool_names_with_poll(
        None, "web:late-receipt", required_tool_any=("weather_forecast",),
    ) == {"weather_forecast"}


def test_live_stage_probe_detects_namespaced_xlsx_formula_xml() -> None:
    assert live_stage_chat_contract_probe._xlsx_sheet_xml_has_formula(b"<f>D2-E2</f>")
    assert live_stage_chat_contract_probe._xlsx_sheet_xml_has_formula(b"<ns0:f>D2-E2</ns0:f>")
    assert not live_stage_chat_contract_probe._xlsx_sheet_xml_has_formula(b"<v>1</v>")


def test_live_stage_probe_extracts_xlsx_sheet_names() -> None:
    workbook_xml = (
        b'<?xml version="1.0"?><workbook><sheets>'
        b'<sheet name="Values" sheetId="1"/><sheet name="Summary" sheetId="2"/>'
        b"</sheets></workbook>"
    )
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as package:
        package.writestr("xl/workbook.xml", workbook_xml)

    assert live_stage_chat_contract_probe._xlsx_sheet_names(buffer.getvalue()) == ("Values", "Summary")


def test_live_stage_probe_reads_active_followup_parent_evidence(tmp_path) -> None:
    db_path = tmp_path / "runtime.db"
    conversation_id = "web:live-stage-contract:active_followup_context_guard:unit:0"
    now = datetime.now(UTC)
    payloads = [
        {
            "conversation_id": conversation_id,
            "created_at": now.isoformat(),
            "turn_id": "turn-live-weather",
            "status": "accepted",
            "parent_turn_id": None,
            "user_message": "weather",
        },
        {
            "conversation_id": conversation_id,
            "created_at": (now + timedelta(seconds=1)).isoformat(),
            "turn_id": "turn-live-artifact",
            "status": "accepted",
            "parent_turn_id": None,
            "user_message": "artifact",
        },
        {
            "conversation_id": conversation_id,
            "created_at": (now + timedelta(seconds=2)).isoformat(),
            "turn_id": "turn-live-sure",
            "status": "accepted",
            "parent_turn_id": "turn-live-weather",
            "user_message": "Sure",
        },
        {
            "conversation_id": conversation_id,
            "created_at": (now + timedelta(seconds=3)).isoformat(),
            "event_type": "conversation.chat_turn",
            "user_message": "Sure",
            "assistant_reply": "Great — here is the hour-by-hour forecast.",
        },
    ]
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE conversation_events (collection TEXT, item_key TEXT, payload TEXT, updated_at TEXT)"
        )
        for index, payload in enumerate(payloads):
            conn.execute(
                "INSERT INTO conversation_events VALUES (?, ?, ?, ?)",
                (
                    "conversation_events",
                    f"item-{index}",
                    json.dumps(payload),
                    (now + timedelta(seconds=index)).isoformat(),
                ),
            )

    assert live_stage_chat_contract_probe._websocket_url("http://127.0.0.1:8753") == "ws://127.0.0.1:8753/ws/chat"
    sure_turn = live_stage_chat_contract_probe._turn_payload(db_path, conversation_id, "turn-live-sure")
    assert sure_turn is not None
    assert sure_turn["parent_turn_id"] == "turn-live-weather"
    chat_turn = live_stage_chat_contract_probe._chat_turn_for_user_message(db_path, conversation_id, "Sure")
    assert chat_turn is not None
    assert "hour-by-hour" in chat_turn["assistant_reply"]


def test_live_stage_probe_uses_newest_matching_chat_turn(tmp_path) -> None:
    db_path = tmp_path / "runtime.db"
    conversation_id = "web:live-stage-contract:scheduler_fresh_followup_after_bulk_enable:unit:0"
    now = datetime.now(UTC)
    payloads = [
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": now.isoformat(),
            "user_message": "Do a fresh check",
            "assistant_reply": "old stale branch",
            "tool_results": [{"tool_name": "run_cron", "status": "failed"}],
        },
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.chat_turn",
            "created_at": (now + timedelta(seconds=1)).isoformat(),
            "user_message": "Do a fresh check",
            "assistant_reply": "fresh inventory",
            "tool_results": [{"tool_name": "list_crons", "status": "completed"}],
        },
    ]
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE conversation_events (collection TEXT, item_key TEXT, payload TEXT, updated_at TEXT)"
        )
        for index, payload in enumerate(payloads):
            conn.execute(
                "INSERT INTO conversation_events VALUES (?, ?, ?, ?)",
                (
                    "conversation_events",
                    f"item-{index}",
                    json.dumps(payload),
                    (now + timedelta(seconds=index)).isoformat(),
                ),
            )

    chat_turn = live_stage_chat_contract_probe._chat_turn_for_user_message(
        db_path,
        conversation_id,
        "Do a fresh check",
    )

    assert chat_turn is not None
    assert chat_turn["assistant_reply"] == "fresh inventory"
    assert chat_turn["tool_results"] == [{"tool_name": "list_crons", "status": "completed"}]


def test_live_stage_probe_reads_turn_specific_latency_total_from_marks(tmp_path) -> None:
    db_path = tmp_path / "runtime.db"
    conversation_id = "web:live-stage-contract:active_related_followup_context_guard:unit:0"
    now = datetime.now(UTC)
    payloads = [
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.latency_timing",
            "turn_id": "turn-live-related-artifact",
            "created_at": now.isoformat(),
            "marks": [{"name": "save_done", "ms": 133934.0}],
        },
        {
            "conversation_id": conversation_id,
            "event_type": "conversation.latency_timing",
            "turn_id": "turn-live-related-followup",
            "created_at": (now + timedelta(seconds=1)).isoformat(),
            "marks": [
                {"name": "model_start", "ms": 13899.5},
                {"name": "save_done", "ms": 53499.3},
            ],
        },
    ]
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE conversation_events (collection TEXT, item_key TEXT, payload TEXT, updated_at TEXT)"
        )
        for index, payload in enumerate(payloads):
            conn.execute(
                "INSERT INTO conversation_events VALUES (?, ?, ?, ?)",
                (
                    "conversation_events",
                    f"item-{index}",
                    json.dumps(payload),
                    (now + timedelta(seconds=index)).isoformat(),
                ),
            )

    assert live_stage_chat_contract_probe._latest_turn_latency_total_ms(
        db_path,
        conversation_id,
        "turn-live-related-followup",
    ) == pytest.approx(53499.3)


def test_live_stage_probe_reads_conversation_events_with_stage_expression_index(tmp_path) -> None:
    db_path = tmp_path / "runtime.db"
    target_conversation_id = "web:live-stage-contract:planner_complex_artifact_workbook:unit:0"
    other_conversation_id = "web:live-stage-contract:other:unit:0"
    now = datetime.now(UTC)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE runtime_items (collection TEXT, item_key TEXT, payload TEXT, updated_at TEXT)"
        )
        conn.execute(
            "INSERT INTO runtime_items VALUES (?, ?, ?, ?)",
            (
                "conversation_events",
                "generic-event-copy",
                json.dumps(
                    {
                        "conversation_id": target_conversation_id,
                        "event_type": "conversation.latency_timing",
                        "turn_id": "turn-from-generic-runtime-items",
                        "created_at": now.isoformat(),
                    }
                ),
                now.isoformat(),
            ),
        )
        conn.execute(
            "CREATE TABLE conversation_events (collection TEXT, item_key TEXT, payload TEXT, updated_at TEXT)"
        )
        conn.execute(
            """
            CREATE INDEX idx_conversation_events_conversation_created
            ON conversation_events (
                json_extract(payload, '$.conversation_id'),
                json_extract(payload, '$.created_at')
            )
            """
        )
        for index in range(25):
            conversation_id = target_conversation_id if index % 2 == 0 else other_conversation_id
            payload = {
                "conversation_id": conversation_id,
                "event_type": "conversation.latency_timing",
                "turn_id": f"turn-{index}",
                "created_at": (now + timedelta(seconds=index)).isoformat(),
                "marks": [{"name": "save_done", "ms": float(index)}],
            }
            conn.execute(
                "INSERT INTO conversation_events VALUES (?, ?, ?, ?)",
                (
                    "conversation_events",
                    f"item-{index}",
                    json.dumps(payload),
                    (now + timedelta(seconds=index)).isoformat(),
                ),
            )

    payloads = live_stage_chat_contract_probe._conversation_event_payloads(
        db_path,
        target_conversation_id,
        event_type="conversation.latency_timing",
    )

    assert [payload["turn_id"] for payload in payloads] == [
        f"turn-{index}" for index in range(0, 25, 2)
    ]
    assert {payload["conversation_id"] for payload in payloads} == {target_conversation_id}


def test_live_stage_probe_plain_chat_fast_path_requires_no_tools_or_artifacts() -> None:
    scenario = Scenario(
        name="plain_chat_attachment_planner_fast_path",
        prompt="How are you?",
        max_average_ms=5000,
        max_single_ms=8000,
        require_no_artifacts=True,
        require_no_tools=True,
    )

    live_stage_chat_contract_probe._assert_contract(
        scenario,
        response={"text": "Doing well."},
        elapsed_ms=1200,
        tool_names=set(),
        runtime_db_available=True,
    )

    with pytest.raises(AssertionError, match="expected no tool use"):
        live_stage_chat_contract_probe._assert_contract(
            scenario,
            response={"text": "Doing well."},
            elapsed_ms=1200,
            tool_names={"browser_navigate"},
            runtime_db_available=True,
        )

    with pytest.raises(AssertionError, match="expected no artifacts"):
        live_stage_chat_contract_probe._assert_contract(
            scenario,
            response={"text": "Doing well.", "artifacts": [{"name": "extra.txt"}]},
            elapsed_ms=1200,
            tool_names=set(),
            runtime_db_available=True,
        )


def test_live_stage_archive_transform_scenario_is_not_underscore_specific(tmp_path) -> None:
    scenario = live_stage_chat_contract_probe.SCENARIOS["archive_transform_workbook"]
    prompt = scenario.prompt.lower()

    assert "underscore" not in prompt
    assert "reversed stem" in prompt
    assert scenario.required_tool_all == ("archive_extract", "spreadsheet_create")
    assert scenario.required_embedded_media_extensions == (".xlsx",)
    assert scenario.min_embedded_media_count == 3

    archive_path = live_stage_chat_contract_probe._write_archive_transform_fixture(tmp_path)
    with live_stage_chat_contract_probe.zipfile.ZipFile(archive_path) as package:
        names = package.namelist()

    assert "observations/alpha-sun.png" in names
    assert "observations/beta-moonlight.png" in names
    assert "observations/gamma-river.png" in names
    assert not any("_" in name for name in names if name.endswith(".png"))


def test_planner_downloaded_data_workbook_scenario_requires_download_read_create_chain() -> None:
    scenario = live_stage_chat_contract_probe.SCENARIOS["planner_downloaded_data_workbook"]

    assert scenario.runner == "planner_downloaded_data_workbook"
    assert scenario.required_tool_all == ("file_download", "file_read", "spreadsheet_create")
    assert scenario.required_artifact_names == ("stage_planner_downloaded_data_workbook.xlsx",)
    assert ".xlsx" in scenario.required_artifact_extensions
    assert "Alabama" in scenario.required_artifact_text_all


def test_live_stage_default_matrix_covers_reported_failure_regressions() -> None:
    defaults = set(live_stage_chat_contract_probe.DEFAULT_LIVE_STAGE_SCENARIOS)

    assert {
        "active_followup_context_guard",
        "active_related_followup_context_guard",
        "calendar_tomorrow_live_access",
        "calendar_weekend_live_access",
        "email_simple_live_access",
        "scheduler_disable_not_delete",
        "scheduler_inventory_read_only",
        "scheduler_fresh_followup_after_bulk_enable",
        "browser_rental_live_records",
        "browser_flight_live_records",
        "market_quote_live_records",
        "browser_screenshot_artifact_delivery",
        "browser_page_screenshot_artifact_delivery",
        "artifact_twenty_row_xlsx_delivery",
        "random_website_artifact_bundle",
        "html_research_real_images",
    }.issubset(defaults)


def test_live_stage_default_matrix_covers_recent_artifact_and_scheduler_regressions() -> None:
    defaults = set(live_stage_chat_contract_probe.DEFAULT_LIVE_STAGE_SCENARIOS)

    assert {
        "requested_workspace_file_delivery",
        "scheduler_manual_run",
        "scheduler_manual_run_short_followup",
        "scheduler_fresh_followup_after_bulk_enable",
        "random_website_artifact_bundle",
        "browser_screenshot_artifact_delivery",
        "browser_page_screenshot_artifact_delivery",
        "mixed_embedded_media_artifact_package",
    }.issubset(defaults)


def test_live_stage_deep_matrix_covers_formatted_artifact_regression() -> None:
    deep = set(live_stage_chat_contract_probe.DEEP_LIVE_STAGE_SCENARIOS)

    assert "formatted_pdf_flyer_pack" in deep
    assert "spreadsheet_visual_workbook_pack" in deep
    assert "executive_personal_briefing_pack" in deep


def test_executive_personal_briefing_scenario_is_cross_domain_and_exactly_packaged() -> None:
    scenario = live_stage_chat_contract_probe.SCENARIOS["executive_personal_briefing_pack"]

    assert {
        "weather_forecast",
        "market_quote",
        "spreadsheet_create",
        "pdf_create",
        "file_write",
    }.issubset(scenario.required_tool_all)
    assert scenario.required_tool_or_structured_unavailable == ("calendar_list",)
    assert scenario.required_tool_any == ("browser_navigate", "web_fetch")
    assert scenario.required_artifact_names == (
        "executive_personal_briefing.xlsx",
        "executive_personal_briefing.pdf",
        "executive_personal_briefing.html",
    )
    assert scenario.min_artifact_count == 3
    assert scenario.max_artifact_count == 3
    assert scenario.require_xlsx_formula is True
    assert scenario.require_xlsx_chart is True
    assert scenario.require_xlsx_conditional_formatting is True
    assert scenario.required_xlsx_sheet_names == ("Briefing", "Markets", "Schedule")
    assert scenario.required_artifact_text_each == ("NULLION EXECUTIVE BRIEFING QA",)
    assert scenario.required_artifact_text_if_tool_unavailable == (
        ("calendar_list", "Calendar status: unavailable"),
    )
    assert "preserved_existing_artifact" in scenario.forbidden_tool_output_text


def test_live_stage_deep_matrix_covers_email_delivery_regressions() -> None:
    deep = set(live_stage_chat_contract_probe.DEEP_LIVE_STAGE_SCENARIOS)

    assert {
        "email_read_followup",
        "email_request_overrides_pr_context",
        "email_request_overrides_security_history",
        "complex_html_email_with_attachments",
        "research_artifact_email_followup_pack",
    }.issubset(deep)


def test_active_related_followup_scenario_requires_turn_dependency_and_html_artifact() -> None:
    scenario = live_stage_chat_contract_probe.SCENARIOS["active_related_followup_context_guard"]

    assert scenario.runner == "active_related_followup_context_guard"
    assert scenario.followups
    assert scenario.required_artifact_extensions == (".html",)
    assert scenario.required_artifact_names == ("stage_active_related_followup.html",)
    assert "Related follow-up revision" in scenario.required_artifact_text_all


def test_calendar_live_stage_scenarios_reject_weather_and_no_tool_answers() -> None:
    for name in ("calendar", "calendar_tomorrow_live_access", "calendar_weekend_live_access"):
        scenario = live_stage_chat_contract_probe.SCENARIOS[name]
        forbidden = " ".join(scenario.forbidden_text).lower()
        assert "weather in" in forbidden
        assert "don't have live" in forbidden
        assert "not exposed" in forbidden
        assert scenario.required_tool_any == ("calendar_list", "connector_request")
        assert "weather_forecast" in scenario.forbidden_tool_any
        assert scenario.required_tool_output_text_any == ("calendar", "event")


def test_browser_live_record_scenarios_reject_search_lead_dumps() -> None:
    for name in ("browser_rental_live_records", "browser_flight_live_records"):
        scenario = live_stage_chat_contract_probe.SCENARIOS[name]
        forbidden = " ".join(scenario.forbidden_text).lower()
        assert "search leads i found" in forbidden
        assert "vehicleSortBy".lower() in forbidden
        assert scenario.auto_approve is True
        assert "browser_navigate" in scenario.required_tool_any

    stock_scenario = live_stage_chat_contract_probe.SCENARIOS["market_quote_live_records"]
    stock_forbidden = " ".join(stock_scenario.forbidden_text).lower()
    assert "advertisement" in stock_forbidden
    assert "robinhood" in stock_forbidden
    assert stock_scenario.required_tool_any == ("market_quote",)


def test_email_simple_live_access_rejects_attachment_browser_and_file_fallbacks() -> None:
    scenario = live_stage_chat_contract_probe.SCENARIOS["email_simple_live_access"]

    assert "email_search" in scenario.required_tool_any
    assert "email_attachment_read" in scenario.forbidden_tool_any
    assert "browser_navigate" in scenario.forbidden_tool_any
    assert "web_fetch" in scenario.forbidden_tool_any
    assert "terminal_exec" in scenario.forbidden_tool_any
    assert "file_read" in scenario.forbidden_tool_any
    assert scenario.require_no_artifacts is True


def test_html_research_real_images_scenario_blocks_svg_side_artifacts() -> None:
    scenario = live_stage_chat_contract_probe.SCENARIOS["html_research_real_images"]

    assert scenario.required_artifact_extensions == (".html",)
    assert scenario.require_raster_html_images is True
    assert ".svg" in scenario.forbidden_artifact_extensions


def test_screenshot_live_scenarios_require_png_artifacts_not_result_dumps() -> None:
    for name in ("browser_screenshot_artifact_delivery", "browser_page_screenshot_artifact_delivery"):
        scenario = live_stage_chat_contract_probe.SCENARIOS[name]
        forbidden = " ".join(scenario.forbidden_text).lower()

        assert scenario.required_artifact_extensions == (".png",)
        assert scenario.require_image_artifact is True
        assert "results i found" in forbidden
        assert "top matches i found" in forbidden
        assert "browser_screenshot" in scenario.required_tool_any


def test_spreadsheet_visual_live_scenario_requires_embedded_media_and_formula_features() -> None:
    scenario = live_stage_chat_contract_probe.SCENARIOS["spreadsheet_visual_workbook_pack"]

    assert ".xlsx" in scenario.required_artifact_extensions
    assert ".xlsx" in scenario.required_embedded_media_extensions
    assert scenario.require_xlsx_formula is True
    assert scenario.require_xlsx_chart is True
    assert scenario.require_xlsx_conditional_formatting is True
    assert scenario.require_visual_assets is True
    assert ".json" in scenario.forbidden_artifact_extensions
    assert ".txt" in scenario.forbidden_artifact_extensions


def test_exact_file_and_mixed_package_live_scenarios_guard_recent_stage_failures() -> None:
    exact_file = live_stage_chat_contract_probe.SCENARIOS["requested_workspace_file_delivery"]
    mixed = live_stage_chat_contract_probe.SCENARIOS["mixed_embedded_media_artifact_package"]

    assert exact_file.runner == "requested_workspace_file_delivery"
    assert "spreadsheet_create" in exact_file.forbidden_tool_any
    assert exact_file.required_artifact_extensions == (".xlsx",)
    assert mixed.max_artifact_count == 5
    assert {".txt", ".json", ".png", ".jpg", ".jpeg"}.issubset(mixed.forbidden_artifact_extensions)


def test_reply_artifact_extension_mentions_flags_forbidden_side_deliverable() -> None:
    text = (
        "Done — attached the requested artifact package:\n"
        "- train_accessible_outdoor_day_trips.html\n"
        "- train_accessible_outdoor_day_trips.svg\n"
        "MEDIA:/tmp/train_accessible_outdoor_day_trips_filewrite.svg"
    )

    assert live_stage_chat_contract_probe._reply_artifact_extension_mentions(text, (".svg", ".json")) == [
        ".svg"
    ]


def test_auto_approve_retries_doctor_budget_mode(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_post_json(url, body, *, timeout):  # noqa: ANN001, ANN202
        del timeout
        calls.append((url, dict(body)))
        if body == {"mode": "once"}:
            raise AssertionError('HTTP 409 from /api/approve/id: {"error":"Choose 2x, 5x, or 10x"}')
        return {"ok": True, "resume": {"type": "message", "text": "continued"}}, 1.0

    monkeypatch.setattr(live_stage_chat_contract_probe, "_post_json", fake_post_json)

    response = live_stage_chat_contract_probe._approve_once("http://stage.test", "id", 5)

    assert response == {"type": "message", "text": "continued"}
    assert calls == [
        ("http://stage.test/api/approve/id", {"mode": "once"}),
        ("http://stage.test/api/approve/id", {"mode": "limit_2x"}),
    ]


def test_scheduler_disable_scenario_requires_toggle_without_file_search() -> None:
    scenario = live_stage_chat_contract_probe.SCENARIOS["scheduler_disable_not_delete"]

    assert scenario.runner == "scheduler_disable_not_delete"
    assert scenario.required_tool_all == ("list_crons",)
    assert scenario.required_tool_any == ("toggle_cron", "update_cron")
    assert scenario.forbidden_tool_any == ("file_search",)
    assert "deleted" in scenario.forbidden_text


def test_scheduler_manual_run_scenario_requires_real_run_tool() -> None:
    scenario = live_stage_chat_contract_probe.SCENARIOS["scheduler_manual_run"]

    assert scenario.runner == "scheduler_manual_run"
    assert scenario.required_tool_all == ("list_crons", "run_cron")
    assert scenario.forbidden_text


def test_random_website_and_formatted_pack_scenarios_reject_sidecars_and_partial_output() -> None:
    random_bundle = live_stage_chat_contract_probe.SCENARIOS["random_website_artifact_bundle"]
    formatted_pack = live_stage_chat_contract_probe.SCENARIOS["formatted_pdf_flyer_pack"]

    assert random_bundle.min_artifact_count >= 4
    assert ".json" in random_bundle.forbidden_artifact_extensions
    assert ".txt" in random_bundle.forbidden_artifact_extensions
    assert "only attached" in random_bundle.forbidden_text
    assert formatted_pack.min_artifact_count >= 3
    assert formatted_pack.required_artifact_extensions == (".pdf", ".html")
    assert formatted_pack.require_visual_assets is True


def test_live_stage_probe_requires_tool_output_evidence_when_requested() -> None:
    scenario = Scenario(
        name="calendar_evidence_guard",
        prompt="Check calendar",
        max_average_ms=1000,
        max_single_ms=1000,
        required_tool_any=("connector_request",),
        required_tool_output_text_any=("calendar",),
    )

    live_stage_chat_contract_probe._assert_contract(
        scenario,
        response={"text": "Calendar has one event."},
        elapsed_ms=100,
        tool_names={"connector_request"},
        runtime_db_available=True,
        tool_output_texts=['{"tool_name":"connector_request","output":{"kind":"calendar_list"}}'],
    )

    with pytest.raises(AssertionError, match="persisted tool evidence"):
        live_stage_chat_contract_probe._assert_contract(
            scenario,
            response={"text": "Calendar has one event."},
            elapsed_ms=100,
            tool_names={"connector_request"},
            runtime_db_available=True,
            tool_output_texts=['{"tool_name":"connector_request","output":{"kind":"generic_scope_request"}}'],
        )


def test_live_stage_probe_requires_completed_tools_and_accepts_structured_unavailable_source() -> None:
    scenario = Scenario(
        name="structured_source_completion_guard",
        prompt="opaque structured request",
        max_average_ms=1000,
        max_single_ms=1000,
        required_tool_all=("weather_forecast",),
        required_tool_or_structured_unavailable=("calendar_list",),
    )
    failed_weather = '{"tool_name":"weather_forecast","status":"failed","output":{}}'
    unavailable_calendar = (
        '{"tool_name":"request_tool_scope","status":"completed","output":'
        '{"unavailable_tools":["calendar_list"],"connector_source_unavailable":true,'
        '"missing_connector_app_scope":false,"source_selection_required":false}}'
    )

    with pytest.raises(AssertionError, match="expected completed results"):
        live_stage_chat_contract_probe._assert_contract(
            scenario,
            response={"text": "Draft"},
            elapsed_ms=100,
            tool_names={"weather_forecast", "request_tool_scope"},
            runtime_db_available=True,
            tool_output_texts=[failed_weather, unavailable_calendar],
        )

    live_stage_chat_contract_probe._assert_contract(
        scenario,
        response={"text": "Calendar was unavailable; weather completed."},
        elapsed_ms=100,
        tool_names={"weather_forecast", "request_tool_scope"},
        runtime_db_available=True,
        tool_output_texts=[
            '{"tool_name":"weather_forecast","status":"completed","output":{"temperature":72}}',
            unavailable_calendar,
        ],
    )


def test_live_stage_probe_does_not_accept_unavailable_source_from_failed_scope() -> None:
    scenario = Scenario(
        name="structured_source_unavailable_guard",
        prompt="opaque structured request",
        max_average_ms=1000,
        max_single_ms=1000,
        required_tool_or_structured_unavailable=("calendar_list",),
    )

    with pytest.raises(AssertionError, match="neither completed runtime evidence"):
        live_stage_chat_contract_probe._assert_contract(
            scenario,
            response={"text": "Unavailable"},
            elapsed_ms=100,
            tool_names={"request_tool_scope"},
            runtime_db_available=True,
            tool_output_texts=[
                '{"tool_name":"request_tool_scope","status":"failed","output":'
                '{"unavailable_required_tool_names":["calendar_list"]}}'
            ],
        )


def test_live_stage_probe_counts_xlsx_rows_for_delivery_contracts() -> None:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as package:
        package.writestr(
            "xl/worksheets/sheet1.xml",
            "<worksheet>"
            "<sheetData>"
            "<row r='1'><c r='A1'><v>header</v></c></row>"
            "<row r='2'><c r='A2'><v>one</v></c></row>"
            "<row r='3'><c r='A3'><v>two</v></c></row>"
            "</sheetData>"
            "</worksheet>",
        )

    assert live_stage_chat_contract_probe._xlsx_row_count(buffer.getvalue()) == 3
    scenario = live_stage_chat_contract_probe.SCENARIOS["artifact_twenty_row_xlsx_delivery"]
    assert scenario.min_xlsx_rows == 21
    assert scenario.required_artifact_text_all == ("QA note",)


def test_mixed_embedded_media_package_requires_exact_final_artifact_count() -> None:
    scenario = live_stage_chat_contract_probe.SCENARIOS["mixed_embedded_media_artifact_package"]

    assert scenario.min_artifact_count == 5
    assert scenario.max_artifact_count == 5
    assert scenario.required_artifact_extensions == (".docx", ".xlsx", ".csv", ".pdf", ".html")


def test_live_stage_probe_reads_latest_model_start_latency_mark(tmp_path) -> None:
    db_path = tmp_path / "runtime.db"
    conversation_id = "web:live-stage-contract:artifact_preflight_scope_fast_path:unit:0"
    now = datetime.now(UTC)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE conversation_events (collection TEXT, item_key TEXT, payload TEXT, updated_at TEXT)"
        )
        for index, model_start_ms in enumerate((4800.0, 700.0)):
            payload = {
                "conversation_id": conversation_id,
                "created_at": (now + timedelta(seconds=index)).isoformat(),
                "event_type": "conversation.latency_timing",
                "marks": [
                    {"name": "turn_worker_started", "ms": 0.0},
                    {"name": "model_start", "ms": model_start_ms},
                ],
            }
            conn.execute(
                "INSERT INTO conversation_events VALUES (?, ?, ?, ?)",
                (
                    "conversation_events",
                    f"latency-{index}",
                    json.dumps(payload),
                    (now + timedelta(seconds=index)).isoformat(),
                ),
            )

    assert live_stage_chat_contract_probe._latest_latency_mark_ms(
        db_path,
        conversation_id,
        "model_start",
    ) == 700.0


def test_live_stage_probe_reads_uncheckpointed_latency_mark_from_status(monkeypatch) -> None:
    conversation_id = "web:live-stage-contract:startup_warm_first_chat:unit:0"

    def fake_get_json(url: str, timeout: int):
        assert url == "http://127.0.0.1:8753/api/status?full=1"
        assert timeout == 11
        return {
            "latency_timings": [
                {
                    "conversation_id": conversation_id,
                    "marks": [
                        {"name": "ingress_received", "ms": 0.0},
                        {"name": "model_start", "ms": 104.5},
                    ],
                }
            ]
        }

    monkeypatch.setattr(live_stage_chat_contract_probe, "_get_json", fake_get_json)

    assert live_stage_chat_contract_probe._latest_status_latency_mark_ms(
        "http://127.0.0.1:8753",
        11,
        conversation_id,
        "model_start",
    ) == 104.5


@pytest.mark.parametrize("page_url,session", [("about:blank", "one"), ("https://example.com", "two")])
def test_live_stage_capture_requires_loaded_same_session(page_url, session):
    scenario = Scenario("capture", "", None, None, require_loaded_browser_capture=True)
    outputs = [json.dumps({"tool_name": "browser_navigate", "status": "completed", "output": {"session_id": "one"}}),
        json.dumps({"tool_name": "browser_screenshot", "status": "completed", "output": {"session_id": session, "page_url": page_url}})]
    with pytest.raises(AssertionError, match="loaded-page capture"):
        live_stage_chat_contract_probe._assert_contract(scenario, response={}, elapsed_ms=1,
            tool_names={"browser_navigate", "browser_screenshot"}, runtime_db_available=True, tool_output_texts=outputs)


def test_live_stage_capture_rejects_white_download(tmp_path):
    from PIL import Image
    path = tmp_path / "white.png"
    Image.new("RGB", (32, 32), "white").save(path)
    scenario = Scenario("capture", "", None, None, require_nonuniform_image=True)
    with pytest.raises(AssertionError, match="single color"):
        live_stage_chat_contract_probe._download_and_check_artifacts("http://localhost", [{"path": str(path), "name": path.name}], scenario=scenario, timeout=1)


def test_live_stage_capture_ignores_timing_event_without_receipt():
    scenario = Scenario("capture", "", None, None, require_loaded_browser_capture=True)
    outputs = [json.dumps({"tool_name": "browser_navigate", "status": "completed", "output": {"session_id": "one"}}),
        json.dumps({"tool_name": "browser_screenshot", "status": "completed", "output": {"session_id": "one", "page_url": "https://example.com/"}}),
        json.dumps({"tool_name": "browser_screenshot", "status": "completed", "event_type": "conversation.tool_timing", "duration_ms": 123})]
    live_stage_chat_contract_probe._assert_contract(scenario, response={}, elapsed_ms=1,
        tool_names={"browser_navigate", "browser_screenshot"}, runtime_db_available=True, tool_output_texts=outputs)
