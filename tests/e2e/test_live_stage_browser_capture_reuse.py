"""Live Web and runtime receipts for browser reuse and fresh navigation."""
import os
import re
from time import monotonic, sleep
from statistics import mean

import pytest

from tests.e2e.test_live_stage_web_matrix import _submit, catalogue, live_web

pytestmark = pytest.mark.skipif(
    os.environ.get("NULLION_LIVE_STAGE_E2E") != "1", reason="Explicit real stage acceptance")


def _assert_origin(live_web, followup, origin):
    _, _, _, database, cid, _ = live_web
    recalled = [r["output"] for r in followup["tool_results"] if r["tool_name"] == "chat_history_search"
                and r["output"].get("result_kind") == "retained_browser_snapshot"]
    deadline = monotonic() + 8
    while True:
        delivered = catalogue._conversation_event_payloads(
            database, cid, event_type="conversation.retained_browser_snapshot")
        records = [row for row in delivered if row["turn_id"] == followup["turn_id"]] + recalled
        if any(record.get("source_invocation_id") == origin for record in records):
            return
        assert monotonic() < deadline, (records, followup["tool_results"])
        sleep(0.25)


def test_recent_train_results_do_not_reopen_browser(live_web, record_property):
    simple = []
    for prompt, expected in [("Reply exactly ORANGE.", "ORANGE"), ("Responde solo AZUL.", "AZUL"),
                             ("Reply exactly GREEN.", "GREEN")]:
        response, turn, elapsed = _submit(live_web, prompt, timeout=30)
        assert expected in response["text"]
        assert not any(r["tool_name"].startswith("browser_") for r in turn["tool_results"])
        simple.append(elapsed)
    record_property("plain_average_ms", mean(simple))
    assert mean(simple) < 10000, simple

    response, turn, baseline = _submit(live_web,
        "On https://www.amtrak.com/home.html, use browser_run_task for the dynamic station and date controls to look up live trains and coach "
        "fares for one traveler today from Poughkeepsie to New York Penn. Capture at least the next three "
        "available departures and fares, but show me only the very next train for now. Do not book.",
        timeout=240)
    browser = [r for r in turn["tool_results"] if r["tool_name"] == "browser_run_task"
               and r["status"] == "completed" and r["output"].get("report_status") == "completed"]
    assert browser, response["text"]
    evidence = browser[-1]["output"]
    assert re.search(r"\$\s*\d", response["text"]) and turn.get("response_fulfilled") is True, response
    for key in ("model_calls", "input_tokens", "model_ms"):
        record_property(f"fresh_browser_{key}", evidence.get("performance", {}).get(key))
    record_property("fresh_ms", baseline)

    for index, prompt in enumerate(("What about other trains?", "¿Y los otros trenes? Usa los resultados que acabas de ver.")):
        reply, followup, elapsed = _submit(live_web, prompt, timeout=45)
        results = followup["tool_results"]
        if index == 0:
            _assert_origin(live_web, followup, browser[-1]["invocation_id"])
        assert not any(r["tool_name"].startswith(("browser_", "web_")) for r in results), results
        extra = []
        for row in evidence.get("observations", []):
            text = row.get("quote", "")
            fare = re.search(r"\$\s*(\d+)", text)
            departure = re.search(r"\b\d{1,2}:\d{2}\b", text)
            if fare and departure and departure.group() not in response["text"]:
                extra.append((fare.group(1), departure.group()))
        assert any(re.search(r"\$\s*" + fare + r"\b", reply["text"]) and departure in reply["text"]
                   for fare, departure in extra), (extra, reply)
        record_property("reuse_ms", elapsed)
        assert elapsed < min(30000, baseline), (elapsed, baseline)


def test_native_sequence_on_stage(live_web, record_property):
    response, turn, elapsed = _submit(live_web,
        "Use one code_mode sequence with exactly browser_navigate and browser_extract_text. Navigate to "
        "https://en.wikipedia.org/wiki/Ada_Lovelace, pass the returned session_id to browser_extract_text "
        "with selector and ready_selector both set to h1, and report the captured heading and URL.", timeout=90)
    tools = turn["tool_results"]
    assert "Ada Lovelace" in response["text"] and "wikipedia.org" in response["text"], response
    assert any(t["tool_name"] == "code_mode" and t["status"] == "completed" for t in tools), tools
    assert not any(t["tool_name"] == "browser_run_task" for t in tools), tools
    names = {t["tool_name"] for t in tools if t["status"] == "completed"}
    assert {"browser_navigate", "browser_extract_text"} <= names, tools
    record_property("native_sequence_ms", elapsed)


def test_adaptive_capture_followup_on_stage(live_web, record_property):
    response, turn, baseline = _submit(live_web,
        "For this browser-worker check, explicitly use browser_run_task as an adaptive interaction on "
        "https://www.wikipedia.org/. Inspect the visible language selector and capture the visible English "
        "and Japanese language link labels and their article counts. For now, show me only the English count.",
        timeout=120)
    browser = [t for t in turn["tool_results"] if t["tool_name"] == "browser_run_task"
               and t["status"] == "completed" and t["output"].get("report_status") == "completed"]
    assert browser and re.search(r"\d[\d, ]{5,}", response["text"]), (response, turn["tool_results"])
    perf = browser[-1]["output"].get("performance", {})
    record_property("adaptive_model_calls", perf.get("model_calls"))
    record_property("adaptive_input_tokens", perf.get("input_tokens"))
    japanese_counts = {
        re.sub(r"\D", "", number)
        for row in browser[-1]["output"].get("observations", [])
        if any(tag in str(row).casefold() for tag in ("日本語", "japanese"))
        for number in re.findall(r"\d[\d, ]{5,}", row.get("quote", ""))
    }
    assert japanese_counts, browser[-1]["output"].get("observations")
    reply, followup, elapsed = _submit(live_web, "¿Y el japonés? Usa la captura anterior.", timeout=45)
    _assert_origin(live_web, followup, browser[-1]["invocation_id"])
    assert not any(t["tool_name"].startswith(("browser_", "web_")) for t in followup["tool_results"])
    match = re.search(r"\d[\d, ]{5,}", reply["text"])
    assert match and re.sub(r"\D", "", match.group()) in japanese_counts, reply
    record_property("reuse_ms", elapsed)
    assert elapsed < min(30000, baseline), (elapsed, baseline)
