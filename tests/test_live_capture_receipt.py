"""A screenshot on disk cannot replace a missing API delivery receipt."""
import pytest
from scripts import live_stage_chat_contract_probe as probe

@pytest.mark.parametrize("name", [
    "browser_screenshot_artifact_delivery",
    "browser_context_capture_recovery",
    "browser_context_capture_recovery_variant",
])
def test_capture_probe_checks_fixture_before_contacting_stage(name, monkeypatch, capsys):
    monkeypatch.delenv("NULLION_LIVE_CAPTURE_SOURCE", raising=False)
    monkeypatch.setattr(probe, "_get_json", lambda *args, **kwargs: pytest.fail("Stage contacted without a fixture"))
    assert probe.main(["--scenario", name]) == 2
    assert "NULLION_LIVE_CAPTURE_SOURCE" in capsys.readouterr().out


@pytest.mark.parametrize("flags", [[], ["--fail-fast"]])
def test_probe_records_preflight_failure_without_url_details(monkeypatch, capsys, flags):
    import json

    def unavailable(*args, **kwargs):
        raise ConnectionError("private local endpoint")

    monkeypatch.setattr(probe, "_get_json", unavailable)
    monkeypatch.setattr(probe, "_post_json", lambda *args, **kwargs: pytest.fail("Paid request after failed preflight"))
    assert probe.main(["--scenario", "simple", *flags]) == 1
    lines = capsys.readouterr().out.splitlines()
    first = json.loads(lines[0])
    assert (first["event"], first["scenario"], first["exception_type"]) == (
        "scenario_failure", "stage_preflight", "ConnectionError")
    assert "private local endpoint" not in "\n".join(lines)


def test_capture_probe_requires_advertised_api_artifact(monkeypatch):
    monkeypatch.setenv('NULLION_LIVE_CAPTURE_SOURCE','http://fixture.invalid/cart/')
    monkeypatch.setattr(probe,'_post_json',lambda *args,**kw:({'text':'Page opened; image prepared.','artifacts':[]},1))
    monkeypatch.setattr(probe,'_event_tool_names_with_poll',lambda *args,**kw:{'browser_screenshot'})
    monkeypatch.setattr(probe,'_event_tool_output_texts_with_poll',lambda *args,**kw:[])
    monkeypatch.setattr(probe,'_wait_for_conversation_media_artifact',lambda *args,**kw:pytest.fail('Disk-only artifact is not delivery'))
    with pytest.raises(AssertionError,match='expected image artifact'):
        probe._run_browser_screenshot_artifact_delivery(base_url='http://127.0.0.1:8753',runtime_db=None,
            scenario=probe.SCENARIOS['browser_screenshot_artifact_delivery'],timeout=1,index=0,runtime_db_available=False)


def test_capture_probe_rejects_delivery_of_an_unrelated_captured_page(monkeypatch):
    import json

    cart = "http://fixture.invalid/cart/"
    other = "https://example.org/"
    monkeypatch.setenv("NULLION_LIVE_CAPTURE_SOURCE", cart)
    records = [
        {"tool_name": "browser_navigate", "status": "completed", "output": {"session_id": "one"}},
        *({"tool_name": "browser_screenshot", "status": "completed", "output": {
            "session_id": "one", "page_url": url, "path": path,
        }} for url, path in [(cart, "/cart.png"), (other, "/other.png")]),
    ]
    monkeypatch.setattr(probe, "_post_json", lambda *args, **kwargs: (
        {"artifacts": [{"name": "other.png", "path": "/other.png"}]}, 1))
    monkeypatch.setattr(probe, "_event_tool_names_with_poll", lambda *args, **kwargs: {"browser_screenshot"})
    monkeypatch.setattr(probe, "_event_tool_output_texts_with_poll", lambda *args, **kwargs: [json.dumps(records)])
    monkeypatch.setattr(probe, "_download_and_check_artifacts", lambda *args, **kwargs: pytest.fail("Downloaded unrelated image"))
    with pytest.raises(AssertionError, match="delivered screenshot did not show the owned fixture URL"):
        probe._run_browser_screenshot_artifact_delivery(base_url="http://127.0.0.1:8753", runtime_db=None,
            scenario=probe.SCENARIOS["browser_screenshot_artifact_delivery"],
            timeout=1, index=0, runtime_db_available=True)
