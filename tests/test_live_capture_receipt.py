"""A screenshot on disk cannot replace a missing API delivery receipt."""
import pytest
from scripts import live_stage_chat_contract_probe as probe

def test_capture_probe_requires_advertised_api_artifact(monkeypatch):
    monkeypatch.setenv('NULLION_LIVE_CAPTURE_SOURCE','http://fixture.invalid/cart/')
    monkeypatch.setattr(probe,'_post_json',lambda *args,**kw:({'text':'Page opened; image prepared.','artifacts':[]},1))
    monkeypatch.setattr(probe,'_event_tool_names_with_poll',lambda *args,**kw:{'browser_screenshot'})
    monkeypatch.setattr(probe,'_event_tool_output_texts_with_poll',lambda *args,**kw:[])
    monkeypatch.setattr(probe,'_wait_for_conversation_media_artifact',lambda *args,**kw:pytest.fail('Disk-only artifact is not delivery'))
    with pytest.raises(AssertionError,match='expected image artifact'):
        probe._run_browser_screenshot_artifact_delivery(base_url='http://127.0.0.1:8753',runtime_db=None,
            scenario=probe.SCENARIOS['browser_screenshot_artifact_delivery'],timeout=1,index=0,runtime_db_available=False)
