import json
import pytest

from scripts.live_stage_provider_guard import terminal_provider_event


@pytest.mark.parametrize('message', ['上限に達しました', 'Límite; capacidad agotada', 'وصل الحد'])
def test_guard_uses_provider_schema_and_never_returns_private_error_text(message):
    error = json.dumps({'error': {'type': 'usage_limit_reached', 'message': message, 'private': 'SECRET'}})
    record = {'event_type': 'doctor.signal_received', 'payload': {'reason':
        'source=telegram_chat;detail=Agent orchestrator error: HTTP 429: ' + error + ';message_text=SECRET'}}
    result = terminal_provider_event(record)
    assert result == {'category': 'quota', 'terminal': True, 'http_status': 429, 'error_type': 'usage_limit_reached'}
    assert 'SECRET' not in json.dumps(result)


@pytest.mark.parametrize('diagnostic', ['HTTP 429: {"error":{"type":"rate_limit_exceeded"}}',
    'HTTP 503: {"error":{"type":"server_error"}}', 'HTTP 429: not json', 'Connection unavailable'])
def test_transient_or_unstructured_failures_are_not_terminal_quota(diagnostic):
    record = {'event_type': 'doctor.signal_received', 'payload': {'reason':
        'detail=' + diagnostic + ';message_text=HTTP 429: {"error":{"type":"usage_limit_reached"}}'}}
    assert terminal_provider_event(record) is None


def test_terminal_auth_code_is_preserved_alongside_generic_provider_error_type():
    record={'event_type':'doctor.signal_received','payload':{'reason':
        'detail=HTTP 401: {"error":{"type":"authentication_error","code":"token_invalidated","message":"認証できません"}}'}}
    result=terminal_provider_event(record)
    assert result['category']=='auth' and result['provider_code']=='token_invalidated'


def test_guard_stops_catalogue_after_first_terminal_failure_without_model_probe(tmp_path, monkeypatch):
    from scripts import live_stage_chat_contract_probe as probe
    from scripts.live_stage_provider_guard import LiveStageProviderGuard
    reads = []
    paid = []
    def check(self):
        reads.append(True)
        return 'terminal provider quota failure' if paid else None
    monkeypatch.setattr(LiveStageProviderGuard, 'check', check)
    monkeypatch.setattr(probe, '_get_json', lambda *a, **kw: {})
    monkeypatch.setattr(probe, 'SCENARIOS', {name: probe.Scenario(name, name, None, None) for name in ('first', 'second')})
    def post(*a, **kw):
        paid.append(True)
        raise RuntimeError('Synthetic provider response')
    monkeypatch.setattr(probe, '_post_json', post)
    assert probe.main(['--runtime-db', str(tmp_path/'absent.db'), '--scenario', 'first', '--scenario', 'second']) == 1
    assert len(paid) == 1 and len(reads) == 2


def test_dynamically_loaded_ui_catalogue_can_load_shared_guard():
    import importlib.util
    from pathlib import Path
    import sys
    path=Path(__file__).parents[1]/'scripts/live_stage_chat_contract_probe.py'
    spec=importlib.util.spec_from_file_location('guard_catalogue_fixture',path)
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module
    try:
        spec.loader.exec_module(module)
        with pytest.raises(SystemExit) as result:module.main(['--help'])
        assert result.value.code==0
    finally:
        sys.modules.pop(spec.name,None)


@pytest.mark.parametrize("failed", [False, True, "quota"])
@pytest.mark.parametrize("consume", ["poll", "finish_pending"])
def test_inflight_evidence_poll_does_not_block_browser_and_preserves_failure(monkeypatch, failed, consume):
    import threading
    import time
    from urllib.error import URLError
    from scripts.live_stage_provider_guard import LiveStageProviderGuard
    started = threading.Event()
    release = threading.Event()
    calls = []
    def read_json(request, **kwargs):
        calls.append((request.get_method(), kwargs["retry_seconds"]))
        started.set()
        assert release.wait(2)
        if failed is True:
            raise URLError(OSError(101, "Network is unreachable"))
        return {"events": [{"details": {"category": "quota", "http_status": 429}}]
                if failed == "quota" else []}
    import json
    import tempfile
    from pathlib import Path
    with tempfile.TemporaryDirectory() as directory:
        config=Path(directory)/"connection.json"
        config.write_text(json.dumps({"port":1,"token":"fixture-only"}))
        monkeypatch.setenv("NULLION_LIVE_NATIVE_EVIDENCE_CONNECTION", str(config))
        monkeypatch.setenv("NULLION_LIVE_EVIDENCE_DIR", directory)
        monkeypatch.setattr("scripts.live_stage_http.read_json", read_json)
        guard=LiveStageProviderGuard()
        before=time.monotonic()
        assert guard.poll() is None
        assert started.wait(1)
        assert guard.check_pending
        for _ in range(20):assert guard.poll() is None
        assert time.monotonic()-before < .2
        assert calls == [("GET",30)]  # Single pending read, never a paid request.
        release.set()
        deadline=time.monotonic()+1
        while not guard._pending_check.done() and time.monotonic()<deadline:time.sleep(.001)
        completed = getattr(guard, consume)
        if failed is True:
            with pytest.raises(URLError):completed()
        elif failed == "quota":
            assert "quota failure (HTTP 429)" in completed()
            assert json.loads((Path(directory)/"provider-blocked.json").read_text())["details"]["category"] == "quota"
        else:assert completed() is None
        assert not guard.check_pending
