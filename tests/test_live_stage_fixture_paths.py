"""Reject unusable container fixture setup before spending provider quota."""
from pathlib import Path

import pytest
from scripts.live_stage_chat_contract_probe import _stage_workspace_upload_root


@pytest.mark.parametrize('problem', ['missing', 'relative', 'mismatched'])
def test_archive_probe_rejects_nonshared_fixture_root_before_provider_call(monkeypatch, tmp_path, problem):
    monkeypatch.setenv('NULLION_E2E_CONTAINER', '1')
    monkeypatch.setenv('NULLION_LIVE_STAGE_ARTIFACT_ROOT', str(tmp_path/'owned'))
    if problem=='missing': monkeypatch.delenv('NULLION_LIVE_STAGE_UPLOAD_ROOT', raising=False)
    else: monkeypatch.setenv('NULLION_LIVE_STAGE_UPLOAD_ROOT', 'relative' if problem=='relative' else str(tmp_path/'unshared'))
    with pytest.raises(AssertionError): _stage_workspace_upload_root()


def test_archive_and_planner_use_same_shared_absolute_fixture_root(monkeypatch, tmp_path):
    monkeypatch.setenv('NULLION_E2E_CONTAINER', '1')
    for key in ['NULLION_LIVE_STAGE_ARTIFACT_ROOT', 'NULLION_LIVE_STAGE_UPLOAD_ROOT']:
        monkeypatch.setenv(key, str(tmp_path/'owned'))
    assert _stage_workspace_upload_root() == tmp_path/'owned'


def test_native_probe_keeps_explicit_upload_root(monkeypatch, tmp_path):
    monkeypatch.delenv('NULLION_E2E_CONTAINER', raising=False)
    monkeypatch.setenv('NULLION_LIVE_STAGE_UPLOAD_ROOT', str(tmp_path))
    assert _stage_workspace_upload_root() == tmp_path


@pytest.mark.parametrize('value',['[8742]','[8752]','[8753]','[true]','["43210"]','{}','[0]'])
def test_live_forwarding_rejects_other_lanes_and_invalid_port_metadata(value):
    from scripts.live_stage_tcp_forwarder import live_forward_ports
    with pytest.raises(ValueError):live_forward_ports(value)


def test_live_forwarding_keeps_owned_fixture_ports_without_duplicate_listeners():
    from scripts.live_stage_tcp_forwarder import live_forward_ports
    assert live_forward_ports('[43210,43211,43210]')==[8753,43210,43211]
