from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from nullion import crons
from nullion.approval_display import approval_display_from_request
from nullion.approvals import create_approval_request
from nullion.cron_delivery import _recover_typed_html_artifact_after_terminal_limit
from nullion.scheduler_deletion_contract import scheduler_deletion_guard_result
from nullion.tools import ToolInvocation, ToolResult


class JsonClient:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return {"content": [{"type": "text", "text": json.dumps(self.payload)}]}


def job_with_report():
    return SimpleNamespace(
        name="株価", task="上昇は緑、下落は赤。価格と短いニュースのみ。", id="report-1",
        artifact_delivery_options={"required_artifact_extensions": [".html"],
                                   "required_artifact_content_tokens": ["AAA", "BBB"]},
    )


def timed_out_result():
    return {"model_timed_out": True, "artifact_delivery_satisfied": False, "artifacts": [],
            "tool_results": [ToolResult("quotes", "market_quote", "completed", {"quotes": [
                {"symbol": "AAA", "price": 20, "change_percent": 2},
                {"symbol": "BBB", "price": 10, "change_percent": -2},
            ]})]}


@pytest.mark.parametrize("message", ["The cron still produced the same html", "同じHTMLが出ました", "El informe sigue igual"])
def test_output_complaint_cannot_authorize_deletion(monkeypatch, message):
    monkeypatch.setattr(crons, "get_cron", lambda _: SimpleNamespace(name="Market report", workspace_id="workspace_admin"))
    client = JsonClient({"authorized": False, "target_id": "cron-1", "user_evidence": message})
    result = scheduler_deletion_guard_result(
        ToolInvocation("delete", "delete_cron", "telegram:123", {"id": "cron-1"}),
        user_message=message, model_client=client,
    )
    assert result.output["reason"] == "scheduler_delete_not_user_authorized"
    assert not result.output.get("requires_approval")


@pytest.mark.parametrize("decision", [
    {"authorized": "true", "target_id": "cron-1", "user_evidence": "削除"},
    {"authorized": True, "target_id": "other", "user_evidence": "削除"},
    {"authorized": True, "target_id": "cron-1", "user_evidence": "invented deletion request"},
])
def test_delete_decision_requires_typed_target_and_user_evidence(monkeypatch, decision):
    monkeypatch.setattr(crons, "get_cron", lambda _: SimpleNamespace(name="Market report", workspace_id="workspace_admin"))
    assert scheduler_deletion_guard_result(
        ToolInvocation("delete", "delete_cron", "telegram:123", {"id": "cron-1"}),
        user_message="削除", model_client=JsonClient(decision),
    ) is not None


def test_exact_non_english_delete_request_is_allowed(monkeypatch):
    monkeypatch.setattr(crons, "get_cron", lambda _: SimpleNamespace(name="Market report", workspace_id="workspace_admin"))
    assert scheduler_deletion_guard_result(
        ToolInvocation("delete", "delete_cron", "telegram:123", {"id": "cron-1"}),
        user_message="Market reportを削除", model_client=JsonClient({
            "authorized": True, "target_id": "cron-1", "user_evidence": "Market reportを削除",
        }),
    ) is None


def test_plain_tools_do_not_call_delete_classifier():
    client = JsonClient({})
    assert scheduler_deletion_guard_result(
        ToolInvocation("read", "market_quote", "telegram:123", {}),
        user_message="こんにちは", model_client=client,
    ) is None
    assert client.calls == []


def test_saved_report_instructions_reach_recovery_and_written_bytes(monkeypatch, tmp_path):
    monkeypatch.setattr("nullion.artifacts.artifact_root_for_principal", lambda _: str(tmp_path))
    report = '<!doctype html><html><body><article style="color:green">AAA 20 +2%</article><article style="color:red">BBB 10 -2%</article></body></html>'
    client = JsonClient({"requirements_satisfied": True, "html": report})
    result = timed_out_result()
    assert _recover_typed_html_artifact_after_terminal_limit(job_with_report(), "telegram:123", result, model_client=client)
    payload = json.loads(client.calls[0]["messages"][0]["content"][0]["text"])
    assert payload["instructions"] == job_with_report().task
    assert len(payload["verified_source_records"]) == 2
    from pathlib import Path
    assert Path(result["artifacts"][0]).read_text() == report
    assert result["artifact_delivery_satisfied"] is True


@pytest.mark.parametrize("payload", [
    {"requirements_satisfied": False, "html": ""},
    {"requirements_satisfied": "true", "html": ""},
    {"requirements_satisfied": True, "html": '<!doctype html><html><body>AAA BBB<script>alert(1)</script></body></html>'},
    {"requirements_satisfied": True, "html": '<!doctype html><html><body>AAA</body></html>'},
])
def test_recovery_never_claims_success_for_unmet_contract(monkeypatch, tmp_path, payload):
    monkeypatch.setattr("nullion.artifacts.artifact_root_for_principal", lambda _: str(tmp_path))
    result = timed_out_result()
    assert not _recover_typed_html_artifact_after_terminal_limit(job_with_report(), "telegram:123", result, model_client=JsonClient(payload))
    assert result["model_timed_out"] is True
    assert result["artifact_delivery_satisfied"] is False
    assert not result["artifacts"]


def test_delete_approval_names_target_and_consequence():
    approval = create_approval_request(requested_by="telegram:123", action="use_tool", resource="delete_cron", context={
        "tool_name": "delete_cron", "tool_arguments": {"id": "cron-1"},
        "scheduler_target": {"id": "cron-1", "name": "Daily Pre-Market Stock Research"},
    })
    display = approval_display_from_request(approval)
    assert display.title == "⚠️ Delete this scheduled task?"
    assert "Daily Pre-Market Stock Research" in display.detail
    assert "cron-1" in display.detail
    assert "stops all future scheduled runs" in display.detail


def test_existing_artifact_delivery_does_not_require_model_design_review(monkeypatch, tmp_path):
    monkeypatch.setattr("nullion.artifacts.artifact_root_for_principal", lambda _: str(tmp_path))
    path = tmp_path / "uniform.html"
    path.write_text('<!doctype html><html><body><article>AAA 20</article><article>BBB 10</article></body></html>')
    result = timed_out_result()
    result["artifact_delivery_satisfied"] = True
    result["artifacts"] = [str(path)]
    result["tool_results"].append(ToolResult("write", "file_write", "completed", {"artifact_path": str(path)}))
    client = JsonClient({"requirements_satisfied": False, "html": ""})
    assert _recover_typed_html_artifact_after_terminal_limit(
        job_with_report(), "telegram:123", result, model_client=client)
    assert result["artifact_delivery_satisfied"] is True
    assert result["artifacts"] == [str(path)]
    assert client.calls == []


def test_generation_is_deliverable_without_second_model_review(monkeypatch, tmp_path):
    monkeypatch.setattr("nullion.artifacts.artifact_root_for_principal", lambda _: str(tmp_path))
    class Client:
        def create(self, **kwargs):
            payload = json.loads(kwargs["messages"][0]["content"][0]["text"])
            decision = {"requirements_satisfied": payload["existing_html"] is None,
                        "html": '<!doctype html><html><body>AAA BBB</body></html>'}
            return {"content": [{"type": "text", "text": json.dumps(decision)}]}
    result = timed_out_result()
    assert _recover_typed_html_artifact_after_terminal_limit(job_with_report(), "telegram:123", result, model_client=Client())
    assert result["artifact_delivery_satisfied"] is True
    assert len(result["cron_artifact_review_attempts"]) == 1


def test_completed_html_bundle_keeps_files_and_caption_without_review(tmp_path):
    paths = [tmp_path / 'first.html', tmp_path / 'second.html']
    documents = ['<!doctype html><html><body>AAA 20</body></html>',
                 '<!doctype html><html><body>BBB 10</body></html>']
    for path, text in zip(paths, documents):
        path.write_text(text)
    job = job_with_report()
    job.artifact_delivery_options['required_artifact_content_tokens'] = []
    result = timed_out_result()
    result.update(model_timed_out=False, artifact_delivery_satisfied=True,
                  artifacts=[str(path) for path in paths], text='Saved caption')
    result['tool_results'].append(ToolResult('write', 'file_write', 'completed', {'artifact_paths': result['artifacts']}))
    client = JsonClient({'requirements_satisfied': True, 'html': ''})
    assert _recover_typed_html_artifact_after_terminal_limit(job, 'telegram:123', result, model_client=client)
    assert result['cron_artifact_file_accepted'] is True
    assert result['artifacts'] == [str(path) for path in paths]
    assert result['text'] == 'Saved caption'
    assert client.calls == []


def test_html_review_preserves_generic_nested_metrics_and_required_work_policy():
    from nullion.cron_delivery import _instruction_aware_cron_html
    seen=[]
    class Model:
        def create(self,**kw):
            payload=json.loads(kw['messages'][0]['content'][0]['text']);seen.append(payload)
            data=json.dumps(payload['verified_tool_evidence'])
            assert '37.5' in data and '2030-11-04T19:30:00Z' in data and 'sensor' in data
            assert 'Missing required facts' in kw['system']
            assert 'unless the saved instructions explicitly forbid partial' not in kw['system']
            return {'content':[{'type':'text','text':json.dumps({'requirements_satisfied':True,'html':'','issues':[]})}]}
    job=SimpleNamespace(name='検証',task='Report the reading, unit and time.',artifact_delivery_options={})
    result={'tool_results':[{'tool_name':'lookup_sensor','status':'completed','output':{
        'sensor':{'reading':37.5,'unit':'C','observed_at':'2030-11-04T19:30:00Z'}}}]}
    document='<!doctype html><html><body>37.5 C, 2030-11-04T19:30:00Z</body></html>'
    assert _instruction_aware_cron_html(job,result,model_client=Model(),existing_html=document)==document
    assert len(seen)==1
