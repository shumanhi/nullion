from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from nullion.cron_delivery import (
    _cron_final_repair_tool_evidence,
    _cron_report_failure_notice,
    _repair_cron_agent_final_text_if_needed,
)
from nullion.tools import ToolResult


def result(name, output, status="completed"):
    return ToolResult(name + "-call", name, status, output)


def test_repair_keeps_late_source_after_large_discovery_sequence():
    records = [result("file_read", {"path": f"/state/{i}.json", "content": "saved checkpoint " * 300})
               for i in range(25)]
    records.insert(0, result("skill_pack_read", {"text": "INTERNAL_DOCUMENTATION " * 100}))
    records.insert(1, result("file_search", {"matches": ["/private/inventory.json"] * 100}))
    records.append(result("browser_navigate", {"result": {"title": "Internal navigation receipt"}}))
    records.append(result("browser_extract_text", {"text": "SOURCE_EVENT: 今週のイベント、価格 25 USD。"}))
    evidence = _cron_final_repair_tool_evidence(records)
    rows = json.loads(evidence)
    assert "SOURCE_EVENT" in evidence
    assert rows[0]["tool_name"] == "browser_extract_text"
    assert not {"skill_pack_read", "file_search", "browser_navigate"} & {r.get("tool_name") for r in rows}
    assert "INTERNAL_DOCUMENTATION" not in evidence and "/private/inventory" not in evidence
    assert len(evidence) < 9300


def test_internal_docs_and_navigation_do_not_prove_an_empty_source():
    assert _cron_final_repair_tool_evidence([
        result("skill_pack_read", {"text": "No new events found. " * 20}),
        result("request_tool_scope", {"available_tools": ["browser_extract_text"]}),
        result("file_search", {"matches": []}),
        result("browser_navigate", {"result": {"title": "Events"}}),
    ]) == ""


@pytest.mark.parametrize("task", ["Report new matched events only.", "新しい該当イベントだけ報告。", "Informa solo eventos nuevos coincidentes."])
def test_repair_retry_receives_validation_feedback_and_same_source(task):
    source = result("web_fetch", {"title": "Current listings", "text": "No new matching event; other current listings present."})
    state = {"text": "internal reference " * 15,
             "tool_results": [result("skill_pack_read", {"text": "internal reference " * 15}), source]}
    prompts = []

    class Model:
        def create(self, **kwargs):
            prompt = kwargs["messages"][0]["content"][0]["text"]
            prompts.append(prompt)
            if len(prompts) == 1:
                return {"content": [{"type": "text", "text": "Malformed report"}]}
            assert "invalid_report_schema" in prompt
            assert "source_state=available" in prompt
            return {"content": [{"type": "text", "text": json.dumps({
                "requirements_satisfied": True, "source_state": "available", "evidence_refs": ["e0"],
                "report": "該当する新しいイベントはありません。"})}]}

    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name="Monitor", task=task), state, model_client=Model())
    assert state["response_fulfilled"] is True
    assert state["cron_agent_final_repaired"] is True
    assert len(prompts) == 2
    for prompt in prompts:
        evidence = prompt.split("Verified compact tool evidence from this run:\n", 1)[1].split("\n\nWrite", 1)[0]
        assert "skill_pack_read" not in evidence and "Current listings" in evidence


def test_internal_only_repair_does_not_call_model_or_claim_success():
    state = {"text": "internal reference " * 15,
             "tool_results": [result("skill_pack_read", {"text": "internal reference " * 15})]}
    class Model:
        def create(self, **kwargs):
            raise AssertionError("No source evidence to repair")
    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name="Monitor", task="報告"), state, model_client=Model())
    assert state["cron_agent_final_repair_failure"] == "tool_evidence_unavailable"
    assert not state.get("cron_agent_final_repaired")


@pytest.mark.parametrize("task", [
    "Report only new matched events; stay silent when none are new.",
    "新しい該当イベントだけ報告し、なければ何も送信しない。",
    "Informa solo eventos nuevos; no envíes nada si no hay novedades.",
])
def test_legacy_monitor_repair_can_obey_saved_silence(task):
    source = result("web_fetch", {"text": "Current listings include only previously reported items."})
    state = {"text": "internal reference " * 15,
             "artifacts": ["/private/checkpoint.json"],
             "tool_results": [result("skill_pack_read", {"text": "internal reference " * 15}), source]}
    class Model:
        def create(self, **kwargs):
            assert task in kwargs["messages"][0]["content"][0]["text"]
            return {"content": [{"type": "text", "text": json.dumps({
                "requirements_satisfied": True, "source_state": "available", "evidence_refs": ["e0"],
                "delivery_decision": "silent", "delivery_reason": "no_verified_match", "report": ""})}]}
    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name="Monitor", task=task, artifact_delivery_options={}),
                                           state, model_client=Model())
    assert state["response_fulfilled"] is True and state["cron_agent_final_repaired"] is True
    assert state["text"] == state["final_text"] == ""
    assert state["cron_alert_delivery_decision"] == "silent"
    assert state["artifacts"] == []


@pytest.mark.parametrize("change", [
    {"requirements_satisfied": False}, {"source_state": "failed"},
    {"evidence_refs": ["unknown"]}, {"delivery_reason": "insufficient_evidence"},
])
def test_silence_cannot_hide_failed_or_unsupported_repair(change):
    state = {"text": "internal reference " * 15,
             "tool_results": [result("skill_pack_read", {"text": "internal reference " * 15}),
                              result("web_fetch", {"text": "Current source listings"})]}
    decision = {"requirements_satisfied": True, "source_state": "available", "evidence_refs": ["e0"],
                "delivery_decision": "silent", "delivery_reason": "no_verified_match", "report": "", **change}
    class Model:
        def create(self, **kwargs):
            return {"content": [{"type": "text", "text": json.dumps(decision)}]}
    events = []
    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name="Monitor", task="Monitor"), state, model_client=Model(),
        record_preflight=lambda stage, **fields: events.append((stage, fields)))
    assert state["cron_agent_final_repair_failed"] is True
    assert state["response_fulfilled"] is False and not state.get("cron_alert_delivery_decision")
    rejected = [fields for stage, fields in events if stage == 'final_repair_rejected']
    assert len(rejected) == 2
    expected = {'delivery_reason': 'silent_reason'}.get(next(iter(change)), next(iter(change)))
    assert all(expected in fields['validation_failures'] for fields in rejected)
    assert all(fields['completed_evidence_count'] == 1 for fields in rejected)
    assert 'Current source listings' not in json.dumps(rejected)


@pytest.mark.parametrize("state,expected", [
    ({"tool_results": [result("browser_extract_text", {"reason": "handler_timeout", "message": "/private/secret"}, "failed")]}, "browser step timed out"),
    ({"cron_agent_final_repair_failure": "model_call_failed"}, "model request failed"),
    ({"cron_agent_final_repair_failure": "report_not_grounded_or_complete"}, "source checks did not produce a complete report"),
    ({"cron_agent_final_repair_failure": "report_not_grounded_or_complete", "tool_results": [result("browser_extract_text", {}, "failed")]}, "source checks did not produce a complete report"),
])
def test_failed_report_notice_has_safe_cause(state, expected):
    text = _cron_report_failure_notice(state)
    assert expected in text
    assert "/private/secret" not in text and "still open" not in text


@pytest.mark.parametrize('source_text', [
    'Current listings: the matching event was already reported; no new matching ticket release.',
    '現在の一覧：該当イベントは報告済みで、新しいチケット発売はありません。',
    'Listados actuales: el evento coincidente ya se informó; no hay nuevos lanzamientos.',
])
def test_repair_keeps_hydrated_sources_and_dedupe_after_many_search_previews(source_text):
    previews = [result('web_search', {'query': f'listing {i}', 'results': [
        {'title': f'Preview {i}-{j}', 'snippet': 'Repeated discovery context ' * 60,
         'url': f'https://example.test/{i}/{j}'} for j in range(6)]}) for i in range(20)]
    checkpoint = result('file_read', {'path': '/state/reported.json',
        'content': '{"reported_events":[{"id":"already-reported","date":"2026-10-31"}]}'})
    records = previews + [
        result('browser_extract_text', {'text': source_text, 'session_id': 'public-listings'}),
        result('browser_extract_text', {'text': 'Other current source: no new matching release.'}), checkpoint]
    evidence = _cron_final_repair_tool_evidence(records)
    rows = json.loads(evidence)
    assert source_text in evidence
    assert 'Other current source' in evidence
    assert 'already-reported' in evidence
    hydrated = [row['tool_name'] for row in rows if row.get('source_excerpt')]
    assert hydrated[:3] == ['browser_extract_text', 'browser_extract_text', 'file_read']
    assert len(evidence) < 9300


@pytest.mark.parametrize('prompt', ['Run my saved report.', '保存済みのレポートを実行して。', 'Ejecuta mi informe guardado.'])
@pytest.mark.parametrize('dispatch_status', ['completed', 'failed'])
def test_completed_background_dispatch_is_not_replaced_by_unfulfilled_foreground_history(tmp_path, monkeypatch, prompt, dispatch_status):
    from nullion import chat_operator
    from nullion.agent_orchestrator import AgentOrchestrator, TurnResult
    from nullion.config import NullionSettings, TelegramSettings
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import ToolRegistry
    monkeypatch.setenv('NULLION_MEMORY_ENABLED', '0')
    runtime = bootstrap_persistent_runtime(tmp_path/'runtime.db')
    runtime.active_tool_registry = ToolRegistry()
    receipt = result('run_cron', {'delivery_status': 'deferred', 'task_group_id': 'background-report',
        'message': 'Started saved report. The result will arrive here.', 'id': 'saved-report'}, status=dispatch_status)
    class Model:
        def create(self, **kwargs):
            return {'content': [{'type': 'text', 'text': '{}'}]}
    model = Model()
    monkeypatch.setattr(AgentOrchestrator, 'run_turn', lambda self, **kwargs: TurnResult(
        turn_id='foreground-report', final_text=receipt.output['message'], tool_results=[receipt],
        response_fulfilled=False, completion_failure_reply='A foreground source read was not performed.'))
    settings = NullionSettings(telegram=TelegramSettings(operator_chat_id='2222', chat_enabled=True))
    reply = chat_operator.handle_chat_operator_message(runtime, prompt, chat_id='2222', settings=settings,
        request_id='start-saved-report', model_client=model, agent_orchestrator=AgentOrchestrator(model_client=model),
        append_activity_trace=False, allow_mini_agents=False)
    expected = receipt.output['message'] if dispatch_status == 'completed' else 'A foreground source read was not performed.'
    assert reply == expected
    turns = runtime.store.list_recent_conversation_events('telegram:2222', event_type='conversation.chat_turn', limit=5)
    assert turns[-1]['assistant_reply'] == expected
    assert turns[-1]['tool_results'][-1]['output']['delivery_status'] == 'deferred'


def test_repair_preserves_complete_structured_watchlist_before_search_previews():
    symbols = [f'SYM{i:02}' for i in range(16)]
    quotes = [{'symbol': symbol, 'price': 20+i, 'previous_close': 19+i, 'change': 1,
               'change_percent': 4.5, 'day_high': 21+i, 'day_low': 19+i, 'volume': 200000,
               'fifty_two_week_low': 10+i, 'fifty_two_week_high': 50+i,
               'currency': 'USD', 'exchange': 'NASDAQ', 'regular_market_time': '2026-10-05T19:23:00Z',
               'source_url': f'https://example.test/finance/chart/{symbol}?interval=1d&range=5d'}
              for i, symbol in enumerate(symbols)]
    previews = [result('web_search', {'results': [{'title': f'News preview {i}',
                'snippet': 'Context ' * 90, 'url': f'https://example.test/news/{i}'}]}) for i in range(16)]
    evidence = json.loads(_cron_final_repair_tool_evidence([result('market_quote', {'quotes': quotes}), *previews]))
    source_records = evidence[0]['source_records']
    assert {row['symbol'] for row in source_records if row.get('symbol')} == set(symbols)
    assert all(row['regular_market_time'] == '2026-10-05T19:23:00Z' for row in source_records if row.get('symbol'))
    assert {row['symbol']: (row['fifty_two_week_low'], row['fifty_two_week_high'])
            for row in source_records if row.get('symbol')} == {
                symbol: (10+i, 50+i) for i, symbol in enumerate(symbols)}


@pytest.mark.parametrize('task', ['Report current matching events.', '現在の該当イベントを報告。', 'Informa eventos coincidentes actuales.'])
def test_repair_rejection_records_match_metadata_without_source_or_receipt_prose(task):
    copied = 'Completed action: terminal_exec.'
    state = {'text': copied, 'tool_results': [
        result('terminal_exec', {'stdout': 'Private command output',
            'action_receipt': {'summary': copied}}),
        result('web_fetch', {'text': 'Current public event listings with no new match.'}),
    ]}
    events = []
    class Model:
        def create(self, **kwargs):
            return {'content': [{'type': 'text', 'text': json.dumps({
                'requirements_satisfied': True, 'source_state': 'available',
                'evidence_refs': ['e0'], 'report': copied})}]}
    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name='Monitor', task=task), state,
        model_client=Model(), record_preflight=lambda stage, **fields: events.append((stage, fields)))
    assert state['cron_agent_final_repair_failure'] == 'repair_still_invalid'
    rejected = [fields for stage, fields in events if stage == 'final_repair_rejected']
    assert len(rejected) == 2
    assert all(fields['validation_reason'] == 'internal_tool_output_leaked' for fields in rejected)
    assert all(fields['internal_output_match'] == {'tool_name': 'terminal_exec',
        'output_field': 'action_receipt.summary', 'matched_chars': len(copied),
        'report_chars': len(copied)} for fields in rejected)
    assert copied not in json.dumps(rejected) and 'Private command output' not in json.dumps(rejected)


@pytest.mark.parametrize('task', ['Report upcoming events only.', '今後のイベントだけを報告。', 'Informa solo eventos futuros.'])
def test_repair_and_primary_prompt_receive_actual_run_clock(task, monkeypatch):
    from datetime import UTC, datetime
    from nullion import cron_delivery
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 5, 20, 0, tzinfo=UTC).astimezone(tz)
    monkeypatch.setattr(cron_delivery, 'datetime', Clock)
    state = {'text': 'internal reference ' * 15,
        'tool_results': [result('skill_pack_read', {'text': 'internal reference ' * 15}),
                        result('web_fetch', {'text': 'Past listing on 2026-05-16.'})]}
    job = SimpleNamespace(name='Monitor', task=task)
    class Model:
        def create(self, **kwargs):
            prompt = kwargs['messages'][0]['content'][0]['text']
            context = json.loads(prompt.split('Current run context:\n', 1)[1].split('\n\n', 1)[0])
            assert context['current_datetime'] == '2026-10-05T20:00:00+00:00'
            return {'content': [{'type': 'text', 'text': json.dumps({
                'requirements_satisfied': True, 'source_state': 'available', 'evidence_refs': ['e0'],
                'delivery_decision': 'silent', 'delivery_reason': 'no_verified_match', 'report': ''})}]}
    _repair_cron_agent_final_text_if_needed(job, state, model_client=Model())
    assert state['cron_alert_delivery_decision'] == 'silent'
    assert '2026-10-05T20:00:00+00:00' in cron_delivery.cron_agent_prompt(job, label='Scheduled')


def test_late_source_details_survive_long_page_excerpt():
    text = 'Source heading\n' + 'earlier unrelated material ' * 180 + '\n新しい公演 November 7 22:30 venue BASEMENT https://example.com/tickets'
    evidence = _cron_final_repair_tool_evidence([result('browser_extract_text', {'text':text})])
    assert '新しい公演' in evidence and 'https://example.com/tickets' in evidence


def test_retry_names_failed_validation_and_allows_supported_optional_gaps():
    state={'text':'internal reference '*15,'tool_results':[result('skill_pack_read',{'text':'internal reference '*15}),result('web_fetch',{'text':'公演 November 7 22:30 BASEMENT; price unavailable; https://example.com/tickets'})]}
    class Model:
        calls=0
        def create(self, **kwargs):
            self.calls+=1
            if self.calls==2:
                prompt=kwargs['messages'][0]['content'][0]['text']
                assert '"requirements_satisfied"' in prompt
                assert 'optional sources or fields' in prompt
            return {'content':[{'type':'text','text':json.dumps({'requirements_satisfied':self.calls==2,'source_state':'available','evidence_refs':['e0'],'report':'公演 November 7 22:30 BASEMENT; price unavailable. https://example.com/tickets'})}]}
    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name='Monitor',task='公演を通知。価格は取得できれば記載。'),state,model_client=Model())
    assert state['cron_agent_final_repaired'] and 'November 7' in state['text']


def test_failure_notice_distinguishes_source_results_from_no_information():
    text = _cron_report_failure_notice({'cron_agent_final_repair_failure':'report_not_grounded_or_complete','cron_completed_source_checks':2})
    assert 'returned information' in text and 'report could not be completed' in text


@pytest.mark.parametrize('title,date', [('WRECKED at BASEMENT', 'November 7, 22:30'), ('公演 at 会場', '11月7日 22:30'), ('Actuación en Sala', '7 de noviembre, 22:30')])
def test_report_may_reuse_public_source_facts_without_being_an_internal_dump(title, date):
    from nullion.cron_delivery import cron_structured_result_block_reason
    source = '\n'.join(['Navigation', 'Older unrelated listings ' * 50, title, date, 'https://example.com/tickets', 'Footer'])
    report = f'**{title}**\n{date}\nVerified upcoming event. Price unavailable.\n[Tickets](https://example.com/tickets)'
    state = {'text':report, 'response_fulfilled':True, 'tool_results':[result('browser_extract_text', {'text':source})]}
    assert cron_structured_result_block_reason(state, text=report, artifacts=None) is None
    # Complete raw dumps remain blocked even with Markdown or report boilerplate.
    assert cron_structured_result_block_reason(state, text='**Report**\n' + source, artifacts=None) == 'cron_run_internal_tool_output_leaked'


def test_report_sharing_source_opening_is_deliverable():
    from nullion.cron_delivery import _cron_visible_text_copies_tool_text
    title = 'A verified upcoming event at a public venue on November 7, 22:30.'
    source = title + '\n' + 'Unrelated source navigation and other listings. ' * 40
    report = title + '\nTickets are available at https://example.com/tickets; price could not be verified.'
    assert not _cron_visible_text_copies_tool_text(report, source)


def test_middle_facts_survive_normal_sized_public_page():
    text = 'Navigation ' * 60 + '\nPUBLIC_FACT: 公演 November 7, 22:30, https://example.com/tickets\n' + 'Other listings ' * 65
    assert len(text) < 2400
    evidence = _cron_final_repair_tool_evidence([result('browser_extract_text', {'text':text})])
    assert 'PUBLIC_FACT' in evidence and 'https://example.com/tickets' in evidence


@pytest.mark.parametrize('task', ['Report supported findings and disclose unavailable checks.', '確認できた内容と未確認の項目を報告。', 'Informa los hallazgos y las comprobaciones no disponibles.'])
def test_failed_source_reference_can_support_uncertainty_with_completed_evidence(task):
    state={'text':'Internal preview','response_presentation':{'kind':'source_preview'},'tool_results':[
        result('web_fetch',{'text':'An appointment is due November 7, 2026; it already exists in the calendar.'}),
        result('web_fetch',{'reason':'handler_timeout'},'failed')]}
    class Model:
        def create(self, **kwargs):
            prompt=kwargs['messages'][0]['content'][0]['text']
            assert 'Permitted evidence_refs' in prompt
            return {'content':[{'type':'text','text':json.dumps({'requirements_satisfied':True,'source_state':'available',
                'evidence_refs':['e0','e1'],'report':'November 7 appointment is already scheduled. One additional check timed out; coverage is partial.'})}]}
    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name='Triage',task=task),state,model_client=Model())
    assert state.get('cron_agent_final_repaired') and state['response_fulfilled'] is True,state


@pytest.mark.parametrize('refs', [['e1'], ['invented'], ['e0','invented'], [None], []])
def test_uncertainty_references_still_require_existing_ids_and_completed_anchor(refs):
    state={'text':'Internal preview','response_presentation':{'kind':'source_preview'},'tool_results':[
        result('web_fetch',{'text':'Verified appointment on November 7, 2026.'}),result('web_fetch',{},'failed')]}
    class Model:
        def create(self,**kwargs):
            return {'content':[{'type':'text','text':json.dumps({'requirements_satisfied':True,'source_state':'available',
                'evidence_refs':refs,'report':'Verified appointment; one source failed.'})}]}
    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name='Triage',task='確認済みの情報だけを報告。'),state,model_client=Model())
    assert state.get('cron_agent_final_repair_failed') and state['response_fulfilled'] is False


@pytest.mark.parametrize('last_attempt', ['partial', 'invalid', 'timeout'])
def test_unfinished_findings_stay_internal_after_unsuccessful_recovery(last_attempt):
    state={'text':'Preview','response_presentation':{'kind':'source_preview'},'tool_results':[
        result('web_fetch',{'text':'Verified appointment at 19:30. Some other checks did not run.'})]}
    class Model:
        calls=0
        def create(self,**kwargs):
            self.calls+=1
            if self.calls==2:
                if last_attempt=='timeout': raise TimeoutError('External interruption')
                if last_attempt=='invalid': return {'content':[{'type':'text','text':'not JSON'}]}
            return {'content':[{'type':'text','text':json.dumps({'requirements_satisfied':False,'source_state':'available',
                'evidence_refs':['e0'],'report':'**Checked**\nThe appointment starts at 19:30. Remaining checks are unfinished.'})}]}
    model=Model()
    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name='確認',task='確認できた内容を報告。'),state,model_client=model)
    assert state['cron_completion_required'] and state['response_fulfilled'] is False
    assert '19:30' in state['cron_unfinished_findings']
    assert '19:30' not in state['text'] and 'Partial report' not in state['text']
    assert model.calls==2


@pytest.mark.parametrize('wrapper', ['plain','fenced'])
def test_repair_accepts_plain_and_fenced_json(wrapper):
    state={'text':'Preview','response_presentation':{'kind':'source_preview'},'tool_results':[result('web_fetch',{'items':[]})]}
    class Model:
        calls=0
        def create(self,**kwargs):
            self.calls+=1
            decision={'requirements_satisfied':True,'source_state':'empty','evidence_refs':['e0'],
                      'delivery_decision':'send','delivery_reason':'report',
                      'report':'The source was checked and contains no records.'}
            payload=json.dumps(decision)
            if wrapper=='fenced': payload='```json\n'+payload+'\n```'
            return {'content':[{'type':'text','text':payload}]}
    model=Model()
    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name='確認',task='結果を毎回報告。'),state,model_client=model)
    assert state.get('cron_agent_final_repaired') and state['text']
    assert not state.get('cron_alert_delivery_decision') and model.calls==1


@pytest.mark.parametrize('bad_field', ['evidence_refs','requirements_satisfied'])
def test_unsafe_partial_report_is_not_retained(bad_field):
    state={'text':'Preview','response_presentation':{'kind':'source_preview'},'tool_results':[result('web_fetch',{'text':'Verified data'})]}
    class Model:
        def create(self,**kwargs):
            decision={'requirements_satisfied':False,'source_state':'available','evidence_refs':['e0'],'report':'A partial result.'}
            decision[bad_field]=['invented'] if bad_field=='evidence_refs' else 'false'
            return {'content':[{'type':'text','text':json.dumps(decision)}]}
    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name='確認',task='結果を報告。'),state,model_client=Model())
    assert state.get('cron_agent_final_repair_failed') and not state.get('cron_report_partial')


def test_missing_typed_required_check_cannot_be_repaired_into_success():
    state={'text':'Preview','response_presentation':{'kind':'source_preview'},
           'cron_source_action_contract':{'missing_callable_required_tool_names':['lookup_schedule']},
           'tool_results':[result('web_fetch',{'text':'Verified obligation at 19:30.'})]}
    class Model:
        calls=0
        def create(self,**kwargs):
            self.calls+=1
            if self.calls==2:
                assert 'required_tools_completed' in kwargs['messages'][0]['content'][0]['text']
            return {'content':[{'type':'text','text':json.dumps({'requirements_satisfied':self.calls==1,
                'source_state':'available','evidence_refs':['e0'],
                'report':'The obligation is at 19:30. The duplicate check did not finish.'})}]}
    model=Model()
    _repair_cron_agent_final_text_if_needed(SimpleNamespace(name='確認',task='重複を確認。'),state,model_client=model)
    assert state.get('cron_completion_required') and state['response_fulfilled'] is False
    assert '19:30' in state['cron_unfinished_findings'] and model.calls==2
    assert '19:30' not in state['text']
