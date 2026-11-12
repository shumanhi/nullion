"""Real Web source verification and response-only obligations in one request."""
import os
import pytest
from tests.e2e.test_live_stage_web_matrix import live_web,_submit,catalogue

pytestmark=pytest.mark.skipif(os.environ.get('NULLION_LIVE_STAGE_E2E')!='1',reason='Opt-in stage UI')

@pytest.mark.parametrize('prompt',["ニューヨーク市の明日の天気を確認して短く答え、時間別の内訳が必要か最後に質問してください。",
    'Consulta el pronóstico de mañana para Nueva York. Responde brevemente y termina preguntando si quiero el desglose por hora.'])
def test_live_web_forecast_and_composed_question_use_different_proof(live_web,record_property,prompt):
    response,turn,elapsed=_submit(live_web,prompt,timeout=150)
    assert any(t['tool_name']=='weather_forecast' and t['status']=='completed' for t in turn.get('tool_results',[])),turn
    assert '?' in response['text'] or '？' in response['text'],response
    assert turn.get('response_fulfilled') is True,turn
    database,conversation=live_web[3:5]
    plans=catalogue._conversation_event_payloads(database,conversation,event_type='conversation.completion_requirement_contract')
    rows=plans[-1]['requirement_rows'];assert not any(r['requirement_kind']=='action' and not r['eligible_evidence_tool_names'] for r in rows),rows
    reviews=catalogue._conversation_event_payloads(database,conversation,event_type='conversation.completion_review')
    assert reviews[-1]['disposition']=='complete' and len(reviews)<=2,reviews
    assert not any(r.get('runtime_failure_code')=='unverified_requirements' for r in reviews),reviews
    assert elapsed<120000,elapsed
    record_property('forecast_question_latency_ms',elapsed)
