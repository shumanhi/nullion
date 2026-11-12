import json
from copy import deepcopy

import pytest

from nullion.chat_text import (REPORT_MESSAGE_FORMAT, render_report_message, render_report_envelope,
    structured_user_message)
from nullion.preferences import Preferences
from tests.report_presentation_fixture import report_fixture


@pytest.mark.parametrize('extension', ['html', 'pdf', 'docx', 'png', 'csv', 'zip'])
@pytest.mark.parametrize('detail_level', ['compact', 'detailed'])
def test_structured_envelope_with_explicit_attachment_uses_shared_renderer(extension, detail_level):
    message, _, _ = report_fixture()
    message['detail_level'] = detail_level
    marker = 'MEDIA:/owned/artifacts/資料.' + extension
    envelope = json.dumps({'format': REPORT_MESSAGE_FORMAT, 'message': message})
    diagnostics = {}
    rendered = render_report_envelope(envelope+'\n'+marker, diagnostics=diagnostics)
    assert rendered == render_report_message(message)+'\n'+marker
    assert diagnostics['report_presentation'] == 'structured'
    assert ('Historical detail' in rendered) == (detail_level == 'detailed')
    assert '$720' in rendered


@pytest.mark.parametrize('suffix', ['\n未完の注記', '\nMEDIA:', '\nMEDIA:/owned/a.pdf\nNota adicional'])
def test_trailing_content_is_preserved_without_exposing_envelope_internals(suffix):
    message, _, _ = report_fixture()
    raw = json.dumps({'format': REPORT_MESSAGE_FORMAT, 'message': message})+suffix
    assert render_report_envelope(raw) == render_report_message(message)+suffix


@pytest.mark.parametrize('marker', ['MEDIA:', '  MEDIA:   ', '\tMEDIA:\t'])
def test_empty_structured_media_marker_is_invalid_without_crashing(marker):
    from nullion.artifacts import parse_media_directive_line
    assert parse_media_directive_line(marker) is None


def test_compact_renderer_groups_common_outcomes_and_preserves_required_findings():
    message, verbose, _ = report_fixture()
    diagnostics = {}
    result = render_report_message(message, preferences=Preferences(markdown_style='plain'), diagnostics=diagnostics)
    assert len(result) < len(verbose) * .65
    assert result.count(message['shared_details'][0]['text']) == 1
    assert 'Historical detail' not in result and 'Full subject:' not in result
    assert 'Final posting is unconfirmed' in result and '$720' in result and '$760' in result
    assert 'amount unavailable' in result and 'optional $10 reward' in result
    for source in message['sources']:
        assert result.count(source['label']) == 1
    assert diagnostics['report_detail_level'] == 'compact'


def test_detailed_override_retains_expansion_and_full_requested_provenance():
    message, _, _ = report_fixture(); message['detail_level'] = 'detailed'
    detailed = render_report_message(message, preferences=Preferences())
    for row in message['sections']:
        for item in row['items']:
            assert all(detail in detailed for detail in item['extra_details'] + item['required_details'])
    for source in message['sources']:
        assert source['label'] in detailed and source['expanded_label'] in detailed
    assert all(text in detailed for text in message['extra_details'])


@pytest.mark.parametrize('context', [
    'Calendar checks confirm existing entries; email notices alone do not prove settlement.',
    '予定表の照合手順とメールが証明できる範囲についての説明。',
    'Explicación de cómo se comprobaron los avisos y las entradas existentes.',
])
@pytest.mark.parametrize('detail_level', ['compact', 'default', 'detailed'])
def test_report_wide_explanation_has_explicit_optional_ownership(context, detail_level):
    from tests.report_presentation_fixture import triage_outcome_fixture
    message = triage_outcome_fixture(); message['detail_level'] = detail_level
    message['extra_details'] = [context]
    diagnostics = {}
    result = render_report_message(message, preferences=Preferences(response_length='concise'), diagnostics=diagnostics)
    assert (context in result) is (detail_level == 'detailed')
    for essential in ['$1,200', '$105.80', '$102', '$400', '$90', '$41', '$12',
            'Payment unconfirmed', 'Posting unconfirmed', 'Completion unconfirmed', 'Nov 25', '0 new events']:
        assert essential in result
    assert result.count(message['shared_details'][0]['text']) == 1
    if detail_level != 'detailed':
        assert len(result.split()) < 200
        assert 'Full subject:' not in result


def test_report_wide_optional_context_obeys_the_existing_detailed_preference():
    message, _, _ = report_fixture(); message['detail_level'] = 'default'
    context = message['extra_details'][0]
    assert context in render_report_message(message, preferences=Preferences(response_length='detailed'))
    assert context not in render_report_message(message, preferences=Preferences(response_length='concise'))


def test_legacy_report_without_report_wide_expansion_remains_valid():
    message, _, _ = report_fixture(); message.pop('extra_details')
    diagnostics = {}
    assert '$720' in render_report_message(message, diagnostics=diagnostics)
    assert diagnostics['report_presentation'] == 'structured'


@pytest.mark.parametrize('detailed', [False, True])
def test_cosmetic_fallback_preserves_optional_report_wide_context_ownership(detailed):
    from tests.report_presentation_fixture import triage_outcome_fixture
    message = triage_outcome_fixture()
    message['detail_level'] = 'detailed' if detailed else 'compact'
    message['sections'][0]['layout'] = 'unsupported'
    diagnostics = {}
    result = render_report_message(message, preferences=Preferences(), diagnostics=diagnostics)
    assert diagnostics['report_presentation'] == 'content_fallback'
    assert (message['extra_details'][0] in result) is detailed
    assert '$105.80' in result and 'Posting unconfirmed' in result


def test_default_detail_level_uses_shared_saved_preference():
    message, _, _ = report_fixture(); message['detail_level'] = 'default'
    assert 'Historical detail' in render_report_message(message, preferences=Preferences(response_length='detailed'))
    assert 'Historical detail' not in render_report_message(message, preferences=Preferences(response_length='concise'))


def test_repeated_source_refs_are_rendered_once_and_primary_precedes_secondary():
    message, _, _ = report_fixture(); message['sections'].reverse()
    message['sections'][0]['items'][0]['source_ids'] = ['s0', 's0']
    result = render_report_message(message, preferences=Preferences())
    assert result.index('Act now') < result.index('Other findings')
    assert result.count(message['sources'][0]['label']) == 1


@pytest.mark.parametrize('bad', ['reference', 'duplicate', 'layout'])
def test_cosmetic_schema_failures_preserve_usable_text_without_retries_or_new_gate(bad):
    message, verbose, _ = report_fixture()
    if bad == 'reference': message['sections'][0]['items'][0]['source_ids'] = ['unknown']
    elif bad == 'duplicate': message['sources'].append(deepcopy(message['sources'][0]))
    else: message['sections'][0]['layout'] = 'unsupported'
    diagnostics = {}
    assert render_report_message(message, fallback=verbose, preferences=Preferences(), diagnostics=diagnostics) == verbose
    assert diagnostics['report_presentation'] == 'content_fallback'
    result = render_report_message(message, preferences=Preferences())
    assert '$200' in result and '$720' in result and '$760' in result
    assert 'Final posting is unconfirmed' in result and 'Historical detail' in result


@pytest.mark.parametrize('style,emoji', [('plain', 'none'), ('light', 'minimal'), ('full', 'expressive')])
def test_original_and_recovery_adapt_through_same_message_renderer(style, emoji):
    message, verbose, _ = report_fixture()
    prefs = Preferences(markdown_style=style, emoji_level=emoji)
    envelope = {'format': REPORT_MESSAGE_FORMAT, 'message': message, 'text': verbose}
    expected = render_report_message(message, preferences=prefs)
    assert render_report_envelope(json.dumps(envelope), preferences=prefs) == expected
    assert structured_user_message(envelope, preferences=prefs) == expected
    assert render_report_envelope('Ordinary prose', preferences=prefs) == 'Ordinary prose'
    assert render_report_envelope('{"format":"other","message":"literal JSON"}', preferences=prefs) == '{"format":"other","message":"literal JSON"}'


def test_group_attachment_cleanup_preserves_shared_paragraphs_and_lists():
    from nullion.result_aggregator import (
        _strip_attached_artifact_references, _strip_unverified_attachment_claims,
    )
    text = '確認済み。\n\nFindings confirmed.\n\nActions\n• Amount unavailable.\n• Deadline tomorrow.\n\nCalendar: no changes.'
    assert _strip_unverified_attachment_claims(text) == text
    assert _strip_attached_artifact_references(text, ['/tmp/report.pdf']) == text
    cleaned = _strip_unverified_attachment_claims('The PDF file is attached.\n\n' + text)
    assert cleaned == text


def test_reviewers_receive_same_report_policy_without_affecting_plain_turn_context():
    from nullion.agent_orchestrator import _completion_review_request_context
    assert 'report_presentation_contract' not in _completion_review_request_context({})
    state = {'tool_flow_context': {'scheduled_task_run': True},
             'response_presentation': {'report_presentation': 'structured', 'report_detail_level': 'compact'}}
    policy = _completion_review_request_context(state)['report_presentation_contract']
    assert policy['detail_level'] == 'compact'
    assert 'expanded_label and extra_details are optional' in policy['review_guidance']
    assert 'never certifies facts' in policy['review_guidance']


@pytest.mark.parametrize('valid', [True, False])
def test_json_document_records_bind_literal_values_without_parsing_plain_file_prose(valid):
    from nullion.tools import ToolResult
    from nullion.source_observation_contract import source_capture_review_records
    content = json.dumps({'記録/一覧': [{'識別子': 'a', 'amount': 10}, {'識別子': 'b', 'amount': 20}]})
    result = ToolResult('owned', 'file_read', 'completed', {'content': content,
        **({'source_document_format': 'json'} if valid else {})})
    rows, fingerprints = source_capture_review_records([result])
    if not valid:
        assert not rows and not fingerprints
        return
    records = [r for r in rows if '識別子' in r.output['record']]
    assert {r.output['record']['amount'] for r in records} == {10, 20}
    assert {r.output['source_record_binding']['json_pointer'] for r in records} == {
        '/記録~1一覧/0', '/記録~1一覧/1'}
    assert len({fingerprints[r.invocation_id] for r in records}) == 2
    for bad in ({'truncated': True}, {'content': 'invalid json'}):
        invalid = ToolResult('owned', 'file_read', 'completed', {**result.output, **bad})
        assert source_capture_review_records([invalid]) == ([], {})


@pytest.mark.parametrize('title',['記録の要点','Resumen de avisos'])
def test_delivery_owned_title_uses_same_renderer_without_nested_report_heading(title):
    message,_,_=report_fixture();message['title']=title
    prefs=Preferences(emoji_level='expressive')
    body=render_report_message(message,preferences=prefs,include_title=False)
    assert title not in body and not body.startswith('📊')
    assert '$720' in body and '$760' in body
    assert render_report_envelope(json.dumps({'format':REPORT_MESSAGE_FORMAT,'message':message}),
        preferences=prefs,include_title=False)==body
    assert title in render_report_message(message,preferences=prefs)


def test_required_content_title_survives_an_enclosing_delivery_title():
    message,_,_=report_fixture();message.update(title='Exact requested report title',title_is_required=True)
    body=render_report_message(message,include_title=False,preferences=Preferences())
    assert 'Exact requested report title' in body and '$720' in body
