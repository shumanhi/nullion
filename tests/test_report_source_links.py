from copy import deepcopy
import json

import pytest

from nullion.chat_text import render_report_message
from nullion.preferences import Preferences
from tests.report_presentation_fixture import report_fixture


@pytest.mark.parametrize('detailed', [False, True])
@pytest.mark.parametrize('label', ['出典', 'Ficha del producto'])
def test_structured_source_url_survives_compact_and_detailed_rendering(detailed, label):
    message, _, _ = report_fixture()
    message['sources'][0].update(label=label, url='https://catalog.example/item?variant=2&region=3')
    message['detail_level'] = 'detailed' if detailed else 'compact'
    text = render_report_message(message, preferences=Preferences())
    from nullion.telegram_formatting import format_telegram_text
    wire, _ = format_telegram_text(text)
    assert 'href="https://catalog.example/item?variant=2&amp;region=3"' in wire
    assert label in wire and '$720' in text


def test_bound_links_repair_label_only_report_without_changing_facts_or_inventing_targets():
    message, _, _ = report_fixture()
    before = deepcopy(message)
    urls = ['https://one.example/item', 'https://two.example/item', 'https://one.example/item']
    text = render_report_message(message, bound_source_urls=urls)
    assert all(text.count(url) == 1 for url in set(urls))
    assert '$720' in text and '$760' in text and message == before
    message['sources'][0]['url'] = urls[0]
    assert render_report_message(message, bound_source_urls=urls).count(urls[0]) == 1


@pytest.mark.parametrize('url', ['javascript:alert(1)', 'file:///private/data', 'https://user:secret@site.example/',
                               'https://site.example/\nprivate'])
def test_source_url_field_does_not_make_unsafe_or_private_targets_clickable(url):
    message, _, _ = report_fixture()
    message['sources'][0]['url'] = url
    text = render_report_message(message, bound_source_urls=[url])
    assert url not in text and '$720' in text


def test_cosmetic_report_fallback_preserves_bound_links_and_native_labels():
    message, _, _ = report_fixture()
    message['sections'][0]['layout'] = 'unknown'
    message['sources'][0]['url'] = 'https://catalog.example/item'
    text = render_report_message(message, bound_source_urls=['https://other.example/item'])
    assert 'https://catalog.example/item' in text and 'https://other.example/item' in text
    assert message['sources'][1]['label'] in text


def test_only_finding_bound_runtime_records_supply_missing_report_links():
    from nullion.agent_orchestrator import (_completion_review_source_links,
        _parse_completion_review_decision, _CompletionRequirementContractRow as Contract)
    from nullion.tools import ToolResult
    from tests.test_reviewed_presentation_correction import correction_payload
    records = [ToolResult(identity, 'opaque_read', 'completed', {'sources': [{'source_url': url}]})
        for identity, url in [('selected/source-record/1', 'https://selected.example/item'),
                              ('other/source-record/2', 'https://other.example/item'),
                              ('browser-parent', 'https://navigation.example/inventory')]]
    refs = {'e0001': records[0].invocation_id, 'e0002': records[1].invocation_id}
    links = _completion_review_source_links(records, {r.invocation_id: 'proof' for r in records[:2]}, refs)
    assert 'browser-parent' not in links
    payload = correction_payload()
    for row in payload['material_requirements'] + payload['supported_findings']:
        row['evidence_invocation_ids'] = ['e0001']
    review = _parse_completion_review_decision({'content': [{'type': 'text', 'text': json.dumps(payload)}]},
        registered_tool_names={'opaque_read'}, requirement_contract=(Contract('r', '選択結果', 'fact', ('opaque_read',)),),
        report_source_links=links)
    assert review and 'https://selected.example/item' in review.corrected_reply
    assert 'https://other.example/item' not in review.corrected_reply
    assert 'https://navigation.example/inventory' not in review.corrected_reply
