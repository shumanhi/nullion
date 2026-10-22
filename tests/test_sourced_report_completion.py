from __future__ import annotations

import html
import json
from pathlib import Path

import pytest

from nullion.agent_orchestrator import (
    _compact_focused_artifact_source_history,
    _completion_review_required,
    _focus_tools_for_ready_artifact_production,
)
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec


def registry(*names):
    result = ToolRegistry()
    for name in names:
        result.register(ToolSpec(name, name, ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 5), lambda call: None)
    return result


@pytest.mark.parametrize('prompt', ['Compare every requested window in report.pdf', '比較対象をすべて report.pdf にまとめて', 'Compara todas las opciones en report.pdf'])
@pytest.mark.parametrize('extension', ['.pdf', '.xlsx', '.html'])
@pytest.mark.parametrize('source_tool', ['browser_extract_text', 'web_search'])
def test_page_text_keeps_research_tools_and_requires_outcome_review(prompt, extension, source_tool):
    tools = registry('browser_navigate', 'browser_extract_text', 'browser_snapshot', 'browser_type_id', 'browser_click_id', 'pdf_create')
    results = [ToolResult('page', source_tool, 'completed', {'text': 'Generic landing page recommendations'})]
    state = {'user_message': prompt, 'tool_registry': tools, 'tool_flow_context': {'requires_artifact_delivery': True, 'required_artifact_extensions': [extension]}}
    assert _focus_tools_for_ready_artifact_production(state, tool_registry=tools, tool_results=results) is tools
    # All tools returning success does not establish comparison coverage.
    assert _completion_review_required(state, tool_results=results)
    assert not _completion_review_required({'user_message': prompt, 'tool_registry': tools}, tool_results=results)


def test_pdf_argument_error_survives_focused_history_compaction(tmp_path):
    from nullion.tools import _build_pdf_create_handler, _default_input_schema_for_tool
    from nullion.tool_repair_contract import tool_repair_kind
    error = _build_pdf_create_handler(tmp_path, include_principal_workspace=False)(ToolInvocation('bad', 'pdf_create', 'owner', {'title': 'Report'}, 'capsule'))
    assert error.status == 'failed'
    assert tool_repair_kind(error) == 'correct_arguments'
    schema = _default_input_schema_for_tool('pdf_create')
    assert {row['required'][0] for row in schema['anyOf']} == {'text_pages', 'html_pages', 'image_paths', 'screenshot_paths'}
    compact = _compact_focused_artifact_source_history([], state={'tool_flow_context': {'requires_artifact_delivery': True}}, tool_results=[error], model_tool_registry=registry('pdf_create'))
    packet = json.dumps(compact)
    assert error.error in packet
    assert 'correct_arguments' in packet
    assert 'text_pages' in packet


@pytest.mark.parametrize('url', [
    'https://search.example.test/?lang=en&party=1#route=AAA.BBB.2026-10-14*BBB.AAA.2026-10-18;c:USD;e:1;sd:1;t:f',
    'https://catalog.example.test/items?tag=a,b&filter=x%26y#panel=one;sort:asc',
])
def test_pdf_markdown_formats_content_and_preserves_complete_urls(url):
    from nullion.tools import _pdf_text_html
    rendered = _pdf_text_html(f'# Comparison\n\n## Windows\n\n| Option | Cost |\n| --- | --- |\n| **A** | *240* |\n\n- Selected option\n\n[Open result]({url})\n\n{url}\n\n<script>alert(1)</script>')
    assert '<h1>Comparison</h1>' in rendered
    assert '<h2>Windows</h2>' in rendered
    assert '<strong>A</strong>' in rendered
    assert '<em>240</em>' in rendered
    assert '<table class="report-table">' in rendered
    assert '<li>Selected option</li>' in rendered
    assert rendered.count(f'href="{html.escape(url, quote=True)}"') == 2
    assert '<script>' not in rendered
    assert '&lt;script&gt;' in rendered


def test_pdf_markdown_cannot_load_inline_images_or_execute_link_protocols():
    from nullion.tools import _pdf_text_html
    rendered = _pdf_text_html('![private image](http://127.0.0.1/secret)\n\n[unsafe](javascript:alert%281%29)')
    assert '<img' not in rendered
    assert 'src=' not in rendered
    assert 'href="javascript:' not in rendered


@pytest.mark.parametrize('recovery_kind', ['semantic_retry', 'artifact_refresh'])
def test_sourced_report_recovery_preserves_scoped_research_and_producer_tools(monkeypatch, recovery_kind):
    from nullion.agent_orchestrator import _CompletionReviewDecision, _agent_turn_finalize_node
    monkeypatch.setattr('nullion.agent_orchestrator._review_risky_agent_completion',
                        lambda *args, **kwargs: _CompletionReviewDecision('retry', ('Missing comparison records',), ('browser_snapshot',)))
    tools = registry('browser_extract_text', 'browser_snapshot', 'browser_type_id', 'browser_click_id', 'pdf_create')
    results = [ToolResult('page', 'browser_extract_text', 'completed', {'text': 'Landing page'})]
    if recovery_kind == 'artifact_refresh':
        # A new source record after the partial report must not close unfinished discovery.
        results = [ToolResult('draft', 'pdf_create', 'completed', {'path': '/tmp/report.pdf'}), *results]
        monkeypatch.setattr('nullion.agent_orchestrator._stale_artifact_extensions_after_new_evidence',
                            lambda *args, **kwargs: {'.pdf'})
    update = _agent_turn_finalize_node({
        'conversation_id': 'telegram:report', 'principal_id': 'owner', 'user_message': '比較を report.pdf にまとめて',
        'content': [{'type': 'text', 'text': 'Attached report.pdf'}], 'messages': [], 'tool_results': results,
        'artifacts': [], 'tool_registry': tools, 'runtime_store': None, 'completion_review_count': 0,
        'tool_flow_context': {'requires_artifact_delivery': True, 'required_artifact_extensions': ['.pdf']},
    })
    assert update['tool_registry'] is tools
    if recovery_kind == 'artifact_refresh':
        nudge = update['messages'][-1]['content'][0]['text']
        assert 'Complete any remaining source work' in nudge
        assert 'Do not continue discovery' not in nudge
    else:
        assert update['completion_review_count'] == 1


@pytest.mark.parametrize('status,quote,value,positive', [
    ('partial','Checked at 19:30','19:30',True),
    ('completed','Checked at 19:30','19:30',True),
    ('blocked','Checked at 19:30','19:30',False),
    ('partial','Unrelated','19:30',False),
    ('partial','','',False),
])
def test_quoted_source_observations_are_fact_evidence_without_certifying_whole_task(status,quote,value,positive):
    from nullion.agent_orchestrator import _completion_review_evidence_result_is_positive
    observed=ToolResult('observed','browser_run_task','completed',{'result_kind':'source_observations',
        'report_status':status,'artifact_paths':[], 'artifact_descriptors':[],
        'observations':[{'value':value,'quote':quote}], 'unknowns':['Required account action not completed']})
    assert _completion_review_evidence_result_is_positive(observed) is positive
    state={'tool_registry':registry('browser_run_task'), 'tool_results':[observed]}
    if status=='partial' and positive:
        assert _completion_review_required(state,tool_results=[observed])
