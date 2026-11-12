import pytest
from nullion.source_observation_contract import source_answer_context,source_answer_repair_context,source_answer_tool_definition,render_source_answer
from nullion.tools import ToolResult

@pytest.mark.parametrize('title,field',[('Exhibit A','Long source field '),('展示A','長い引用文'),('Exposición A','Descripción extensa ')])
def test_repair_reports_length_even_when_quote_relationship_fails_first(title,field):
    values=[title,field*100,'Another independent property '*35]
    result=ToolResult('capture','browser_run_task','completed',{'result_kind':'source_observations','report_status':'completed',
        'observations':[{'value':v,'quote':v,'source_url':'https://example.test/record','captured_at':100} for v in values]})
    context=source_answer_context([result]);rows=context['evidence'];ids=[r['excerpts'][0]['id'] for r in rows]
    plan={'intro':'','focus_roles':['entity'],'points':[{'evidence_ids':[r['id'] for r in rows],
        'title_ids':[ids[0]],'detail_ids':ids[1:],'detail_groups':[ids[1:]],'semantic_role':'entity','relevance':'Requested exhibit and properties'}],'limitation_ids':[]}
    with pytest.raises(ValueError,match='quoted relationship') as failed:render_source_answer([result],plan,require_semantics=True)
    diagnostic=source_answer_repair_context([result],plan,failed.value)
    issue=next(row for row in diagnostic['validation_issues'] if row.get('code')=='supporting_excerpt_limit')
    assert issue['point_index']==0 and issue['max_detail_chars']==650 and issue['detail_chars']>650
    assert context['rendering_limits']['max_detail_chars_per_point']==650
    assert '650 characters' in source_answer_tool_definition(context,require_semantics=True)['description']


@pytest.mark.parametrize('field',['Long verified field ', '長い引用文', 'Descripción extensa '])
def test_long_verified_field_is_not_a_completion_failure(field):
    detail=field*100
    result=ToolResult('capture','opaque_read','completed',{'result_kind':'source_observations',
        'report_status':'completed','observations':[{'value':'Subject\n'+detail,
            'quote':'Subject\n'+detail,'semantic_role':'entity',
            'source_url':'https://example.test/record','captured_at':100}]})
    row=source_answer_context([result])['evidence'][0]
    plan={'intro':'','focus_roles':['entity'],'points':[{'evidence_ids':[row['id']],
        'title_ids':[row['excerpts'][0]['id']],'detail_ids':[row['excerpts'][1]['id']],
        'detail_groups':[[row['excerpts'][1]['id']]],'semantic_role':'entity',
        'relevance':'Requested complete field'}],'limitation_ids':[]}
    assert detail.strip() in render_source_answer([result],plan,require_semantics=True)
