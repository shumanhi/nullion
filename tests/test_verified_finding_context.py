import json
import pytest
from nullion.chat_text import ReplyFinding,complete_verified_findings
from nullion.live_tracking import evaluate_semantic


@pytest.mark.parametrize('summary',['条件に一致しました。','Se cumple la condición.','تم التحقق من التغيير.'])
def test_verified_context_is_completed_without_english_triggers(summary):
    value=complete_verified_findings(summary,(ReplyFinding('散歩道が開通',citations='https://example.test/garden'),),preferences={'emoji_level':'none','markdown_style':'plain'})
    assert summary in value and '散歩道が開通' in value and 'https://example.test/garden' in value
    assert complete_verified_findings(value,(ReplyFinding('散歩道が開通',citations='https://example.test/garden'),))==value


@pytest.mark.parametrize('url',['javascript:alert(1)','file:///secret','https://user:password@example.test/a','https://[broken'])
def test_context_does_not_publish_unsafe_citations(url):
    value=complete_verified_findings('結果',(ReplyFinding('Matched record',citations=url),))
    assert 'Matched record' in value and url not in value


@pytest.mark.parametrize('pointer,kind,value',[('/items/1/title','value','Selected title'),('/items/1','value',{'title':'Selected title','canonical_url':'https://example.test/selected'})])
def test_semantic_generic_summary_gets_only_the_cited_current_record(pointer,kind,value):
    output={'result_kind':'browser_item_collection','items':[
        {'title':'Unrelated private row','canonical_url':'https://example.test/unrelated'},
        {'title':'Selected title','canonical_url':'https://example.test/selected'}]}
    class Model:
        calls=0
        def create(self,**kw):
            self.calls+=1
            return {'content':[{'type':'text','text':json.dumps({'status':'matched','summary':'A change matched.',
                'evidence':[{'step':0,'pointer':pointer,'kind':kind,'value':value}]})}]}
    model=Model();result=evaluate_semantic({'instruction':'Only the selected result'},[output],model)
    assert result.status=='matched' and model.calls==1
    assert 'Selected title' in result.summary and 'https://example.test/selected' in result.summary
    assert 'Unrelated private row' not in result.summary and '/unrelated' not in result.summary


def test_empty_authored_match_is_still_invalid():
    class Model:
        def create(self,**kw):return {'content':[{'type':'text','text':json.dumps({'status':'matched','summary':'',
            'evidence':[{'step':0,'pointer':'/text','value':'Cedar changed'}]})}]}
    result=evaluate_semantic({'instruction':'Current change'},[{'text':'Cedar changed'}],Model())
    assert result.status=='unknown' and result.reason=='evaluation_invalid'
