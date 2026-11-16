"""Request session composition, native permissions and real sandbox regressions."""
import time

import pytest

from nullion import code_mode
from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, ToolResult


def registry():
    reg = ToolRegistry()
    for name, effect, allowed in [('read', ToolSideEffectClass.READ, True),
            ('write', ToolSideEffectClass.WRITE, True), ('legacy', ToolSideEffectClass.READ, False)]:
        reg.register(ToolSpec(name, name, ToolRiskLevel.LOW, effect, False, 5,
            input_schema={'type': 'object'}, code_mode_allowed=allowed),
            lambda i: ToolResult(i.invocation_id, i.tool_name, 'completed', {'number': 7}))
    return reg


def execute(state, content):
    from nullion.tools import ToolInvocation
    b = content[0]
    r = state['tool_registry'].invoke(ToolInvocation(b['id'], b['name'], 'test', b['input']))
    return {'tool_results': [*state.get('tool_results', []), r],
            'messages': [*state.get('messages', []), {'role':'user', 'content':[
                {'type':'tool_result','tool_use_id':b['id'],'content':[]}]}]}


def run(code, monkeypatch, names=None, executor=execute):
    monkeypatch.setenv('NULLION_CODE_MODE', 'read_only')
    state = {'tool_registry': registry(), 'messages': [], 'principal_id': 'test'}
    code_mode.offer_code_mode(state['tool_registry'].list_tool_definitions(), state['tool_registry'], state=state)
    return code_mode.execute_if_selected(state, [{'type': 'tool_use', 'name': code_mode.TOOL_NAME,
        'id': 'outer', 'input': {'code': code, 'tool_names': names if names is not None else ['read']}}], execute=executor)


def test_disabled_path_does_not_import_or_probe_monty(monkeypatch):
    monkeypatch.delenv('NULLION_CODE_MODE', raising=False)
    monkeypatch.setattr(code_mode.importlib.util, 'find_spec', lambda _: pytest.fail('dependency probe'))
    definitions = [{'name': 'read'}]
    assert code_mode.offer_code_mode(definitions, registry()) is definitions
    assert code_mode.execute_if_selected({}, [], execute=execute) is None


def test_scope_uses_adapter_opt_in_and_read_metadata(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE', 'read_only')
    reg = registry()
    offered = code_mode.offer_code_mode(reg.list_tool_definitions(), reg)
    assert offered[-1]['input_schema']['properties']['tool_names']['items']['enum'] == ['read']
    assert run('1', monkeypatch, ['write'])['tool_results'][-1].output['executed_calls'] == 0


def test_code_mode_and_scope_share_artifact_producer_precedence(monkeypatch):
    from nullion.reasoning_guidance import ARTIFACT_PRODUCTION_GUIDANCE, ARTIFACT_PRODUCER_SELECTION_GUIDANCE
    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    reg = registry()
    description = code_mode.offer_code_mode(reg.list_tool_definitions(), reg)[-1]['description']
    assert ARTIFACT_PRODUCER_SELECTION_GUIDANCE in description
    assert ARTIFACT_PRODUCER_SELECTION_GUIDANCE in ARTIFACT_PRODUCTION_GUIDANCE
    assert 'terminal_exec for rendering with installed Python libraries' not in description


def test_real_monty_composes_and_retains_receipts(monkeypatch):
    update = run("rows = [call_tool('read', {}) for _ in range(3)]\nsum(r['output']['number'] for r in rows)", monkeypatch)
    assert update['tool_results'][-1].output['value'] == 21
    assert len(update['tool_results']) == 4
    assert update['messages'][-1]['content'][0]['tool_use_id'] == 'outer'


@pytest.mark.parametrize('reader', ['source_13', 'fuente_9', '資料_7'])
def test_program_can_plan_native_prerequisite_from_metadata_without_a_failed_round(monkeypatch, reader):
    import json
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=ToolRegistry();calls=[]
    for name,effect,prerequisites in [(reader,ToolSideEffectClass.READ,()),
                                      ('worker',ToolSideEffectClass.WRITE,(reader,))]:
        def handler(inv):
            calls.append(inv.tool_name)
            return ToolResult(inv.invocation_id,inv.tool_name,'completed',{'number':7})
        reg.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,5,
            input_schema={'type':'object'},code_mode_allowed=True,code_mode_observation_tools=prerequisites),handler)
    state={'tool_registry':reg,'messages':[],'principal_id':'test'}
    offered=code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    signatures=json.loads(offered[-1]['description'].split('Compact scoped signatures: ',1)[1])
    assert signatures['worker']['requires_observation_any']==[reader]
    program="info=describe_tool('worker')\nsource=info['requires_observation_any'][0]\ncall_tool(source,{})\ncall_tool('worker',{})"
    result=code_mode.execute_if_selected(state,[{'type':'tool_use','name':'code_mode','id':'one-program',
        'input':{'code':program,'tool_names':[reader,'worker']}}],execute=execute)
    assert calls==[reader,'worker']
    assert all(row.status=='completed' for row in result['tool_results'])
    assert result['tool_results'][-1].output['executed_calls']==2


def test_native_wait_does_not_consume_the_python_orchestration_deadline(monkeypatch):
    from types import SimpleNamespace
    clock=[0.0];calls=[]
    monkeypatch.setattr(code_mode,'time',SimpleNamespace(monotonic=lambda:clock[0]))
    def native(state,blocks):
        calls.append(blocks[0]['id']);clock[0]+=100
        return execute(state,blocks)
    result=run("a=call_tool('read', {})\nb=call_tool('read', {})\na['output']['number']+b['output']['number']",
        monkeypatch,executor=native)['tool_results'][-1]
    assert result.status=='completed' and result.output['value']==14
    assert len(calls)==2 and result.output['execution_timing']['native_wait_seconds']==200


@pytest.mark.parametrize('asynchronous',[False,True])
def test_terminal_native_failure_is_hidden_from_shared_scope_after_program_yields(monkeypatch,asynchronous):
    import asyncio
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=ToolRegistry()
    reg.register(ToolSpec('failed_source','Read source',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,
        input_schema={'type':'object'},code_mode_allowed=True),
        lambda inv:ToolResult(inv.invocation_id,inv.tool_name,'failed',
            {'failure_scope':'tool','terminal_user_action_required':True,'retryable':False}))
    state={'tool_registry':reg,'messages':[],'principal_id':'test'}
    code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    content=[{'type':'tool_use','name':'code_mode','id':'outer',
        'input':{'tool_names':['failed_source'],'code':"call_tool('failed_source', {})"}}]
    async def native(current,blocks):return execute(current,blocks)
    update=(asyncio.run(code_mode.execute_async(state,content,execute=native)) if asynchronous
        else code_mode.execute_if_selected(state,content,execute=execute))
    assert update['tool_results'][-1].output['unavailable_tool_names']==['failed_source']
    assert not update['tool_registry'].can_invoke_tool('failed_source')
    assert update['tool_registry'].list_specs()==[]
    assert reg.get_spec('failed_source').name=='failed_source','A new request retains the registered capability'


def test_managed_orchestration_deadline_still_stops_non_native_work(monkeypatch):
    from types import SimpleNamespace
    clock=[0.0];calls=[];eligible=code_mode._eligible
    monkeypatch.setattr(code_mode,'time',SimpleNamespace(monotonic=lambda:clock[0]))
    def native(state,blocks):
        calls.append(blocks[0]['id']);clock[0]+=100
        return execute(state,blocks)
    def slow_host(registry,name,state=None):
        if calls:clock[0]+=91
        return eligible(registry,name,state)
    monkeypatch.setattr(code_mode,'_eligible',slow_host)
    result=run("call_tool('read', {})\ndescribe_tool('read')\ndescribe_tool('read')",
        monkeypatch,executor=native)['tool_results'][-1]
    assert result.status=='failed' and result.output['reason']=='code_mode_deadline'
    assert len(calls)==1


@pytest.mark.parametrize('code', ["open('/etc/passwd').read()", "import socket\nsocket.socket()", 'while True: pass'])
def test_real_sandbox_refuses_host_access_and_infinite_execution(code, monkeypatch):
    started = time.monotonic()
    result = run(code, monkeypatch)['tool_results'][-1]
    assert result.status == 'failed'
    assert result.output['executed_calls'] == 0
    assert time.monotonic() - started < 8


def test_denial_cannot_be_caught_to_continue_reads(monkeypatch):
    calls = []
    def deny(state, content):
        calls.append(content)
        return {'tool_results': [ToolResult('denied', 'read', 'denied', {'reason': 'approval_required'})]}
    update = run("for x in range(3):\n    try:\n        call_tool('read', {})\n    except Exception:\n        pass\n42", monkeypatch, executor=deny)
    assert len(calls) == 1
    assert update['tool_results'][-1].status == 'failed'
    assert update['tool_results'][0].output['reason'] == 'approval_required'


def test_disable_switch_prevents_stale_program_selection(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE', 'off')
    update = code_mode.execute_if_selected({'messages': []}, [{'type': 'tool_use',
        'name': code_mode.TOOL_NAME, 'id': 'old', 'input': {'code': '1', 'tool_names': ['read']}}],
        execute=lambda *_: pytest.fail('unexpected execution'))
    assert update['tool_results'][-1].output['reason'] == 'code_mode_unavailable'


def test_worker_boots_with_installed_binary():
    assert code_mode._run_python('1 + 2', lambda *_: None, lambda *_: None) == 3


def test_call_budget_stops_without_replaying_reads(monkeypatch):
    update = run("for _ in range(20):\n    call_tool('read', {})", monkeypatch)
    result = update['tool_results'][-1]
    assert result.status == 'failed' and result.output['reason'] == 'code_mode_call_limit'
    assert len(result.output['reads']) == code_mode.MAX_CALLS


def test_partial_evidence_survives_python_error(monkeypatch):
    update = run("call_tool('read', {})\n1/0", monkeypatch)
    assert update['tool_results'][0].status == 'completed'
    assert update['tool_results'][-1].status == 'failed'
    assert update['tool_results'][-1].output['automatic_replay'] is False


def test_program_can_handle_read_failure_and_branch_without_model_reentry(monkeypatch):
    called = []
    def native(state, blocks):
        b = blocks[0]; called.append(b['input']['source'])
        result = ToolResult(b['id'], 'read', 'failed' if len(called) == 1 else 'completed',
            {'failure_reason':'source_error'} if len(called) == 1 else {'number':37})
        return {'tool_results':[*state.get('tool_results', []), result]}
    update = run("r = call_tool('read', {'source':'a'}, on_error='return')\n"
        "if r['status'] == 'failed':\n    r = call_tool('read', {'source':'b'})\n"
        "r['output']['number']", monkeypatch, executor=native)
    result = update['tool_results'][-1]
    assert result.status == 'completed' and result.output['value'] == 37
    assert called == ['a','b'] and len(result.output['failures']) == 1


@pytest.mark.parametrize('status,output', [('denied',{}), ('failed',{})])
def test_error_return_never_continues_denial_or_uncertain_write(monkeypatch,status,output):
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=registry(); state={'tool_registry':reg,'messages':[]};calls=[]
    code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    def native(state,blocks):
        calls.append(blocks[0])
        return {'tool_results':[ToolResult('native','write',status,output)]}
    update=code_mode.execute_if_selected(state,[{'type':'tool_use','name':'code_mode','id':'p',
        'input':{'tool_names':['write','read'],'code':
            "call_tool('write', {}, on_error='return')\ncall_tool('read', {})"}}],execute=native)
    assert len(calls)==1 and update['tool_results'][-1].status=='failed'


def test_error_return_has_a_bounded_failure_budget(monkeypatch):
    calls=[]
    def native(state,blocks):
        calls.append(blocks[0])
        return {'tool_results':[*state.get('tool_results',[]),ToolResult(str(len(calls)),'read','failed',{})]}
    result=run("for _ in range(16):\n    call_tool('read', {}, on_error='return')",monkeypatch,executor=native)['tool_results'][-1]
    assert len(calls)==code_mode.MAX_HANDLED_FAILURES+1
    assert result.status=='failed' and result.output['reason']=='code_mode_operation_stopped'


def test_shared_request_retains_heap_across_recovery_then_closes(monkeypatch):
    from nullion.code_mode_sessions import SessionJournal
    from nullion.chat_operator import _registry_with_request_retry_evidence
    from nullion.agent_orchestrator import TurnResult
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=registry();state={'tool_registry':reg,'messages':[],'principal_id':'p',
        'conversation_id':'c','user_message':'確認'}
    code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    @code_mode.request_session_scope
    def request():
        first=code_mode.execute_if_selected(state,[{'type':'tool_use','name':'code_mode','id':'one',
            'input':{'tool_names':[],'code':'saved = 37'}}],execute=execute)
        code_mode.finish_request(first)
        identity=first['code_mode_session_id']
        result=TurnResult('turn-one','partial',tool_results=first['tool_results'],code_mode_session_id=identity)
        retry=_registry_with_request_retry_evidence(reg,result=result,conversation_id='c',principal_id='p',user_message='確認')
        assert retry.turn_continuation_evidence.code_mode_session_id==identity
        second=code_mode.execute_if_selected({**first,'code_mode_session_id':retry.turn_continuation_evidence.code_mode_session_id},
            [{'type':'tool_use','name':'code_mode','id':'two','input':{'tool_names':[],'code':'saved + 5'}}],execute=execute)
        assert second['tool_results'][-1].output['value']==42
        code_mode.finish_request(second)
        assert SessionJournal(second).read(identity)['phase']=='idle'
        return second,identity
    final,identity=request()
    assert SessionJournal(final).read(identity)['phase']=='closed'


def test_session_follows_first_native_checkpoint_without_losing_heap(tmp_path,monkeypatch):
    from nullion.runtime_store import RuntimeStore
    from nullion.code_mode_sessions import SessionJournal
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    store=RuntimeStore();reg=registry()
    state={'tool_registry':reg,'messages':[],'runtime_store':store,'principal_id':'p',
        'conversation_id':'c','user_message':'確認'}
    code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    def native(current,blocks):
        store._memory_checkpoint_path=tmp_path/'runtime.db'
        return execute(current,blocks)
    first=code_mode.execute_if_selected(state,[{'type':'tool_use','name':'code_mode','id':'save',
        'input':{'tool_names':['read'],'code':"saved=37\ncall_tool('read', {})"}}],execute=native)
    second=code_mode.execute_if_selected(first,[{'type':'tool_use','name':'code_mode','id':'use',
        'input':{'tool_names':[],'code':'saved+5'}}],execute=native)
    assert second['tool_results'][-1].output['value']==42,second['tool_results'][-1]
    assert not store._code_mode_sessions
    assert SessionJournal(second).read(second['code_mode_session_id'])['phase']=='idle'


def test_repair_capabilities_in_code_diagnostics_are_not_completion_obligations():
    from types import SimpleNamespace
    from nullion.chat_operator import _turn_result_schema_followup_tool_names
    native=ToolResult('n','read','failed',{'recovery':{'required_tool_names':['read']}},recovered_by=('fixed',))
    composed=ToolResult('p','code_mode','failed',{'failures':[{'required_tool_names':['browser_screenshot','write']}]})
    assert _turn_result_schema_followup_tool_names(SimpleNamespace(tool_results=[native,composed]))==()


@pytest.mark.parametrize('status,output,ready',[
    ('failed',{'failure_reason':'source_error'},True),
    ('failed',{'failure_reason':'source_timeout'},True),
    ('failed',{'reason':'approval_required','executed':False},False),
    ('denied',{'reason':'boundary_policy_denied'},False),
    ('failed',{'reason':'invalid_tool_arguments','executed':False},False),
])
def test_read_first_precondition_accepts_native_source_failure_only(status,output,ready):
    reg=registry();reg.get_spec('write').code_mode_observation_tools=('read',)
    result=ToolResult('observed','read',status,output)
    assert code_mode.observation_ready(reg,'write',[result]) is ready


def test_cancel_before_first_read(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE', 'read_only')
    reg = registry()
    state = {'tool_registry': reg, 'messages': [], 'cancellation_checker': lambda: True}
    code_mode.offer_code_mode(reg.list_tool_definitions(), reg, state=state)
    update = code_mode.execute_if_selected(state, [{'type':'tool_use', 'name':code_mode.TOOL_NAME,
        'id':'cancel', 'input':{'tool_names':['read'], 'code':"call_tool('read', {})"}}],
        execute=lambda *_: pytest.fail('cancelled read executed'))
    assert update['tool_results'][-1].output['reason'] == 'request_cancelled'


def test_dependency_absent_keeps_direct_tools(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE', 'read_only')
    monkeypatch.setattr(code_mode.importlib.util, 'find_spec', lambda _: None)
    definitions = [{'name': 'read'}]
    assert code_mode.offer_code_mode(definitions, registry()) is definitions


def test_off_preserves_existing_executor_without_workers(monkeypatch):
    from nullion.code_mode import execute_if_selected as redirect
    monkeypatch.setenv('NULLION_CODE_MODE', 'off')
    monkeypatch.setattr(code_mode, '_run_python', lambda *_: pytest.fail('worker started'))
    assert redirect({}, [{'type':'tool_use','name':'read','id':'x','input':{}}], execute=execute) is None


@pytest.mark.asyncio
async def test_langchain_read_uses_session_and_records_both_receipts(monkeypatch):
    from nullion.langchain_adapters import nullion_tools_as_langchain_tools
    monkeypatch.setenv('NULLION_CODE_MODE', 'read_only')
    reg = registry(); observed = []
    tools = nullion_tools_as_langchain_tools(reg, allowed_tools=['read'], principal_id='test',
        cleanup_scope='test', tool_result_callback=observed.append)
    assert [t.name for t in tools] == [code_mode.TOOL_NAME]
    import json
    result = json.loads(await tools[0].ainvoke({'tool_names':['read'], 'code':
        "from html import escape\nassert escape('<7>')=='&lt;7&gt;'\ncall_tool('read', {})['output']"}))
    assert result['output']['value'] == {'number':7}
    assert [r.tool_name for r in observed] == ['read', code_mode.TOOL_NAME]
    assert observed[-1].status == 'completed'
    assert result['output']['host_calls']=={'html.escape':1}
    data_only=json.loads(await tools[0].ainvoke({'tool_names':[], 'code':"escape('<8>')"}))
    assert data_only['output']['value']=='&lt;8&gt;'
    assert [r.tool_name for r in observed].count('read')==1


@pytest.mark.asyncio
async def test_langchain_uses_the_same_inline_read_recovery(monkeypatch):
    import json
    from nullion.langchain_adapters import nullion_tools_as_langchain_tools
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=ToolRegistry();calls=[]
    def handler(i):
        calls.append(i)
        return ToolResult(i.invocation_id,i.tool_name,'failed' if len(calls)==1 else 'completed',{'value':37})
    reg.register(ToolSpec('read_record','Read record',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,
        input_schema={'type':'object'}),handler)
    tools=nullion_tools_as_langchain_tools(reg,allowed_tools=['read_record'],principal_id='test',cleanup_scope='inline')
    result=json.loads(await tools[0].ainvoke({'tool_names':['read_record'],
        'code':"r=call_tool('read_record', {'source':'a'}, on_error='return')\n"
            "if r['status']=='failed':\n    r=call_tool('read_record', {'source':'b'})\nr['output']['value']"}))
    assert result['status']=='completed' and result['output']['value']==37 and len(calls)==2


def test_new_layer_exposes_program_instead_of_individual_read_tools(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE', 'read_only')
    reg = registry()
    definitions = reg.list_tool_definitions()
    state = {}
    offered = code_mode.offer_code_mode(definitions, reg, state=state)
    assert {d['name'] for d in offered} == {'write','legacy',code_mode.TOOL_NAME}
    assert state['completion_requirement_contract_cache']['_code_mode_schemas']['read']['input_schema'] == {'type':'object'}
    monkeypatch.setenv('NULLION_CODE_MODE', 'off')
    assert code_mode.offer_code_mode(definitions, reg, state=state) is definitions
    assert '_code_mode_schemas' not in state['completion_requirement_contract_cache']


@pytest.mark.asyncio
async def test_langchain_program_composes_scoped_reads_and_cannot_expand_scope(monkeypatch):
    import json
    from nullion.langchain_adapters import nullion_tools_as_langchain_tools
    monkeypatch.setenv('NULLION_CODE_MODE', 'read_only')
    reg = registry(); observed = []
    reg.register(ToolSpec('other_read','other read',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,
        input_schema={'type':'object'}),lambda i:ToolResult(i.invocation_id,i.tool_name,'completed',{'number':8}))
    tools = nullion_tools_as_langchain_tools(reg, allowed_tools=['read','other_read'], principal_id='test',
        cleanup_scope='test', tool_result_callback=observed.append)
    assert [t.name for t in tools] == [code_mode.TOOL_NAME]
    result = json.loads(await tools[0].ainvoke({'tool_names':['read','other_read'],
        'code':"a = call_tool('read', {})\nb = call_tool('other_read', {})\na['output']['number'] + b['output']['number']"}))
    assert result['output']['value'] == 15
    assert [r.tool_name for r in observed] == ['read','other_read',code_mode.TOOL_NAME]
    denied = json.loads(await tools[0].ainvoke({'tool_names':['write'], 'code':"call_tool('write', {})"}))
    assert denied['status'] == 'failed'
    assert not any(r.tool_name == 'write' for r in observed)


@pytest.mark.asyncio
@pytest.mark.parametrize('mode',['read_only','on'])
async def test_composed_langchain_read_cannot_fallback_to_write(monkeypatch,mode):
    import json
    from nullion.langchain_adapters import nullion_tools_as_langchain_tools
    monkeypatch.setenv('NULLION_CODE_MODE',mode)
    called = []
    class Registry(ToolRegistry):
        def list_tool_definitions(self):
            rows = super().list_tool_definitions()
            for row in rows:
                if row['name']=='read': row['fallback_tools']=['write']
            return rows
    reg = Registry()
    reg.register(ToolSpec('read','read',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,
        input_schema={'type':'object'}),lambda i:ToolResult(i.invocation_id,i.tool_name,'failed',{}))
    reg.register(ToolSpec('write','write',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,5,
        input_schema={'type':'object'}),lambda i:called.append(i) or ToolResult(i.invocation_id,i.tool_name,'completed',{}))
    tools = nullion_tools_as_langchain_tools(reg,allowed_tools=['read','write'],principal_id='test',cleanup_scope='test')
    program = next(t for t in tools if t.name==code_mode.TOOL_NAME)
    result = json.loads(await program.ainvoke({'tool_names':['read'],'code':"call_tool('read', {})"}))
    assert result['status']=='failed' and not called


def test_compact_schema_preserves_executable_properties():
    schema = {'title':'Tool docs','description':'Verbose docs','type':'object',
        'required':['description'], 'properties':{'description':{'type':'string','description':'Argument help'},
            'title':{'type':'string','enum':['a.b']}}}
    assert code_mode._compact_schema(schema) == {'type':'object','required':['description'],
        'properties':{'description':{'type':'string'},'title':{'type':'string','enum':['a.b']}}}


def test_native_ui_opt_in_does_not_grant_account_mutation_or_arbitrary_execution(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE','read_only')
    reg=ToolRegistry()
    for name,effect in [('ui',ToolSideEffectClass.WRITE),('account',ToolSideEffectClass.ACCOUNT_WRITE),
                        ('exec',ToolSideEffectClass.DANGEROUS_EXEC)]:
        reg.register(ToolSpec(name,name,ToolRiskLevel.LOW,effect,False,5,input_schema={'type':'object'},
            code_mode_allowed=True,code_mode_ui_interaction=True),
            lambda i:ToolResult(i.invocation_id,i.tool_name,'completed',{}))
    assert code_mode._eligible(reg,'ui')
    assert not code_mode._eligible(reg,'account') and not code_mode._eligible(reg,'exec')
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    assert code_mode._eligible(reg,'account') and code_mode._eligible(reg,'exec')


def test_all_tools_mode_includes_scoped_writes_and_preserves_session(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE', 'on')
    reg = registry()
    state = {'tool_registry':reg, 'principal_id':'fixture', 'conversation_id':'request-one', 'user_message':'確認', 'messages':[]}
    definitions = code_mode.offer_code_mode(reg.list_tool_definitions(), reg, state=state)
    program = next(d for d in definitions if d['name']==code_mode.TOOL_NAME)
    assert program['input_schema']['properties']['tool_names']['items']['enum']==['read','write']
    def feed(identity, code, names):
        nonlocal state
        state = code_mode.execute_if_selected(state, [{'type':'tool_use','id':identity,'name':code_mode.TOOL_NAME,
            'input':{'code':code, 'tool_names':names}}], execute=execute)
        return state['tool_results'][-1]
    first = feed('first', "data = call_tool('read', {})\nremembered = data['output']['number']\nremembered", ['read'])
    assert first.output['value']==7, first.output
    identity = first.output['session_id']
    second = feed('second', "saved = call_tool('write', {'number':remembered})\nremembered + saved['output']['number']", ['write'])
    assert second.output['value']==14, second.output
    assert second.output['session_id']==identity
    assert second.output['operations'][0]['operation_kind']=='write'
    assert state['invocation_metadata'][second.invocation_id]['tool_use_id']=='second'
    duplicate = feed('second', "saved = call_tool('write', {'number':remembered})\nremembered + saved['output']['number']", ['write'])
    assert duplicate.status=='failed'
    assert len([r for r in state['tool_results'] if r.tool_name=='write'])==1


def test_nested_native_cache_replacement_reaches_outer_state(monkeypatch):
    def native(state, blocks):
        state['completed_read_results'] = {}
        state['action_review_cache'] = {'review':'native'}
        return execute(state, blocks)
    update = run("call_tool('read', {})", monkeypatch, executor=native)
    assert update['completed_read_results']=={}
    assert update['action_review_cache']=={'review':'native'}


def test_large_program_compaction_keeps_exact_receipts():
    long_id='opaque-'+('x'*700)
    output={'value':{'body':'x'*20000}, 'operations':[{'invocation_id':long_id,'status':'completed'}],
            'reason':'code_mode_completed','automatic_replay':False}
    compact=code_mode.compact_output(output,max_chars=2000)
    assert compact['operations']==output['operations']
    assert compact['value']['inspection_invocation_ids']==[long_id]


@pytest.mark.parametrize('expression', ["{'report':result['output'],'large':'x'*30000}", 'None'])
def test_large_program_keeps_native_facts_separate_from_arbitrary_value(monkeypatch, expression):
    import json
    native={'result_kind':'source_observations', 'report_status':'partial',
        'observations':[{'label':'Selected value','value':'37','quote':'37',
            'source_url':'https://example.test', 'control_state':{'tag':'SELECT','value':'37',
                'options':[{'value':str(index),'label':'option-'+str(index),'selected':index==37}
                    for index in range(200)]}},
            {'label':'Enabled action','value':'6:00 PM','quote':'For 2 people, 6:00 PM',
                'source_url':'https://example.test','control_state':{'tag':'A','disabled':False}}],
        'captured_documents':[{'main_document_text':'x'*10000,'source_url':'https://example.test'}]}
    def native_executor(state, content):
        block=content[0]
        return {'tool_results':[*state.get('tool_results',[]), ToolResult(block['id'],'read','completed',native)]}
    result=run("result = call_tool('read', {})\n"+expression, monkeypatch, executor=native_executor)['tool_results'][-1]
    for _ in range(3):
        compact=code_mode.compact_output(result.output,max_chars=5000)
        # Native ownership and exact enabled-action state survive. A returned
        # Python object is never the source of these host-owned observations.
        observations=compact.get('native_observations') or compact['value']['observations']
        evidence=observations[0]
        assert evidence['invocation_id']==result.output['operations'][0]['invocation_id']
        assert evidence['output']['observations'][-1]['value']=='6:00 PM'
        assert evidence['output']['observations'][-1]['control_state']['disabled'] is False
        assert len(json.dumps(compact)) < 5000
        result.output=compact


def test_session_journal_claims_and_owner_are_durable(tmp_path):
    from nullion.code_mode_sessions import SessionJournal
    from types import SimpleNamespace
    path=tmp_path/'runtime.db'
    state={'principal_id':'p','conversation_id':'c','user_message':'確認','runtime_store':SimpleNamespace(_memory_checkpoint_path=path)}
    journal=SessionJournal(state);record=journal.create(); stale=journal.read(record['id'])
    journal.save(record,phase='running')
    with pytest.raises(RuntimeError):journal.save(stale,phase='running')
    restarted=SessionJournal({**state,'runtime_store':SimpleNamespace(_memory_checkpoint_path=path)})
    assert restarted.read(record['id'])['phase']=='running'
    with pytest.raises(ValueError):SessionJournal({**state,'principal_id':'another'}).read(record['id'])


@pytest.mark.asyncio
async def test_async_unknown_write_outcome_is_never_retried(monkeypatch):
    from nullion.langchain_adapters import nullion_tools_as_langchain_tools
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    monkeypatch.setenv('NULLION_LANGCHAIN_TOOL_RETRIES','4')
    calls=[]
    reg=ToolRegistry()
    def uncertain(invocation):
        calls.append(invocation)
        raise ConnectionError('Lost acknowledgement after write')
    reg.register(ToolSpec('write','write',ToolRiskLevel.LOW,ToolSideEffectClass.WRITE,False,5,input_schema={'type':'object'}),uncertain)
    tool=nullion_tools_as_langchain_tools(reg,allowed_tools=['write'],principal_id='p',cleanup_scope='t')[0]
    import json
    result=json.loads(await tool.ainvoke({'tool_names':['write'],'code':"call_tool('write', {})"}))
    assert result['status']=='failed' and len(calls)==1


def test_python_repair_continues_globals_and_never_replays_completed_write(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=registry();state={'tool_registry':reg,'messages':[]}
    code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    state=code_mode.execute_if_selected(state,[{'type':'tool_use','id':'broken','name':code_mode.TOOL_NAME,
        'input':{'tool_names':['write'],'code':"saved = call_tool('write', {})\n1/0"}}],execute=execute)
    assert state['tool_results'][-1].status=='failed'
    assert state['tool_results'][-1].output['reason']=='code_mode_python_error'
    assert 'ZeroDivisionError' in state['tool_results'][-1].output['execution_error']['message']
    repaired=code_mode.execute_if_selected(state,[{'type':'tool_use','id':'corrected','name':code_mode.TOOL_NAME,
        'input':{'tool_names':['write'],'code':"saved['output']['number']"}}],execute=execute)
    assert repaired['tool_results'][-1].output['value']==7
    assert len([r for r in repaired['tool_results'] if r.tool_name=='write'])==1


def test_failed_native_read_does_not_poison_session_or_execute_rest_of_program(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=registry();calls=[]
    def flaky(state,blocks):
        calls.append(blocks[0]['input'])
        if blocks[0]['input'].get('fail'):
            return {'tool_results':[*state.get('tool_results',[]),ToolResult('failed-read','read','failed',{'executed':False})]}
        return execute(state,blocks)
    state={'tool_registry':reg,'messages':[]}
    code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    state=code_mode.execute_if_selected(state,[{'type':'tool_use','name':code_mode.TOOL_NAME,'id':'initial',
        'input':{'tool_names':['read','write'],'code':"kept = 37\nfailed = call_tool('read', {'fail':True})\ncall_tool('write', {})"}}],execute=flaky)
    assert state['tool_results'][-1].status=='failed'
    assert calls==[{'fail':True}]
    repaired=code_mode.execute_if_selected(state,[{'type':'tool_use','name':code_mode.TOOL_NAME,'id':'repair',
        'input':{'tool_names':['read'],'code':"value = call_tool('read', {})\nkept + value['output']['number']"}}],execute=flaky)
    assert repaired['tool_results'][-1].output['value']==44
    assert calls==[{'fail':True},{}]


def test_failed_write_cannot_be_repeated_by_a_repair_program(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=registry();calls=[]
    def uncertain(state,blocks):
        calls.append(blocks[0]['name'])
        return {'tool_results':[*state.get('tool_results',[]),ToolResult('uncertain','write','failed',{})]}
    state={'tool_registry':reg,'messages':[]}
    code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    def block(identity):return {'type':'tool_use','name':code_mode.TOOL_NAME,'id':identity,
        'input':{'tool_names':['write'],'code':"call_tool('write', {})"}}
    state=code_mode.execute_if_selected(state,[block('attempt')],execute=uncertain)
    repaired=code_mode.execute_if_selected(state,[block('retry')],execute=uncertain)
    assert repaired['tool_results'][-1].output['reason']=='code_mode_unknown_write_outcome'
    assert calls==['write']


def test_scope_recovery_receipt_cannot_mask_failed_native_operation(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=registry()
    reg.register(ToolSpec('制御','Control',ToolRiskLevel.LOW,ToolSideEffectClass.READ,False,5,
        code_mode_allowed=False,input_schema={'type':'object'}),lambda _:None)
    calls=[]
    def recover_scope(state,blocks):
        calls.append(blocks[0]['name'])
        state.setdefault('invocation_metadata',{})['primary']={'tool_use_id':blocks[0]['id']}
        return {'tool_results':[*state.get('tool_results',[]),
            ToolResult('primary','read','failed',{'executed':False},recovered_by=('control',)),
            ToolResult('control','制御','completed',{'message':'Scope changed'})]}
    state={'tool_registry':reg,'messages':[]}
    code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    result=code_mode.execute_if_selected(state,[{'type':'tool_use','name':code_mode.TOOL_NAME,'id':'program',
        'input':{'tool_names':['read','write'],'code':"call_tool('read', {})\ncall_tool('write', {})"}}],execute=recover_scope)
    summary=result['tool_results'][-1]
    assert summary.status=='failed' and summary.output['reason']=='code_mode_operation_stopped'
    assert calls==['read']


def test_program_without_final_expression_returns_native_evidence(monkeypatch):
    result=run("saved = call_tool('read', {})",monkeypatch)['tool_results'][-1]
    assert result.status=='completed',result.output
    observation=result.output['value']['observations'][0]
    assert observation['output']['number']==7
    assert observation['inspection_reference']=={'invocation_id':observation['invocation_id'],'json_pointer':''}


@pytest.mark.parametrize('program,detail',[
    ('import unavailable_example_library','unavailable_example_library'),
    ('missing_value + 1','missing_value'),
    ('if True\n    pass','Expected `:`'),
])
def test_python_errors_report_the_actual_failure_without_host_tracebacks(monkeypatch,program,detail):
    result=run(program,monkeypatch)['tool_results'][-1]
    assert result.status=='failed' and result.output['reason']=='code_mode_python_error',result.output
    diagnostic=result.output['execution_error']
    assert detail in diagnostic['message'] and len(diagnostic['message'])<=2000
    assert 'site-packages' not in diagnostic['message']
    assert not result.output['operations']


def test_syntax_failure_identifies_unexecuted_program_and_exact_source_line(monkeypatch):
    result=run("new_value = 37\ntext = 'unterminated",monkeypatch)['tool_results'][-1]
    error=result.output['execution_error']
    assert error['program_executed'] is False and error['cpython_syntax_valid'] is False
    assert error['source_line']['line']==2 and 'unterminated' in error['source_line']['text']
    assert 'No variables from this program were created' in error['recovery']


def test_failed_native_operation_returns_its_error_and_typed_repair(monkeypatch):
    def failed(state,blocks):
        return {'tool_results':[ToolResult('failed-native','read','failed',
            {'reason':'tool_precondition_unmet','executed':False,
             'recovery':{'kind':'repair_precondition','retry_tool_name':'read','required_tool_names':['legacy']}},
            'The current page must be loaded before capture.')]}
    result=run("call_tool('read', {})",monkeypatch,executor=failed)['tool_results'][-1]
    failure=result.output['failures'][0]
    assert failure['error']=='The current page must be loaded before capture.'
    assert failure['output']['executed'] is False
    assert failure['required_tool_names']==['legacy'] and failure['repair_kind']=='repair_precondition'
    assert failure['inspection_reference']=={'invocation_id':'failed-native','json_pointer':''}


def test_observation_guard_uses_the_shared_native_precondition_contract(monkeypatch):
    from nullion.tool_repair_contract import tool_repair_kind
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=registry()
    reg.get_spec('write').code_mode_observation_tools=('read',)
    result=code_mode.observation_denial(reg,'write','guarded',[])
    assert tool_repair_kind(result)=='repair_precondition'
    assert result.output['recovery']['required_tool_names']==['read']


@pytest.mark.parametrize('source',['東京 & café','مرحبا بالعالم','Hola mundo'])
def test_unique_host_helper_name_resolves_without_an_import(monkeypatch,source):
    from urllib.parse import quote_plus
    result=run(f"saved=call_tool('read', {{}})\nquote_plus({source!r})",monkeypatch)['tool_results'][-1]
    assert result.status=='completed',result.output
    assert result.output['value']==quote_plus(source)
    assert result.output['host_calls']=={'urllib.parse.quote_plus':1}
    assert result.output['executed_calls']==1


@pytest.mark.parametrize('source',['東京 & café','مرحبا بالعالم','Hola mundo'])
def test_host_modules_work_inside_the_real_program_driver(monkeypatch,source):
    from html import escape
    from urllib.parse import quote
    program=("import urllib.parse\nfrom html import escape\nfrom textwrap import dedent\n"
        f"source = {source!r}\n"
        "{'url':urllib.parse.quote(source,safe=''),'html':escape(source),"
        "'text':dedent('  first\\n  second'),'schema':describe_module('urllib.parse')}")
    result=run(program,monkeypatch)['tool_results'][-1]
    assert result.status=='completed',result.output
    assert result.output['value']['url']==quote(source,safe='')
    assert result.output['value']['html']==escape(source)
    assert result.output['value']['text']=='first\nsecond'
    assert 'urlencode' in result.output['value']['schema']['functions']
    assert result.output['host_calls']=={'urllib.parse.quote':1,'html.escape':1,'textwrap.dedent':1}
    assert not result.output['operations']


def test_host_module_bindings_survive_a_new_program(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=registry();state={'tool_registry':reg,'messages':[]}
    code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    def block(identity,code):return {'type':'tool_use','name':'code_mode','id':identity,
        'input':{'tool_names':['read'],'code':code}}
    state=code_mode.execute_if_selected(state,[block('save',"from urllib.parse import quote\nencoded=quote('saved value',safe='')")],execute=execute)
    assert state['tool_results'][-1].status=='completed',state['tool_results'][-1].output
    restored=code_mode.execute_if_selected(state,[block('reuse',"encoded + '/' + quote('次',safe='')")],execute=execute)
    assert restored['tool_results'][-1].output['value']=='saved%20value/%E6%AC%A1'
    assert restored['tool_results'][-1].output['host_calls']=={'urllib.parse.quote':1}


_HELPER_CASES = [
    ('urllib.parse','quote',('東京 & café',),{'safe':''}),
    ('urllib.parse','quote_plus',('東京 & café',),{}),
    ('urllib.parse','quote_from_bytes',(b'a b',),{}),
    ('urllib.parse','unquote',('caf%C3%A9',),{}),
    ('urllib.parse','unquote_plus',('a+b',),{}),
    ('urllib.parse','unquote_to_bytes',('caf%C3%A9',),{}),
    ('urllib.parse','urlencode',({'q':'a b'},),{}),
    ('urllib.parse','parse_qs',('q=a+b',),{}),
    ('urllib.parse','parse_qsl',('q=a+b',),{}),
    ('urllib.parse','urljoin',('https://example.test/a/','b'),{}),
    ('html','escape',('<東京 & café>',),{}),
    ('html','unescape',('&lt;東京&gt;',),{}),
    ('textwrap','dedent',('  東京\n  café',),{}),
    ('textwrap','wrap',('one two three',),{'width':4}),
    ('textwrap','fill',('one two three',),{'width':4}),
    ('textwrap','shorten',('one two three',),{'width':9}),
    ('shlex','quote',('a b',),{}),
    ('shlex','join',(['a b','c'],),{}),
]


@pytest.mark.parametrize('module,name,args,kwargs',_HELPER_CASES)
@pytest.mark.parametrize('binding',['module','alias','restored'])
def test_all_approved_helpers_keep_exact_behavior_across_imports_and_restoration(monkeypatch,module,name,args,kwargs,binding):
    from importlib import import_module
    arguments=', '.join([*(repr(a) for a in args),*(f'{k}={v!r}' for k,v in kwargs.items())])
    expected=getattr(import_module(module),name)(*args,**kwargs)
    if isinstance(expected,bytes):
        import base64
        expected={'type':'bytes','encoding':'base64','data':base64.b64encode(expected).decode('ascii'),'byte_length':len(expected)}
    prefix=f'import {module}' if binding=='module' else f'from {module} import {name} as helper'
    expression=f'{module}.{name}({arguments})' if binding=='module' else f'helper({arguments})'
    if binding=='restored':
        monkeypatch.setenv('NULLION_CODE_MODE','on')
        reg=registry();state={'tool_registry':reg,'messages':[],'principal_id':'test'}
        code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
        for identity,code in [('save',prefix+"\ncall_tool('read', {})"),('restore',expression)]:
            state=code_mode.execute_if_selected(state,[{'type':'tool_use','name':'code_mode','id':identity,
                'input':{'tool_names':['read'],'code':code}}],execute=execute)
            assert state['tool_results'][-1].status=='completed',state['tool_results'][-1].output
        result=state['tool_results'][-1]
        assert len([r for r in state['tool_results'] if r.tool_name=='read'])==1
    else:
        result=run(prefix+"\ncall_tool('read', {})\n"+expression,monkeypatch)['tool_results'][-1]
    assert result.status=='completed',result.output
    assert result.output['value']==expected
    assert result.output['host_calls']=={module+'.'+name:1}


@pytest.mark.parametrize('module,name,args,kwargs',[row for row in _HELPER_CASES if row[1]!='quote'])
def test_unique_helper_bindings_use_the_same_bounded_implementation(monkeypatch,module,name,args,kwargs):
    from importlib import import_module
    arguments=', '.join([*(repr(a) for a in args),*(f'{k}={v!r}' for k,v in kwargs.items())])
    result=run(f'{name}({arguments})',monkeypatch)['tool_results'][-1]
    assert result.status=='completed',result.output
    expected=getattr(import_module(module),name)(*args,**kwargs)
    if isinstance(expected,bytes):
        import base64
        expected={'type':'bytes','encoding':'base64','data':base64.b64encode(expected).decode('ascii'),'byte_length':len(expected)}
    assert result.output['value']==expected
    assert result.output['host_calls']=={module+'.'+name:1}


def test_shared_export_names_never_select_an_arbitrary_module(monkeypatch):
    result=run("quote('a b')",monkeypatch)['tool_results'][-1]
    assert result.status=='failed' and result.output['execution_error']['kind']=='NameError'
    assert not result.output['host_calls']


def test_binary_helper_result_does_not_discard_the_session_or_repeat_native_calls(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=registry();state={'tool_registry':reg,'messages':[],'principal_id':'test'}
    code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    for identity,code in [('binary',"call_tool('read', {})\nraw=unquote_to_bytes('%FF%00')\n{'raw':[raw]}"),
                          ('reuse',"list(raw)")]:
        state=code_mode.execute_if_selected(state,[{'type':'tool_use','name':'code_mode','id':identity,
            'input':{'tool_names':['read'],'code':code}}],execute=execute)
        result=state['tool_results'][-1]
        assert result.status=='completed',result.output
        if identity=='binary':
            assert result.output['value']=={'raw':[{'type':'bytes','encoding':'base64','data':'/wA=','byte_length':2}]}
        else:assert result.output['value']==[255,0]
    assert len([r for r in state['tool_results'] if r.tool_name=='read'])==1


@pytest.mark.parametrize('code',[
    "quote_plus('x'*70000)",
    "fill('x',subsequent_indent='y'*129)",
    "[escape('x') for _ in range(129)]",
])
def test_direct_helper_bindings_preserve_input_and_call_limits(monkeypatch,code):
    result=run(code,monkeypatch)['tool_results'][-1]
    assert result.status=='failed' and not result.output['operations']


@pytest.mark.parametrize('program',[
    "from urllib import request",
    "import subprocess",
    "import os\nos.system('true')",
    "import urllib.parse\nurllib.parse.os.system('true')",
    "import shlex\nshlex.os.system('true')",
    "from shlex import split",
])
def test_host_modules_do_not_expose_imports_or_effect_primitives(monkeypatch,program):
    result=run(program,monkeypatch)['tool_results'][-1]
    assert result.status=='failed' and not result.output['operations'],result.output


@pytest.mark.parametrize('program',[
    "import html\nhtml.escape('x'*70000)",
    "import textwrap\ntextwrap.fill('x',subsequent_indent='y'*129)",
    "import html\n[html.escape('x') for _ in range(129)]",
    "import shlex\nshlex.quote('x'*70000)",
])
def test_host_helper_resource_limits_are_enforced(monkeypatch,program):
    result=run(program,monkeypatch)['tool_results'][-1]
    assert result.status=='failed' and not result.output['operations'],result.output


def test_unsupported_import_recovery_budget_survives_corrected_programs(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=registry();state={'tool_registry':reg,'messages':[]}
    code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    for index in range(2):
        state=code_mode.execute_if_selected(state,[{'type':'tool_use','name':'code_mode','id':f'attempt-{index}',
            'input':{'tool_names':['read'],'code':f'import unavailable_library_{index}'}}],execute=execute)
    result=state['tool_results'][-1]
    assert result.output['reason']=='code_mode_import_recovery_exhausted',result.output
    assert result.output['execution_error']['kind']=='module_unavailable'
    assert result.output['execution_error']['unsupported_imports']==2
    assert 'urllib.parse' in result.output['execution_error']['host_modules']


@pytest.mark.parametrize('program',["while True: pass","huge='x'*100_000_000"])
def test_resource_errors_discard_heap_and_preserve_completed_native_receipts(monkeypatch,program):
    from nullion.code_mode_sessions import SessionJournal
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=registry();state={'tool_registry':reg,'messages':[]}
    code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    state=code_mode.execute_if_selected(state,[{'type':'tool_use','name':'code_mode','id':'resource-error',
        'input':{'tool_names':['write'],'code':"saved=call_tool('write', {})\n"+program}}],execute=execute)
    result=state['tool_results'][-1]
    assert result.output['reason']=='code_mode_resource_limit',result.output
    assert result.output['execution_error']['session_usable'] is False
    frames=result.output['execution_error']['frames']
    if frames:assert frames[-1]['line']==2
    assert len([r for r in state['tool_results'] if r.tool_name=='write'])==1
    record=SessionJournal(state).read(state['code_mode_session_id'])
    assert record['phase']=='blocked' and record['snapshot'] is None


def test_unknown_host_function_is_a_correctable_python_error(monkeypatch):
    result=run('undeclared_function()',monkeypatch)['tool_results'][-1]
    assert result.output['reason']=='code_mode_python_error',result.output
    assert result.output['execution_error']['kind']=='NameError'
    assert 'undeclared_function' in result.output['execution_error']['message']
    assert result.output['execution_error']['session_usable'] is True


def test_repeated_python_errors_have_a_durable_correction_limit(monkeypatch):
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=registry();state={'tool_registry':reg,'messages':[]}
    code_mode.offer_code_mode(reg.list_tool_definitions(),reg,state=state)
    for index in range(3):
        state=code_mode.execute_if_selected(state,[{'type':'tool_use','name':'code_mode','id':f'error-{index}',
            'input':{'tool_names':['read'],'code':f'missing_variable_{index}'}}],execute=execute)
    result=state['tool_results'][-1]
    assert result.output['reason']=='code_mode_python_recovery_exhausted',result.output
    assert result.output['execution_error']['remaining_corrections']==0


def test_empty_tool_selection_can_process_saved_values_and_host_helpers(monkeypatch):
    result=run("from urllib.parse import quote\nquote('東京',safe='')",monkeypatch,names=[])['tool_results'][-1]
    assert result.status=='completed' and result.output['value']=='%E6%9D%B1%E4%BA%AC',result.output
    assert not result.output['operations']


def test_empty_tool_selection_cannot_execute_a_native_action(monkeypatch):
    result=run("call_tool('read', {})",monkeypatch,names=[])['tool_results'][-1]
    assert result.status=='failed' and result.output['reason']=='code_mode_scope_denied'
    assert not result.output['operations']


def test_invalid_tool_selection_returns_exact_corrective_constraints(monkeypatch):
    from nullion.tool_repair_contract import tool_repair_kind
    result=run('1',monkeypatch,names=['request_tool_scope'])['tool_results'][-1]
    assert result.output['reason']=='invalid_tool_arguments' and tool_repair_kind(result)=='correct_arguments'
    assert result.output['validation_errors'][0]['rejected']==['request_tool_scope']
    assert result.output['validation_errors'][0]['allowed_tool_names']==['read']
    assert result.output['executed'] is False


def test_attaching_persistence_does_not_reset_an_existing_durable_claim(tmp_path):
    import json
    from nullion.runtime_store import RuntimeStore
    from nullion.code_mode_sessions import SessionJournal
    store=RuntimeStore();state={'runtime_store':store,'principal_id':'p','conversation_id':'c','user_message':'確認'}
    journal=SessionJournal(state);stale=journal.create();memory_copy=json.loads(json.dumps(stale))
    store._memory_checkpoint_path=tmp_path/'runtime.db'
    durable=journal.read(stale['id']);journal.save(durable,phase='blocked')
    store._code_mode_sessions[stale['id']]=memory_copy
    assert SessionJournal(state).read(stale['id'])['phase']=='blocked'
    with pytest.raises(RuntimeError,match='already_claimed'):
        SessionJournal(state).save(stale,phase='running')


def test_oversized_native_program_value_keeps_completed_receipts(monkeypatch):
    result=run("saved=call_tool('read', {})\n{'receipt':saved,'padding':'x'*70000}",monkeypatch)['tool_results'][-1]
    assert result.status=='completed' and result.output['reason']=='code_mode_completed'
    assert result.output['value'] is None and result.output['value_limit']['omitted']
    assert result.output['executed_calls']==1
    assert result.output['native_observations'][0]['output']['number']==7


def test_oversized_data_only_value_still_requires_a_smaller_result(monkeypatch):
    result=run("'x'*70000",monkeypatch)['tool_results'][-1]
    assert result.status=='failed' and result.output['reason']=='code_mode_output_limit'


def test_retained_inspection_requires_the_existing_runtime_availability_signal(monkeypatch):
    from nullion.retained_work_context import INSPECT_DEFINITION
    monkeypatch.setenv('NULLION_CODE_MODE','on')
    reg=registry()
    for available in (False,True):
        definitions=code_mode.offer_code_mode([*reg.list_tool_definitions(),INSPECT_DEFINITION],reg,
            state={'retained_inspection_available':available})
        program=next(d for d in definitions if d['name']=='code_mode')
        assert ('inspect_retained_evidence' in program['input_schema']['properties']['tool_names']['items']['enum']) is available


def test_receipt_attribute_error_recovers_from_saved_dictionary_without_repeating_read(monkeypatch):
    first=run("receipt=call_tool('read', {})\nreceipt.output",monkeypatch)
    error=first['tool_results'][-1].output['execution_error']
    assert error['kind']=='AttributeError' and "receipt['output']" in error['recovery']
    second=code_mode.execute_if_selected(first,[{'type':'tool_use','name':'code_mode','id':'fixed-access',
        'input':{'tool_names':[],'code':"receipt['output']['number']"}}],execute=execute)
    assert second['tool_results'][-1].output['value']==7
    assert len([r for r in second['tool_results'] if r.tool_name=='read'])==1


@pytest.mark.parametrize('payload',["quotes ' and \" plus\nline",'東京 & café; $HOME',"$(printf 37) `printf 37`"])
def test_bounded_shell_helpers_match_cpython_without_evaluating_content(monkeypatch,payload):
    import shlex
    result=run('import shlex\nvalue='+repr(payload)+"\n{'arg':shlex.quote(value),'command':shlex.join(['python3','-c',value])}",monkeypatch)['tool_results'][-1]
    assert result.status=='completed',result.output
    assert result.output['value']=={'arg':shlex.quote(payload),'command':shlex.join(['python3','-c',payload])}
    assert result.output['host_calls']=={'shlex.quote':1,'shlex.join':1}
    assert result.output['executed_calls']==0


def test_generator_compatibility_repair_reuses_the_completed_native_read(monkeypatch):
    program="data=call_tool('read', {})['output']['number']\nnext(x for x in [data] if x>0)"
    state=run(program,monkeypatch)
    failure=state['tool_results'][-1]
    assert failure.status=='failed'
    assert failure.output['execution_error']['kind']=='TypeError'
    assert failure.output['execution_error']['python_runtime_note']==code_mode.ITERATOR_COMPATIBILITY_GUIDANCE
    fixed=code_mode.execute_if_selected(state,[{'type':'tool_use','name':'code_mode','id':'repair-generator',
        'input':{'tool_names':[],'code':"next(iter(x for x in [data] if x>0), None)"}}],execute=execute)
    assert fixed['tool_results'][-1].status=='completed',fixed['tool_results'][-1].output
    assert fixed['tool_results'][-1].output['value']==7
    assert len([r for r in fixed['tool_results'] if r.tool_name=='read'])==1


@pytest.mark.parametrize('bad_call', ["call_tool('read')", "call_tool('read', {}, unsupported=True)",
    "call_tool('read', {}, on_error='ignore')"])
def test_call_tool_signature_error_preserves_session_and_completed_receipts(monkeypatch, bad_call):
    first = run("saved=call_tool('read', {})['output']['number']\n"+bad_call, monkeypatch)
    failure = first['tool_results'][-1]
    assert failure.output['reason'] == 'code_mode_python_error'
    assert failure.output['execution_error']['kind'] == 'TypeError'
    assert failure.output['execution_error']['session_usable'] is True
    fixed = code_mode.execute_if_selected(first, [{'type': 'tool_use', 'name': 'code_mode', 'id': 'fix-signature',
        'input': {'tool_names': [], 'code': 'saved + 30'}}], execute=execute)
    assert fixed['tool_results'][-1].output['value'] == 37
    assert len([r for r in fixed['tool_results'] if r.tool_name == 'read']) == 1


def test_plain_next_list_type_error_is_not_reported_as_a_generator_divergence(monkeypatch):
    result=run('next([1,2])',monkeypatch)['tool_results'][-1]
    assert result.status=='failed' and result.output['execution_error']['kind']=='TypeError'
    assert 'python_runtime_note' not in result.output['execution_error']


@pytest.mark.parametrize('mutation',['none','already_seen','failed','changed','python_value'])
def test_visual_handoff_keeps_only_new_authorized_hash_bound_previews(tmp_path,mutation):
    from PIL import Image
    from nullion.tools import ToolInvocation,_build_file_read_handler
    path=tmp_path/'reference.png';Image.new('RGB',(30,20),'red').save(path)
    result=_build_file_read_handler(tmp_path,allowed_roots=[tmp_path])(
        ToolInvocation('read','file_read','operator',{'path':str(path)}))
    first=code_mode._new_visual_reference_blocks([result],[])
    assert len(first)==2 and first[1]['type']=='image'
    messages=[]
    if mutation=='already_seen':messages=[{'role':'user','content':first}]
    if mutation=='failed':result.status='failed'
    if mutation=='changed':Image.new('RGB',(30,20),'navy').save(path)
    if mutation=='python_value':result.tool_name='code_mode'
    actual=code_mode._new_visual_reference_blocks([result,result],messages)
    assert len(actual)==(2 if mutation=='none' else 0)


def test_native_snapshot_observation_paths_resolve_against_original_receipt():
    from nullion.code_mode import _default_observations
    from nullion.tools import ToolResult
    from nullion.retained_work_context import inspect_retained, inspection_failure_output
    native = ToolResult('observed-snapshot', 'browser_snapshot', 'completed', {
        'session_id': 'owned-browser', 'snapshot': {'url': 'https://example.test/start',
            'element_count': 1, 'elements': [{'element_id': 'link-1', 'tag': 'a',
                'role': 'link', 'text': 'Formulario', 'href': 'https://example.test/form'}]}})
    observed = _default_observations([native])['observations'][0]
    assert 'elements' not in observed['output']
    assert observed['output']['snapshot']['elements'][0]['href'] == 'https://example.test/form'
    request = {'requests': [{'invocation_id': native.invocation_id,
        'json_pointer': '/snapshot/elements/0/href', 'offset': 0, 'length': 2000}]}
    assert not inspection_failure_output(request, [native])['request_errors']
    assert 'https://example.test/form' in str(inspect_retained(request, [native]))
