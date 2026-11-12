"""Metadata discovery -> selected read -> answer at the real chat boundary."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json
import os
import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E only")


@pytest.mark.parametrize("prompt", ["Check my calendar", "Revisa mi calendario", "私の予定表を確認してください"])
@pytest.mark.parametrize("omit_required", [False, True])
@pytest.mark.parametrize("premature_resolution", [False, True, "stuck"])
def test_named_source_resolution_continues_without_unrelated_account_choice(tmp_path, monkeypatch, prompt, omit_required, premature_resolution):
    from datetime import UTC, datetime
    from fastapi.testclient import TestClient
    from nullion import web_app, turn_context_policy, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL, BoundaryPolicyRule, BoundaryKind, PolicyMode
    from nullion.tools import ToolResult
    from tests.test_account_source_resolution import registry
    monkeypatch.setenv("HOME", str(tmp_path));monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_ENV_FILE", str(tmp_path / "missing.env"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "0")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "off")
    monkeypatch.setenv("NULLION_MODEL_TOOL_RESULT_MAX_CHARS", "12000")
    chat_store._store = None
    adapter_ids=["google-mail","google-calendar","outlook",*[
        f"catalogue-adapter-{index}-"+"x"*50 for index in range(100)]]
    monkeypatch.setattr(turn_context_policy, "_active_connector_provider_context", lambda: [{
        "provider_id":"example", "display_name":"Example gateway",
        "active_app_ids":adapter_ids,
        "app_inventory_kind":"supported_catalogue",
        "structured_tools":["email_search","calendar_list"]}])
    tools = registry(); reads = []; scopes = []
    # Fake only the external account adapter; routing and persistence stay real.
    def account(inv):
        reads.append(inv.tool_name)
        return ToolResult(inv.invocation_id, inv.tool_name, "completed", {
            "events":[{"id":"event-1","summary":"Project meeting","start":"2026-10-24T14:00:00Z"}]})
    # Registration replaces the external adapter while retaining its metadata.
    spec = tools.get_spec("calendar_list")
    tools.unregister("calendar_list")
    tools.register(spec, account)
    class Provider:
        prematurely_finished = False
        resolution_finished = False
        main_calls = 0
        def create(self, **kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            names = {item["name"] for item in kw.get("tools", [])}
            system = str(kw.get("system") or "")
            if names:
                self.main_calls += 1
            if system.startswith("You are a material-requirements planner"):
                value = {"all_material_requirements_enumerated":True, "material_requirements":[]}
            elif system.startswith("You are a semantic completion reviewer"):
                value = {"disposition":"complete", "all_material_requirements_enumerated":True,
                         "material_requirements":[],"unresolved_requirements":[],"reason":"Source read completed"}
            elif not names:
                value = {"skill_pack_action":"none","confidence":1}
            elif not scopes:
                scopes.append("discovery")
                return {"stop_reason":"tool_use", "content":[{"type":"tool_use","id":"discover",
                    "name":"request_tool_scope","input":{"capabilities":["connector"],
                    "source_user_requested":True,"source_evidence":prompt}}]}
            elif "calendar_list" not in names:
                if not getattr(self,'metadata_route_checked',False):
                    evidence=[block.get('content') for message in kw['messages']
                        for block in (message.get('content') if isinstance(message.get('content'),list) else [])
                        if isinstance(block,dict) and block.get('type')=='tool_result']
                    texts=[text for value in evidence for text in (
                        [value] if isinstance(value,str) else [block.get('text','')
                            for block in value if isinstance(block,dict)])]
                    self.scope_evidence=texts
                    packets=[json.loads(value) for value in texts if value.startswith('{')]
                    compact=[packet.get('output',packet) for packet in packets]
                    assert compact and compact[-1]['result_kind']=='tool_scope_evidence',texts
                    assert compact[-1]['available_sources'][0]['active_app_ids']==adapter_ids,texts
                    assert compact[-1]['source_resolution_required'] and not compact[-1]['source_selection_required']
                    assert 'resolve its exact read tool' in compact[-1]['message'],compact
                    self.metadata_route_checked=True
                if premature_resolution and (premature_resolution == "stuck" or not self.resolution_finished):
                    self.resolution_finished = True
                    return {"stop_reason":"end_turn", "content":[{"type":"text","text":"1. Entry A\n2. Entry B"}]}
                scopes.append("selected")
                arguments = {"capabilities":["connector"],
                    "source_user_requested":True,"source_evidence":prompt,"tool_names":["calendar_list"]}
                if not omit_required: arguments["required_tool_names"] = ["calendar_list"]
                return {"stop_reason":"tool_use", "content":[{"type":"tool_use","id":"select",
                    "name":"request_tool_scope","input":arguments}]}
            elif omit_required and not self.prematurely_finished:
                self.prematurely_finished = True
                return {"stop_reason":"end_turn","content":[{"type":"text","text":"The source is ready."}]}
            elif not reads:
                return {"stop_reason":"tool_use", "content":[{"type":"tool_use","id":"read",
                    "name":"calendar_list","input":{"start":"2026-10-24T00:00:00Z", "end":"2026-10-25T00:00:00Z"}}]}
            else:
                return {"stop_reason":"end_turn","content":[{"type":"text","text":"Project meeting — 14:00 UTC."}]}
            return {"stop_reason":"end_turn","content":[{"type":"text","text":json.dumps(value)}]}
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.store.add_permission_grant(create_permission_grant(approval_id="fixture",
        principal_id=OPERATOR_PERMISSION_PRINCIPAL, permission="tool:calendar_list",
        granted_by="fixture", granted_at=datetime.now(UTC)))
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule("fixture", OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.ACCOUNT_ACCESS, PolicyMode.ALLOW, "*", "fixture", datetime.now(UTC)))
    runtime.checkpoint()
    model = Provider();runtime.model_client = model
    with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=tools)) as client:
        response = client.post("/api/chat", json={"text":prompt,"conversation_id":"web:resolve"})
        assert response.status_code == 200, response.text
        if premature_resolution == "stuck":
            assert response.json()["text"] == "Source selection did not finish, so I couldn't start that account lookup.", response.json()
            assert not reads and model.main_calls == 4
        else:
            assert "Project meeting" in response.json()["text"], (response.json(),getattr(model,'scope_evidence',[]))
    turns = [row for row in runtime.store.list_conversation_events("web:resolve")
             if row.get("event_type") == "conversation.chat_turn"]
    result = turns[-1]["tool_results"]
    discovery = next(row["output"] for row in result if row["tool_name"] == "request_tool_scope")
    assert discovery["source_resolution_required"] and not discovery["source_selection_required"]
    assert discovery["available_tools"] == []
    if premature_resolution == "stuck":
        assert not reads and scopes == ["discovery"] and model.main_calls == 4
    else:
        assert reads == ["calendar_list"] and scopes == ["discovery", "selected"]


@pytest.mark.parametrize("prompt",["Check my emails","Revisa mis correos","私のメールを確認してください"])
def test_context_discovery_turn_keeps_source_routes_without_extra_classifier(tmp_path,monkeypatch,prompt,record_property):
    from datetime import UTC,datetime
    from fastapi.testclient import TestClient
    from nullion import web_app,chat_store,chat_operator
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.approvals import create_permission_grant
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL,BoundaryPolicyRule,BoundaryKind,PolicyMode
    from nullion.tools import register_email_plugin
    from tests.test_account_source_resolution import registry
    import time
    monkeypatch.setenv("HOME",str(tmp_path));monkeypatch.setenv("NULLION_HOME",str(tmp_path/"home"))
    monkeypatch.setenv("NULLION_ENV_FILE",str(tmp_path/"absent.env"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED","0");monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE","off")
    chat_store._store=None
    reads=[];tools=registry()
    def account(query,limit):
        reads.append((query,limit))
        return [{"id":"fixture-1","subject":"Project update","from":"team@example.com","snippet":"No action required"}]
    register_email_plugin(tools,email_searcher=account)
    from nullion import turn_context_policy
    monkeypatch.setattr(turn_context_policy,"_active_connector_provider_context",lambda:[{
        "provider_id":"fixture","display_name":"Fixture account gateway",
        "active_app_ids":["google-mail"],"structured_tools":["email_search"]}])
    class Provider:
        phase="setup"
        ordinary_calls=0
        def create(self,**kw):
            protocol_reply = workflow_provider_reply(kw, completion=True)
            if protocol_reply is not None: return protocol_reply
            names={x["name"] for x in kw.get("tools",[])}
            system=str(kw.get("system") or "")
            if system.startswith("You are a material-requirements planner"):
                value={"all_material_requirements_enumerated":True,"material_requirements":[]}
            elif system.startswith("You are a semantic completion reviewer"):
                value={"disposition":"complete","all_material_requirements_enumerated":True,
                    "material_requirements":[],"unresolved_requirements":[],"reason":"Source read completed"}
            elif self.phase=="plain":
                self.ordinary_calls+=1
                if self.ordinary_calls>1:
                    time.sleep(3);raise AssertionError("Extra classifier entered plain chat")
                return {"stop_reason":"end_turn","content":[{"type":"text","text":"56"}]}
            elif not names:
                value={"skill_pack_action":"none","confidence":1,"disposition":"independent"}
            elif self.phase=="setup":
                return {"stop_reason":"end_turn","content":[{"type":"text","text":"Understood."}]}
            elif "email_search" not in names:
                context=json.dumps(kw.get("messages",[]),ensure_ascii=False)
                if "Compact registered capability index" not in context:
                    return {"stop_reason":"end_turn","content":[{"type":"text","text":"1. Configured mailbox\n2. Another mailbox"}]}
                assert 'account_read_routes' in context and "email_search" in context
                return {"stop_reason":"tool_use","content":[{"type":"tool_use","id":"scope",
                    "name":"request_tool_scope","input":{"capabilities":["connector"],
                        "tool_names":["email_search"],"required_tool_names":["email_search"],
                        "source_user_requested":True,"source_evidence":prompt}}]}
            elif not reads:
                return {"stop_reason":"tool_use","content":[{"type":"tool_use","id":"read",
                    "name":"email_search","input":{"query":"newer_than:7d","limit":5}}]}
            else:
                return {"stop_reason":"end_turn","content":[{"type":"text","text":"Project update: no action required."}]}
            return {"stop_reason":"end_turn","content":[{"type":"text","text":json.dumps(value)}]}
    runtime=bootstrap_persistent_runtime(tmp_path/"runtime.db");model=Provider();runtime.model_client=model
    runtime.store.add_permission_grant(create_permission_grant(approval_id="fixture",principal_id=OPERATOR_PERMISSION_PRINCIPAL,
        permission="tool:email_search",granted_by="fixture",granted_at=datetime.now(UTC)))
    runtime.store.add_boundary_policy_rule(BoundaryPolicyRule("fixture",OPERATOR_PERMISSION_PRINCIPAL,
        BoundaryKind.ACCOUNT_ACCESS,PolicyMode.ALLOW,"*","fixture",datetime.now(UTC)))
    runtime.checkpoint()
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=tools)) as client:
        setup=client.post("/api/chat",json={"text":"Remember that we discussed account security; no account lookup now.","conversation_id":"web:source-context"})
        assert setup.status_code==200
        model.phase="account"
        response=client.post("/api/chat",json={"text":prompt,"conversation_id":"web:source-context"})
        assert response.status_code==200 and "Project update" in response.json()["text"],response.text
        events=[e for e in runtime.store.list_conversation_events("web:source-context") if e.get("event_type")=="conversation.chat_turn"]
        assert len(reads)==1 and any(r["tool_name"]=="email_search" and r["status"]=="completed" for r in events[-1]["tool_results"])
        # Simulate the previously expensive full inventory. Ordinary requests
        # must retain metadata awareness while never invoking this dependency.
        full_calls=[]
        def slow_full(*args,**kwargs):
            full_calls.append(True);time.sleep(3);return "unnecessary full inventory"
        monkeypatch.setattr(chat_operator,"_chat_capability_inventory_prompt",slow_full)
        elapsed=[];model.phase="plain"
        for index,text in enumerate(["7 × 8 の答えだけ。","Solo 7 × 8.","Calcule 7 × 8."]):
            model.ordinary_calls=0;started=time.monotonic()
            r=client.post("/api/chat",json={"text":text,"conversation_id":"web:compact-plain-"+str(index)})
            elapsed.append(time.monotonic()-started)
            assert r.status_code==200 and r.json()["text"]=="56" and model.ordinary_calls==1,r.text
        record_property("plain_elapsed_seconds",json.dumps(elapsed))
        record_property("plain_average_seconds",sum(elapsed)/len(elapsed))
        record_property("plain_p95_upper_bound_seconds",max(elapsed))
        assert not full_calls and sum(elapsed)/len(elapsed)<1.5 and max(elapsed)<2.5,elapsed
        assert len(reads)==1
