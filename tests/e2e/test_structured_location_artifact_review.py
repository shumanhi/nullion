"""Native weather records remain valid evidence through real Web artifact delivery."""
from tests.workflow_provider_protocol import workflow_provider_reply
import json, os
import pytest
pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker product-boundary E2E")

@pytest.mark.parametrize("prompt", ["Prepare a weather report CSV.", "天気のCSVを作成してください。", "Crea un CSV del tiempo."])
def test_weather_location_object_is_reviewed_once_and_report_downloads(tmp_path, monkeypatch, prompt):
    from fastapi.testclient import TestClient
    from nullion import web_app, chat_store
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.artifacts import artifact_root_for_principal
    from nullion.tools import ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass, register_weather_forecast_tool, _build_file_write_handler, ToolResult
    monkeypatch.setenv("HOME", str(tmp_path)); monkeypatch.setenv("NULLION_HOME", str(tmp_path/"home"))
    monkeypatch.setenv("NULLION_ENV_FILE", str(tmp_path/"none.env")); monkeypatch.setenv("NULLION_MEMORY_ENABLED", "0")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "off"); chat_store._store = None
    runtime = bootstrap_persistent_runtime(tmp_path/"runtime.db"); registry = ToolRegistry(filesystem_allowed_roots=[tmp_path]); fetches = []
    def fetch(url, timeout):
        fetches.append(url)
        return {"timezone":"America/New_York", "current":{"time":"2026-10-25T01:30", "temperature_2m":36.3, "weather_code":0}, "daily":{"time":["2026-10-25"], "temperature_2m_max":[65.2], "temperature_2m_min":[32.1], "weather_code":[3]}}
    register_weather_forecast_tool(registry, json_get=fetch)
    # Only the independent external page boundary is deterministic; routing,
    # native weather normalization, review, persistence, and delivery stay real.
    registry.register(ToolSpec("web_fetch", "Read official page", ToolRiskLevel.LOW, ToolSideEffectClass.READ, False, 10), lambda inv: ToolResult(inv.invocation_id, inv.tool_name, "completed", {"text":"The science center is open today.", "url":"https://example.test/science"}))
    registry.register(ToolSpec("file_write", "Write report", ToolRiskLevel.LOW, ToolSideEffectClass.WRITE, False, 10), _build_file_write_handler(tmp_path, allowed_roots=[tmp_path]))
    root = artifact_root_for_principal("web:weather-proof"); root.mkdir(parents=True, exist_ok=True); path = root/"weather.csv"
    content = "Place,Temperature F\nProvidence,36.3\n"
    class Provider:
        phase = 0; reviews = 0
        def create(self, **kw):
            protocol_reply = workflow_provider_reply(kw, completion=False)
            if protocol_reply is not None: return protocol_reply
            names = {t["name"] for t in kw.get("tools", [])}; system = str(kw.get("system") or "")
            def text(v): return {"stop_reason":"end_turn", "content":[{"type":"text", "text":v}]}
            def tool(n, args): return {"stop_reason":"tool_use", "content":[{"type":"tool_use", "id":n+str(self.phase), "name":n, "input":args}]}
            if system.startswith("You are a material-requirements planner"):
                return text(json.dumps({"all_material_requirements_enumerated":True, "material_requirements":[{"requirement":"Read current weather", "requirement_kind":"fact", "evidence_subject_id":"weather", "eligible_evidence_tool_names":["weather_forecast"]}, {"requirement":"Deliver weather CSV", "requirement_kind":"deliverable", "evidence_subject_id":"csv", "eligible_evidence_tool_names":["file_write"]}]}))
            if system.startswith("You are a semantic completion reviewer"):
                self.reviews += 1; data = json.loads(kw["messages"][0]["content"][0]["text"])
                rows = []
                for row in data["material_requirement_contract"]:
                    evidence = [r for r in data["tool_evidence"] if r["tool_name"] == "weather_forecast"] if row["evidence_subject_id"] == "weather" else [r for r in data["tool_evidence"] if r.get("output", {}).get("artifact_record_binding")]
                    assert len(evidence) == 1, data
                    if row["evidence_subject_id"] == "weather":
                        observed = evidence[0]["output"]
                        assert observed["current"]["summary"] == "clear sky", observed
                        assert observed["daily"][0]["summary"] == "overcast", observed
                    rows.append({"requirement_id":row["requirement_id"], "satisfied":True, "draft_covered":True, "evidence_invocation_ids":[evidence[0]["invocation_id"]]})
                return text(json.dumps({"disposition":"complete", "all_material_requirements_enumerated":True, "material_requirements":rows, "unresolved_requirements":[]}))
            if "select_attachment_format" in names: return tool("select_attachment_format", {"extension":".csv", "confidence":1})
            if not names: return text("{}")
            self.phase += 1
            if self.phase == 1: return tool("request_tool_scope", {"capabilities":["local_files", "weather"], "tool_names":["weather_forecast", "file_write", "web_fetch"], "required_tool_names":["weather_forecast", "file_write"], "artifact_extensions":[".csv"]})
            if self.phase == 2: return tool("weather_forecast", {"latitude":41.824, "longitude":-71.413, "forecast_days":1})
            if self.phase == 3: return tool("web_fetch", {"url":"https://example.test/science"})
            if self.phase == 4: return tool("file_write", {"path":str(path), "content":content})
            assert self.phase == 5, "Successful source or report was needlessly repeated"
            return text("The weather report is attached.\nMEDIA:"+str(path))
    model = Provider(); runtime.model_client = model
    with TestClient(web_app.create_app(runtime, orchestrator=AgentOrchestrator(model_client=model), registry=registry)) as client:
        response = client.post("/api/chat", json={"text":prompt, "conversation_id":"web:weather-proof"}); assert response.status_code == 200
        data = response.json(); assert len(data["artifacts"]) == 1 and "review could not" not in data["text"], data
        assert client.get(data["artifacts"][0]["url"]).content == content.encode()
        assert model.reviews == 1 and len(fetches) == 1
    stored = bootstrap_persistent_runtime(tmp_path/"runtime.db")
    turn = next(r for r in stored.store.list_conversation_events("web:weather-proof") if r.get("event_type") == "conversation.chat_turn")
    assert len([r for r in turn["tool_results"] if r["tool_name"] == "weather_forecast"]) == 1
