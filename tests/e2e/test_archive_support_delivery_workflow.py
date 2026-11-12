"""Real Web route, support writes, shell archive, and served ZIP bytes."""
from tests.workflow_provider_protocol import workflow_provider_reply
import io
import json
import os
import shlex
import sys
from datetime import UTC, datetime
from zipfile import ZipFile

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Docker E2E only")


@pytest.mark.parametrize("prompt", ["Deliver only a ZIP containing north.csv and south.csv.",
    "Entrega solo un ZIP que contenga north.csv y south.csv.", "north.csvとsouth.csvを含むZIPだけを添付してください。"])
def test_archive_support_writes_do_not_become_attachments(tmp_path, monkeypatch, prompt):
    from fastapi.testclient import TestClient
    from nullion import chat_store, web_app
    from nullion.agent_orchestrator import AgentOrchestrator
    from nullion.approvals import create_permission_grant
    from nullion.artifacts import artifact_root_for_principal
    from nullion.policy import OPERATOR_PERMISSION_PRINCIPAL
    from nullion.runtime import bootstrap_persistent_runtime
    from nullion.tools import (ToolRegistry, ToolSpec, ToolRiskLevel, ToolSideEffectClass,
                               _build_file_write_handler, _build_terminal_exec_handler)

    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("NULLION_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("NULLION_ENV_FILE", str(tmp_path / "absent.env"))
    monkeypatch.setenv("NULLION_MEMORY_ENABLED", "0")
    monkeypatch.setenv("NULLION_TASK_PLANNER_FEED_MODE", "off")
    chat_store._store = None
    conversation = "web:archive-support"
    root = artifact_root_for_principal(conversation)
    source = root.parent / "scratch" / "archive-input"
    target = root / "bundle.zip"
    command = ("from pathlib import Path;from zipfile import ZipFile;"
               f"p=Path({str(target)!r});p.parent.mkdir(parents=True,exist_ok=True);"
               f"z=ZipFile(p,'w');z.write({str(source / 'north.csv')!r},'north.csv');"
               f"z.write({str(source / 'south.csv')!r},'south.csv');z.close()")

    class Provider:
        phase = 0
        def create(self, **kwargs):
            protocol_reply = workflow_provider_reply(kwargs, completion=False)
            if protocol_reply is not None: return protocol_reply
            names = {tool["name"] for tool in kwargs.get("tools", [])}
            system = str(kwargs.get("system") or "")
            def text(value):
                return {"stop_reason":"end_turn", "content":[{"type":"text", "text":value}]}
            def tool(name, data):
                return {"stop_reason":"tool_use", "content":[{"type":"tool_use", "id":str(self.phase), "name":name, "input":data}]}
            if system.startswith("You are a material-requirements planner"):
                context = json.loads(kwargs["messages"][0]["content"][0]["text"])
                assert context['artifact_delivery_contract']['required_artifact_extensions'] == ['.zip']
                return text(json.dumps({"all_material_requirements_enumerated":True, "material_requirements":[
                    {"requirement":"Deliver ZIP with both CSV members", "requirement_kind":"deliverable",
                     "evidence_subject_id":"archive", "eligible_evidence_tool_names":["terminal_exec"]}]}))
            if system.startswith("You are a semantic completion reviewer"):
                data = json.loads(kwargs["messages"][0]["content"][0]["text"])
                records = [row for row in data["tool_evidence"] if isinstance(row.get("output"),dict)
                           and row["output"].get("artifact_record_binding")]
                assert len(records) == 1
                preview = data['artifact_evidence'][0]['content_preview']
                assert 'north.csv' in preview and 'North,12' in preview and 'South,19' in preview
                return text(json.dumps({"disposition":"complete", "all_material_requirements_enumerated":True,
                    "unresolved_requirements":[], "material_requirements":[{**row, "satisfied":True,
                        "draft_covered":True, "evidence_invocation_ids":[records[0]["invocation_id"]]}
                        for row in data["material_requirement_contract"]]}))
            if "select_attachment_format" in names:
                return tool("select_attachment_format", {"extension":".zip", "confidence":1})
            if not names:
                return text("{}")
            self.phase += 1
            if self.phase == 1:
                return tool("request_tool_scope", {"capabilities":["local_files","local_shell"],
                    "tool_names":["file_write","terminal_exec"], "required_tool_names":["file_write","terminal_exec"],
                    "artifact_extensions":[".zip"], "excluded_artifact_extensions":[".csv"]})
            if self.phase in (2,3):
                assert "file_write" in names
                region, value = ("North",12) if self.phase == 2 else ("South",19)
                return tool("file_write", {"path":str(source / (region.lower()+".csv")),
                    "content":f"Region,Value\n{region},{value}\n"})
            if self.phase == 4:
                return tool("terminal_exec", {"command":shlex.quote(sys.executable)+" -c 'raise ValueError(\"fixture retry\")'"})
            if self.phase == 5:
                return tool("terminal_exec", {"command":shlex.quote(sys.executable)+" -c "+shlex.quote(command)})
            return text("Created the archive.")

    registry = ToolRegistry(filesystem_allowed_roots=[tmp_path])
    registry.register(ToolSpec("file_write","Write supporting inputs",ToolRiskLevel.MEDIUM,
        ToolSideEffectClass.WRITE,False,20),_build_file_write_handler(tmp_path,allowed_roots=[tmp_path]))
    registry.register(ToolSpec("terminal_exec","Create archive",ToolRiskLevel.HIGH,
        ToolSideEffectClass.DANGEROUS_EXEC,True,20),_build_terminal_exec_handler(tmp_path,allowed_roots=[tmp_path]))
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    runtime.store.add_permission_grant(create_permission_grant(approval_id="fixture",
        principal_id=OPERATOR_PERMISSION_PRINCIPAL,permission="tool:terminal_exec",
        granted_by="fixture",granted_at=datetime.now(UTC)))
    runtime.checkpoint()
    model = Provider()
    runtime.model_client = model
    with TestClient(web_app.create_app(runtime,orchestrator=AgentOrchestrator(model_client=model),registry=registry)) as client:
        response = client.post("/api/chat",json={"text":prompt,"conversation_id":conversation})
        assert response.status_code == 200, response.text
        data = response.json()
        assert len(data["artifacts"]) == 1 and data["artifacts"][0]["name"] == "bundle.zip", data
        download = client.get(data["artifacts"][0]["url"])
        assert download.status_code == 200
        with ZipFile(io.BytesIO(download.content)) as archive:
            assert set(archive.namelist()) == {"north.csv","south.csv"}
            assert b"North,12" in archive.read("north.csv") and b"South,19" in archive.read("south.csv")
        turns = [row for row in runtime.store.list_conversation_events(conversation)
                 if row.get("event_type") == "conversation.chat_turn"]
        scopes = [row["output"] for row in turns[-1]["tool_results"] if row["tool_name"] == "request_tool_scope"]
        assert scopes and all(row["artifact_extensions"] == [".zip"] for row in scopes)
