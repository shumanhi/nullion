"""Verify generated and imported report bytes at artifact and email boundaries."""

import base64
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from tests.e2e.test_quoted_reply_review_and_cleanup import isolate
from tests.e2e.test_web_surface_contracts import _UvicornThread, _free_port


pytestmark = pytest.mark.skipif(os.environ.get("NULLION_RUN_E2E") != "1", reason="Container E2E")


HEADING = "Asunción · €"


@pytest.mark.parametrize("origin", ["generated", "generated-undeclared", "generated-utf8", "legacy-import", "utf8-import"])
def test_web_artifact_uses_matching_charset_and_preserves_download(tmp_path, monkeypatch, origin):
    from playwright.sync_api import expect, sync_playwright

    from nullion.artifacts import artifact_descriptor_for_path, artifact_root_for_principal
    from nullion.tools import ToolInvocation, ToolRegistry, _build_file_write_handler
    from nullion.web_app import create_app

    runtime = isolate(tmp_path, monkeypatch)
    principal = "web:charset-preview"
    root = artifact_root_for_principal(principal)
    root.mkdir(parents=True, exist_ok=True)
    path = root / "voyage.htm"
    declaration = '<meta charset="windows-1252">' if origin in {"generated", "legacy-import"} else ""
    document = f"<!doctype html><html><head>{declaration}</head><body><h1>{HEADING}</h1></body></html>"
    if origin == "generated-utf8":
        document = document.replace("<head>", '<head><meta charset="UTF-8">') + "\n"
    if origin.startswith("generated"):
        writer = _build_file_write_handler(root, allowed_roots=[root])
        result = writer(ToolInvocation("encoding-write", "file_write", principal,
                                      {"path": str(path), "content": document}))
        assert result.status == "completed", result.error
    else:
        path.write_bytes(document.encode("cp1252" if origin == "legacy-import" else "utf-8"))
    original_bytes = path.read_bytes()
    if origin == "generated-utf8":
        assert original_bytes == document.encode("utf-8")
    elif origin.startswith("generated"):
        assert b'<meta charset="utf-8">' in original_bytes[:1024]
        assert b"windows-1252" not in original_bytes

    descriptor = artifact_descriptor_for_path(path, artifact_root=root)
    endpoint = f"/api/artifacts/{descriptor.artifact_id}"
    with _UvicornThread(create_app(runtime, orchestrator=None, registry=ToolRegistry()), _free_port()) as server:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            response = page.goto(server.base_url + endpoint + "/preview", wait_until="domcontentloaded")
            assert response.status == 200
            expect(page.locator("h1")).to_have_text(HEADING)
            download = page.request.get(server.base_url + endpoint)
            assert download.ok and download.body() == original_bytes
            browser.close()
    assert path.read_bytes() == original_bytes


@pytest.mark.parametrize("mime_type, extension, encoding, invalid_byte", [
    ("text/plain; charset=windows-1252", ".txt", "cp1252", False),
    ("text/html; charset=windows-1252", ".htm", "cp1252", False),
    ("text/html; charset=utf-16le", ".html", "utf-16-le", False),
    ("text/plain; charset=x-fixture-unknown", ".txt", "utf-8", False),
    ("text/plain; charset=utf-8", ".txt", "utf-8", True),
    ("text/html; charset=utf-8", ".html", "utf-8", True),
])
def test_connector_attachment_preserves_bytes_and_decodes_text(tmp_path, monkeypatch, mime_type, extension,
                                                               encoding, invalid_byte):
    from nullion import tools

    isolate(tmp_path, monkeypatch)
    html = mime_type.startswith("text/html")
    text = f"<html><body><h1>{HEADING}</h1></body></html>" if html else HEADING
    original = text.encode(encoding)
    if invalid_byte:
        original = original.replace(HEADING.encode(encoding), b"\xff " + HEADING.encode(encoding))
    monkeypatch.setattr(tools, "_connector_connection_for_invocation",
                        lambda *_: SimpleNamespace(provider_profile="https://connector.example.test"))
    monkeypatch.setattr(tools, "_connector_json_request",
                        lambda *a, **kw: {"data": base64.urlsafe_b64encode(original).decode("ascii")})
    invocation = tools.ToolInvocation("read-encoding-fixture", "email_attachment_read", "web:incoming", {
        "message_id": "fixture-message",
        "attachment_id": "fixture-file",
        "provider_id": "gmail",
        "filename": "voyage" + extension,
        "mime_type": mime_type,
    })
    result = tools._build_connector_email_attachment_read_handler()(invocation)
    assert result.status == "completed", result.error
    assert result.output["text_preview"] == ("� " if invalid_byte else "") + HEADING
    assert Path(result.output["artifact_path"]).read_bytes() == original


@pytest.mark.parametrize("encoding", ["cp1252", "invalid"])
def test_exact_email_review_decodes_before_creating_a_preview(tmp_path, monkeypatch, encoding):
    from nullion.web_app import create_app
    from tests.e2e.test_approval_exact_action_boundaries import email_fixture

    sent = []

    def deliver(message, attachments):
        sent.append(message.get_body(preferencelist=("html",)).get_content())
        return {"message_id": "charset-fixture"}

    runtime, registry, executor, invocation, source = email_fixture(tmp_path, monkeypatch, deliver)
    document = f'<html><head><meta charset="windows-1252"></head><body><h1>{HEADING}</h1></body></html>'
    raw = document.encode("cp1252") if encoding == "cp1252" else b"<html><body>Not UTF-8: \xff</body></html>"
    source.write_bytes(raw)
    pending = executor.invoke(invocation)
    if encoding == "invalid":
        assert pending.output["reason"] == "email_source_encoding_invalid"
        assert "approval_id" not in pending.output and not sent
    else:
        assert pending.output["reason"] == "approval_required"
        preview = Path(pending.output["html_preview_path"]).read_bytes()
        assert b'<meta charset="utf-8">' in preview[:1024] and HEADING.encode() in preview
        approval = pending.output["approval_id"]
        with TestClient(create_app(runtime, orchestrator=None, registry=registry)) as client:
            response = client.post(f"/api/approve/{approval}", json={"mode": "once"})
            assert response.status_code == 200 and response.json()["ok"]
        assert len(sent) == 1 and HEADING in sent[0]
    assert source.read_bytes() == raw
