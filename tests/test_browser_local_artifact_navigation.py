from __future__ import annotations

from nullion.plugins.browser_plugin.browser_policy import BrowserPolicy
from nullion.plugins.browser_plugin.browser_session import BrowserSessionPool
from nullion.plugins.browser_plugin.browser_tools import BrowserTools
from nullion.tools import ToolInvocation
from nullion.workspace_storage import workspace_storage_roots_for_principal


class BrowserBackendDouble:
    def __init__(self) -> None:
        self.navigated_urls: list[str] = []

    async def navigate(self, session_id: str, url: str) -> dict[str, object]:
        self.navigated_urls.append(url)
        return {"session_id": session_id, "url": url, "title": "local artifact"}

    async def close_session(self, session_id: str) -> None:
        return None


def _invocation(url: str, *, principal_id: str = "web:operator") -> ToolInvocation:
    return ToolInvocation(
        invocation_id=f"inv-{len(url)}",
        tool_name="browser_navigate",
        principal_id=principal_id,
        arguments={"url": url},
    )


def _browser_tools() -> tuple[BrowserTools, BrowserBackendDouble]:
    backend = BrowserBackendDouble()
    return BrowserTools(backend=backend, pool=BrowserSessionPool(), policy=BrowserPolicy()), backend


def test_browser_navigate_allows_workspace_html_artifact_preview(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    artifact = workspace_storage_roots_for_principal("web:operator").artifacts / "rapport.html"
    artifact.write_text("<!doctype html><html><body><h1>Valid</h1></body></html>", encoding="utf-8")
    tools, backend = _browser_tools()

    result = tools.browser_navigate(_invocation(artifact.as_uri()))

    assert result.status == "completed"
    assert backend.navigated_urls == [artifact.as_uri()]


def test_browser_navigate_accepts_workspace_html_path_without_localhost_preview_server(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    artifact = workspace_storage_roots_for_principal("web:operator").artifacts / "validation.html"
    artifact.write_text("<!doctype html><html><body><p>ok</p></body></html>", encoding="utf-8")
    tools, backend = _browser_tools()

    result = tools.browser_navigate(_invocation(str(artifact)))

    assert result.status == "completed"
    assert backend.navigated_urls == [artifact.as_uri()]


def test_browser_navigate_keeps_arbitrary_local_files_blocked(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    outside = tmp_path / "outside.html"
    outside.write_text("<!doctype html><html><body>outside</body></html>", encoding="utf-8")
    tools, backend = _browser_tools()

    result = tools.browser_navigate(_invocation(outside.as_uri()))

    assert result.status == "failed"
    assert "Only http/https URLs are allowed" in str(result.error)
    assert backend.navigated_urls == []


def test_browser_navigate_allows_admin_private_localhost(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    monkeypatch.setenv("NULLION_BROWSER_BLOCK_PRIVATE", "true")
    tools, backend = _browser_tools()

    result = tools.browser_navigate(_invocation("http://127.0.0.1:8123/report.html"))

    assert result.status == "completed"
    assert backend.navigated_urls == ["http://127.0.0.1:8123/report.html"]


def test_browser_navigate_keeps_private_localhost_blocked_for_member_users(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("NULLION_WORKSPACE_STORAGE_ROOT", str(tmp_path / "workspaces"))
    monkeypatch.setenv("NULLION_BROWSER_BLOCK_PRIVATE", "true")
    tools, backend = _browser_tools()

    result = tools.browser_navigate(_invocation("http://127.0.0.1:8123/report.html", principal_id="user:member-1"))

    assert result.status == "failed"
    assert "private/local host blocked" in str(result.error)
    assert backend.navigated_urls == []
