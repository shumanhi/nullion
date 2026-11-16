import json

import pytest

from nullion.tools import ToolInvocation


def test_designed_image_is_discoverable_without_loading_a_connector(tmp_path):
    from nullion.artifact_production import production_tool_names
    from nullion.tools import build_default_tool_registry
    registry = build_default_tool_registry(workspace_root=tmp_path)
    spec = registry.get_spec("image_render")
    assert spec.code_mode_allowed and not spec.requires_approval
    assert "image_render" in production_tool_names(registry, (".png",))
    definition = next(t for t in registry.list_tool_definitions() if t["name"] == "image_render")
    schema = definition["input_schema"]
    assert schema["properties"]["card_pages"]["maxItems"] == 1
    assert "$ref" not in json.dumps(schema)


@pytest.mark.parametrize("extra", [
    {"card_pages": []}, {"width": True}, {"width": 2000}, {"pixel_ratio": 3},
    {"output_path": "bad.jpg"}, {"output_path": 37},
    {"card_pages": [{"title": "旅", "cards": [{"heading": "予定", "text": "10:53", "wide": "yes"}]}]},
])
def test_invalid_image_arguments_are_repairable_before_any_capture(tmp_path, monkeypatch, extra):
    from nullion import image_render
    from nullion.tool_repair_contract import tool_repair_kind
    def unexpected(*args):
        pytest.fail("Invalid arguments must not start Chromium")
    monkeypatch.setattr(image_render, "_render", unexpected)
    args = {"card_pages": [{"title": "旅", "cards": [{"heading": "予定", "text": "10:53"}]}], **extra}
    result = image_render.build_handler(tmp_path, [tmp_path])(ToolInvocation("bad", "image_render", "web:image", args))
    assert result.status == "failed" and result.output["executed"] is False
    assert tool_repair_kind(result) == "correct_arguments"
    assert not list(tmp_path.rglob("*.png"))


@pytest.mark.parametrize("source", ["https://source.test/photo.png", "/etc/secret.png", "missing.png"])
def test_unavailable_or_unauthorized_images_do_not_start_capture(tmp_path, monkeypatch, source):
    from nullion import image_render
    monkeypatch.setattr(image_render, "_render", lambda *args: pytest.fail("Unexpected capture"))
    result = image_render.build_handler(tmp_path, [tmp_path])(ToolInvocation("assets", "image_render", "web:image", {
        "card_pages": [{"title": "旅", "cards": [{"heading": "予定", "text": "10:53", "image_path": source}]}]}))
    assert result.status == "failed" and result.output["reason"] == "invalid_tool_arguments"


def test_generated_image_preview_is_bound_to_exact_current_bytes(tmp_path):
    from hashlib import sha256
    from PIL import Image
    from nullion.agent_orchestrator import _tool_result_visual_reference_blocks
    from nullion.tools import ToolResult
    path = tmp_path / "designed.png"
    Image.new("RGB", (40, 40), "green").save(path)
    result = ToolResult("image", "image_render", "completed", {"path": str(path), "type": "image",
        "visual_preview_available": True, "image_sha256": sha256(path.read_bytes()).hexdigest()})
    assert any(b["type"] == "image" for b in _tool_result_visual_reference_blocks(result))
    Image.new("RGB", (40, 40), "red").save(path)
    assert not _tool_result_visual_reference_blocks(result)


def test_missing_source_capture_cannot_be_repaired_with_a_generated_image(tmp_path):
    from nullion.chat_operator import _repair_tool_registry_for_missing_attachment_extensions
    from nullion.tools import build_default_tool_registry
    from nullion.turn_context_policy import ScopedTurnToolRegistry, TurnToolEvidence, TurnToolScopeDecision
    base=build_default_tool_registry(workspace_root=tmp_path)
    request='選択したページの撮影をPNGで返してください。'
    decision=TurnToolScopeDecision(valid=True,requested_tool_names=('browser_screenshot',),
        requested_artifact_extensions=('.png',),source_artifact_delivery_requested=True,
        source_artifact_request_evidence=request)
    scoped=ScopedTurnToolRegistry(base,evidence=TurnToolEvidence(current_user_message=request,has_url_target=True),
        tool_scope_decision=decision)
    repaired=_repair_tool_registry_for_missing_attachment_extensions(base_registry=base,active_registry=scoped,
        missing_extensions=('.png',),existing_registry_decision=decision)
    assert repaired is scoped
    assert not repaired.can_invoke_tool('image_render')
