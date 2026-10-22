from __future__ import annotations

from datetime import UTC, datetime

from nullion.approvals import ApprovalRequest, ApprovalStatus
from nullion.chat_response_contract import (
    ChatResponseContract,
    ChatTurnStateSnapshot,
    ContextLinkMode,
    DraftOperationalClaim,
    ModelDraftResponse,
    OperationalFact,
    OperationalFactKind,
    build_live_information_resolution_facts,
    build_pending_approval_facts_from_tool_results,
    build_tool_execution_facts_from_tool_results,
    is_canonical_deferred_runtime_offer_reply,
    render_chat_response_for_telegram,
    text_mentions_approval_claim,
)
from nullion.live_information import LiveInformationResolution
from nullion.tools import ToolResult


def _state(*facts: OperationalFact) -> ChatTurnStateSnapshot:
    return ChatTurnStateSnapshot(
        conversation_id="conv",
        turn_id="turn",
        user_message="message",
        context_link=ContextLinkMode.STANDALONE,
        facts=facts,
        pending_approval_ids=tuple(
            str(fact.payload["approval_id"])
            for fact in facts
            if fact.kind is OperationalFactKind.APPROVAL_REQUEST_PENDING and fact.payload.get("approval_id")
        ),
    )


def _render(
    text: str,
    *facts: OperationalFact,
    claims: tuple[DraftOperationalClaim, ...] = (),
    claimed_tool_name: str | None = None,
) -> str:
    return render_chat_response_for_telegram(
        ChatResponseContract(
            _state(*facts),
            ModelDraftResponse(
                text,
                operational_claims=claims,
                claimed_tool_name=claimed_tool_name,
            ),
        )
    )


def test_pending_approval_facts_render_real_approval_prompt(monkeypatch) -> None:
    approval = ApprovalRequest(
        "ap-1",
        "operator",
        "allow_boundary",
        "web_fetch",
        ApprovalStatus.PENDING,
        datetime(2026, 1, 1, tzinfo=UTC),
        context={"boundary_kind": "outbound_network", "selector": "example.com"},
    )
    result = ToolResult("inv-1", "web_fetch", "DENIED", {"reason": "approval_required", "approval_id": "ap-1"})
    facts, pending = build_pending_approval_facts_from_tool_results([result, result], approval_lookup=lambda approval_id: approval)

    monkeypatch.setattr("nullion.chat_response_contract.load_settings", lambda: type("S", (), {"web_session_allow_duration": "1h"})())

    assert pending == ("ap-1",)
    reply = _render("approval required", *facts)
    assert reply.startswith("Tool approval requested: ap-1")
    assert "Approval required before Nullion can continue." in reply
    assert "Approval ID: ap-1" in reply
    assert "Reply /approve ap-1" in reply

    ignored, ignored_pending = build_pending_approval_facts_from_tool_results(
        [ToolResult("inv-2", "web_fetch", "denied", {"reason": "other", "approval_id": "ap-2"})],
        approval_lookup=lambda approval_id: None,
    )
    assert ignored == ()
    assert ignored_pending == ()


def test_pending_approval_facts_accept_direct_approval_required_status(monkeypatch) -> None:
    approval = ApprovalRequest(
        "ap-direct",
        "operator",
        "use_tool",
        "email_send",
        ApprovalStatus.PENDING,
        datetime(2026, 1, 1, tzinfo=UTC),
        context={"tool_name": "email_send", "tool_arguments": {"to": ["recipient@example.com"]}},
    )
    result = ToolResult(
        "inv-email",
        "email_send",
        "approval_required",
        {"reason": "approval_required", "requires_approval": True, "approval_id": "ap-direct"},
    )

    facts, pending = build_pending_approval_facts_from_tool_results(
        [result],
        approval_lookup=lambda approval_id: approval,
    )

    monkeypatch.setattr(
        "nullion.chat_response_contract.load_settings",
        lambda: type("S", (), {"web_session_allow_duration": "1h"})(),
    )

    assert pending == ("ap-direct",)
    reply = _render("approval required", *facts)
    assert reply.startswith("Tool approval requested: ap-direct")
    assert "Approval required before Nullion can continue." in reply
    assert "Approval ID: ap-direct" in reply


def test_tool_execution_facts_drive_failure_completed_and_negative_claim_replies() -> None:
    failed_facts = build_tool_execution_facts_from_tool_results(
        [ToolResult("inv-fail", "web_fetch", "failed", {}, error="network down")]
    )
    reply = _render("web_fetch failed", *failed_facts)
    assert "I attempted web_fetch" in reply
    assert "network down" in reply

    facts = build_tool_execution_facts_from_tool_results([ToolResult("inv-ok", "file_write", "completed", {"path": "/tmp/a.txt"})])
    assert _render(
        "批准が必要",
        *facts,
        claims=(DraftOperationalClaim.APPROVAL_REQUIRED,),
        claimed_tool_name="file_write",
    ) == "I completed file_write in this turn."
    assert _render(
        "工具 web_fetch 状态",
        *facts,
        claims=(DraftOperationalClaim.APPROVAL_REQUIRED,),
        claimed_tool_name="web_fetch",
    ) == "I haven't fetched or attached the requested page in this turn yet. If you want, I can try the actual fetch now or draft the exact next step first."
    assert _render(
        "工具 web_fetch 状态\nMEDIA: /tmp/a.png",
        *facts,
        claims=(DraftOperationalClaim.APPROVAL_REQUIRED,),
        claimed_tool_name="web_fetch",
    ).endswith("MEDIA: /tmp/a.png")

    attempted_only = build_tool_execution_facts_from_tool_results([ToolResult("inv-run", "web_fetch", "running", {})])
    assert _render(
        "I haven't attempted shell_exec in this turn yet",
        *attempted_only,
        claims=(DraftOperationalClaim.TOOL_ATTEMPTED,),
        claimed_tool_name="shell_exec",
    ) == "I haven't attempted shell_exec in this turn yet"
    assert _render(
        "shell_exec",
        *attempted_only,
        claims=(DraftOperationalClaim.TOOL_ATTEMPTED,),
    ) == "shell_exec"


def test_image_generation_setup_failure_gets_user_facing_guidance() -> None:
    failed_facts = build_tool_execution_facts_from_tool_results(
        [
            ToolResult(
                "inv-img",
                "image_generate",
                "failed",
                {"reason": "provider_not_configured", "capability": "image_generate"},
                error="image_generate provider is not configured",
            )
        ]
    )

    reply = _render("done", *failed_facts)
    assert "API image generation" in reply
    assert "not configured" in reply
    assert "local fallback images" in reply
    assert "💡 Setup tip:" in reply
    assert "Settings" in reply


def test_local_image_fallback_artifact_note_does_not_depend_on_english_prompt(tmp_path) -> None:
    from nullion.artifacts import artifact_root_for_principal
    from nullion.chat_operator import _append_chat_artifacts_to_reply
    from nullion.runtime import bootstrap_persistent_runtime

    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    principal_id = "telegram:imagen"
    artifact_path = artifact_root_for_principal(principal_id) / "imagenes.xlsx"
    artifact_path.write_bytes(b"not a real spreadsheet, just enough bytes")
    tool_results = [
        ToolResult(
            "inv-img",
            "image_generate",
            "failed",
            {},
            error="local_media_provider requires NULLION_IMAGE_GENERATE_COMMAND for image_generate",
        ),
        ToolResult("inv-shell", "terminal_exec", "completed", {"exit_code": 0}),
    ]

    reply = _append_chat_artifacts_to_reply(
        runtime,
        reply="Listo.",
        artifact_paths=[str(artifact_path)],
        prompt="crear una hoja con cinco ilustraciones",
        principal_id=principal_id,
        tool_results=tool_results,
        required_attachment_extensions=(".xlsx",),
    )

    assert "Done — attached the requested spreadsheet." in reply
    assert "local fallback" in reply
    assert "💡 Setup tip:" in reply
    assert "image generation provider in Settings" in reply
    assert f"MEDIA:{artifact_path}" in reply


def test_artifact_appender_filters_intermediate_zip_when_xlsx_is_required(tmp_path, monkeypatch) -> None:
    from nullion.artifacts import artifact_root_for_principal
    from nullion.chat_operator import _append_chat_artifacts_to_reply
    from nullion.runtime import bootstrap_persistent_runtime

    monkeypatch.setenv("NULLION_DATA_DIR", str(tmp_path / "data"))
    runtime = bootstrap_persistent_runtime(tmp_path / "runtime.db")
    principal_id = "telegram:archive-filter"
    artifact_root = artifact_root_for_principal(principal_id)
    artifact_root.mkdir(parents=True, exist_ok=True)
    archive_path = artifact_root / "Archive.zip"
    workbook_path = artifact_root / "Archive_extracted_manifest.xlsx"
    archive_path.write_bytes(b"PK\x03\x04zip")
    workbook_path.write_bytes(b"PK\x03\x04xlsx")
    tool_results = [
        ToolResult(
            "download",
            "file_download",
            "completed",
            {"artifact_path": str(archive_path), "artifact_paths": [str(archive_path)]},
        ),
        ToolResult(
            "extract",
            "archive_extract",
            "completed",
            {"artifact_path": str(workbook_path), "artifact_paths": [str(workbook_path)]},
        ),
    ]

    reply = _append_chat_artifacts_to_reply(
        runtime,
        reply="Listo.",
        artifact_paths=[],
        prompt="crear hoja de cálculo",
        principal_id=principal_id,
        tool_results=tool_results,
        required_attachment_extensions=(".xlsx",),
    )

    assert f"MEDIA:{workbook_path}" in reply
    assert str(archive_path) not in reply


def test_turn_artifact_filter_excludes_intermediate_zip_when_xlsx_is_required(tmp_path) -> None:
    from nullion.chat_operator import _filter_paths_to_requested_deliverables_if_present

    archive_path = tmp_path / "Archive.zip"
    workbook_path = tmp_path / "Archive_extracted_manifest.xlsx"
    archive_path.write_bytes(b"PK\x03\x04zip")
    workbook_path.write_bytes(b"PK\x03\x04xlsx")

    filtered = _filter_paths_to_requested_deliverables_if_present(
        [str(archive_path), str(workbook_path)],
        requested_extensions=(".xlsx",),
    )

    assert filtered == [str(workbook_path)]


def test_execution_state_narration_reports_latest_tool_and_live_information_paths() -> None:
    completed = build_tool_execution_facts_from_tool_results([ToolResult("inv-ok", "web_search", "completed", {})])
    assert _render("स्थिती", *completed, claims=(DraftOperationalClaim.EXECUTION_STATE_REQUESTED,)) == "Execution state: preferred plugin path."

    denied = build_tool_execution_facts_from_tool_results(
        [ToolResult("inv-deny", "web_fetch", "denied", {"reason": "approval_required"})]
    )
    assert _render("状態", *denied, claims=(DraftOperationalClaim.EXECUTION_STATE_REQUESTED,)).startswith("Execution state: approval required.")

    nonterminal = build_tool_execution_facts_from_tool_results([ToolResult("inv-run", "web_fetch", "queued", {})])
    assert _render("estado", *nonterminal, claims=(DraftOperationalClaim.EXECUTION_STATE_REQUESTED,)) == "Execution state: in progress."

    for resolution, expected in [
        (LiveInformationResolution.PREFERRED_PLUGIN_PATH.value, "Execution state: preferred plugin path."),
        (LiveInformationResolution.CORE_FALLBACK.value, "Execution state: core fallback path."),
        (LiveInformationResolution.APPROVAL_REQUIRED.value, "Execution state: approval required."),
        (LiveInformationResolution.NO_USEFUL_RESULT.value, "Execution state: no useful result."),
        (LiveInformationResolution.BLOCKED.value, "Execution state: blocked."),
    ]:
        assert _render(
            "état",
            *build_live_information_resolution_facts([resolution]),
            claims=(DraftOperationalClaim.EXECUTION_STATE_REQUESTED,),
        ) == expected


def test_ungrounded_drafts_are_rewritten_and_canonical_replies_detected() -> None:
    assert text_mentions_approval_claim("This requires approval before I can run it") is False
    reply = _render("执行工具", claims=(DraftOperationalClaim.TOOL_ATTEMPTED,))
    assert "haven't actually run that tool path" in reply
    assert is_canonical_deferred_runtime_offer_reply(reply)

    success_reply = _render("完了", claims=(DraftOperationalClaim.TOOL_COMPLETED,))
    assert "haven't actually run a tool" in success_reply
    assert is_canonical_deferred_runtime_offer_reply(success_reply)
    assert is_canonical_deferred_runtime_offer_reply(None) is False


def test_live_information_fact_builder_ignores_unknown_resolution_values() -> None:
    facts = build_live_information_resolution_facts([
        "unknown",
        LiveInformationResolution.NO_USEFUL_RESULT.value,
    ])
    assert len(facts) == 1
    assert facts[0].payload["resolution"] == LiveInformationResolution.NO_USEFUL_RESULT.value
