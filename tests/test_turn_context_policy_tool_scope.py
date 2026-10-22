from __future__ import annotations

from types import SimpleNamespace

import pytest

from nullion import turn_context_policy
from nullion.runtime_cache import RuntimeCacheResult
from nullion.tools import ToolInvocation, ToolRegistry, ToolResult, ToolRiskLevel, ToolSideEffectClass, ToolSpec
from nullion.turn_context_policy import (
    ScopedTurnToolRegistry,
    TurnToolEvidence,
    TurnToolScopeDecision,
    build_turn_tool_scope_decision,
    build_turn_tool_evidence,
    materialize_mini_agent_tool_scope_registry,
    scoped_turn_tool_registry,
    tool_registry_allows_connector_context,
    tool_registry_allows_skill_pack_context,
    tool_registry_allows_skill_pack_prompt_context,
    turn_tool_registry_for_evidence,
    turn_tool_evidence_needs_model_scope_decision,
    turn_is_context_linked,
    turn_tool_scope_decision_may_apply,
)


def _spec(name: str, capability_tags: tuple[str, ...] = ()) -> ToolSpec:
    return ToolSpec(
        name=name,
        description=name,
        risk_level=ToolRiskLevel.LOW,
        side_effect_class=ToolSideEffectClass.READ,
        requires_approval=False,
        timeout_seconds=1,
        capability_tags=capability_tags,
    )


def _registry() -> ToolRegistry:
    registry = ToolRegistry()
    for spec in (
        _spec("file_read"),
        _spec("file_download"),
        _spec("archive_create"),
        _spec("archive_extract"),
        _spec("file_search"),
        _spec("file_write"),
        _spec("file_patch"),
        _spec("workspace_summary"),
        _spec("document_create"),
        _spec("pdf_create"),
        _spec("pdf_edit"),
        _spec("presentation_create"),
        _spec("spreadsheet_create"),
        _spec("create_cron", ("scheduler", "cron")),
        _spec("delete_cron", ("scheduler", "cron")),
        _spec("delete_reminder", ("scheduler", "reminder")),
        _spec("list_crons", ("scheduler", "cron")),
        _spec("list_reminders", ("scheduler", "reminder")),
        _spec("run_cron", ("scheduler", "cron")),
        _spec("set_reminder", ("scheduler", "reminder")),
        _spec("toggle_cron", ("scheduler", "cron")),
        _spec("update_cron", ("scheduler", "cron")),
        _spec("update_reminder", ("scheduler", "reminder")),
        _spec("skill_pack_read", ("skill_pack",)),
        _spec("chat_history_search", ("conversation_history", "account_read")),
        _spec("connector_request", ("connector",)),
        _spec("email_send", ("email", "connector", "account_write")),
        _spec("email_search", ("email", "connector", "account_read")),
        _spec("email_read", ("email", "connector", "account_read")),
        _spec("email_attachment_read", ("email", "connector", "account_read")),
        _spec("calendar_list", ("calendar", "connector", "account_read")),
        _spec("calendar_create", ("calendar", "connector", "account_write")),
        _spec("calendar_update", ("calendar", "connector", "account_write")),
        _spec("calendar_respond", ("calendar", "connector", "account_write")),
        _spec("calendar_delete", ("calendar", "connector", "account_write")),
        _spec("browser_open"),
        _spec("browser_navigate"),
        _spec("browser_extract_text"),
        _spec("browser_extract_detail"),
        _spec("browser_extract_items"),
        _spec("browser_find"),
        _spec("browser_image_collect"),
        _spec("browser_click_element"),
        _spec("browser_type_field"),
        _spec("browser_scroll"),
        _spec("browser_wait_for"),
        _spec("browser_assert_page_state"),
        _spec("browser_screenshot"),
        _spec("browser_run_js"),
        _spec("browser_close"),
        _spec("market_quote", ("market_data", "quote")),
        _spec("weather_forecast", ("weather",)),
        _spec("image_generate", ("media", "image_generation")),
        _spec("terminal_exec"),
    ):
        registry.register(
            spec,
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    return registry


class _ScopeClient:
    def __init__(self, text: str) -> None:
        self.text = text
        self.calls: list[dict[str, object]] = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return {"stop_reason": "end_turn", "content": [{"type": "text", "text": self.text}]}


def test_independent_unparented_turn_overrides_stale_task_frame_continuation() -> None:
    result = SimpleNamespace(
        turn=SimpleNamespace(disposition="independent", parent_turn_id=None),
        task_frame_continuation=SimpleNamespace(mode=SimpleNamespace(value="continue")),
    )

    assert not turn_is_context_linked(result)


@pytest.fixture(autouse=True)
def _empty_workspace_connections_by_default(monkeypatch) -> None:
    from nullion import connections

    monkeypatch.setattr(connections, "load_connection_registry", lambda *args, **kwargs: SimpleNamespace(connections=[]))


def test_ordinary_chat_hides_scheduler_skill_pack_and_connector_tools() -> None:
    client = _ScopeClient('{"web_action":"none","scheduler_action":"none","skill_pack_action":"none","confidence":0.91}')
    registry = _registry()

    scoped = scoped_turn_tool_registry(
        registry,
        evidence=build_turn_tool_evidence(user_message="¿Pronóstico para mañana?", conversation_result=None),
        model_client=client,
        user_message="¿Pronóstico para mañana?",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert names == {"request_tool_scope", "terminal_exec", "chat_history_search"}
    # Lightweight turns may include the cached compact installed-skill index so
    # the model can request an exact scope without loading full skill docs.
    assert tool_registry_allows_skill_pack_context(scoped) is True
    assert tool_registry_allows_skill_pack_prompt_context(scoped) is False
    assert tool_registry_allows_connector_context(scoped) is False
    assert client.calls == []


def test_shared_turn_registry_selector_keeps_scope_gateway_when_decision_is_skipped() -> None:
    registry = _registry()
    evidence = build_turn_tool_evidence(
        user_message="estado de mi agenda este fin de semana",
        conversation_result=None,
        saved_history_available=True,
    )

    selected = turn_tool_registry_for_evidence(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="estado de mi agenda este fin de semana",
        skip_tool_scope_decision=True,
    )

    names = {definition["name"] for definition in selected.list_tool_definitions()}
    assert names == {"request_tool_scope", "terminal_exec", "chat_history_search"}

    linked_evidence = TurnToolEvidence(
        context_linked=True,
        saved_history_available=True,
        prior_tool_scopes=("scheduler",),
    )
    linked_selected = turn_tool_registry_for_evidence(
        registry,
        evidence=linked_evidence,
        model_client=None,
        user_message="structured continuation",
        skip_tool_scope_decision=True,
    )
    linked_names = {definition["name"] for definition in linked_selected.list_tool_definitions()}
    assert "request_tool_scope" in linked_names
    assert "list_crons" in linked_names


def test_shared_turn_registry_selector_uses_empty_fast_path_without_scoped_tools() -> None:
    registry = ToolRegistry()
    registry.register(
        _spec("file_search", ("local_files",)),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    evidence = build_turn_tool_evidence(
        user_message="estado neutral",
        conversation_result=None,
        saved_history_available=True,
    )

    selected = turn_tool_registry_for_evidence(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="estado neutral",
        skip_tool_scope_decision=True,
    )

    assert {definition["name"] for definition in selected.list_tool_definitions()} == set()


def test_shared_turn_registry_selector_uses_empty_fast_path_with_full_registry_no_evidence() -> None:
    registry = _registry()
    evidence = build_turn_tool_evidence(
        user_message="estado neutral",
        conversation_result=None,
        saved_history_available=False,
    )

    selected = turn_tool_registry_for_evidence(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="estado neutral",
        skip_tool_scope_decision=True,
    )

    names = {definition["name"] for definition in selected.list_tool_definitions()}
    assert names == {"request_tool_scope", "market_quote", "weather_forecast", "terminal_exec", "chat_history_search"}
    assert not any(name.startswith("browser_") for name in names)
    assert "web_search" not in names


def test_plain_direct_safe_read_fast_path_does_not_classify_user_language() -> None:
    registry = _registry()
    evidence = build_turn_tool_evidence(
        user_message="Quel temps fait-il?",
        conversation_result=None,
        saved_history_available=False,
    )

    selected = turn_tool_registry_for_evidence(
        registry,
        evidence=evidence,
        model_client=None,
        user_message="Quel temps fait-il?",
        skip_tool_scope_decision=True,
    )

    names = {definition["name"] for definition in selected.list_tool_definitions()}
    assert names == {"request_tool_scope", "market_quote", "weather_forecast", "terminal_exec", "chat_history_search"}
    assert not any(name.startswith("browser_") for name in names)
    assert "web_search" not in names


def test_plain_turn_with_active_connector_app_id_forces_structured_scope_decision(monkeypatch) -> None:
    monkeypatch.setattr(turn_context_policy, "cached_active_connector_app_id_providers", lambda **kwargs: ())
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(None, False),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "display_name": "Example API Gateway",
                "structured_tools": ["connector_request"],
                "skill_pack_id": "example/api-gateway-skill",
                "active_app_ids": ["example-drive"],
                "reference_paths": ["references/example-drive/README.md"],
                "reference_path_count": 1,
            }
        ],
    )
    client = _ScopeClient(
        '{"web_action":"none","scheduler_action":"none","skill_pack_action":"connector",'
        '"connector_app_ids":["example-drive"],"connector_source_user_requested":true,'
        '"connector_source_evidence":"example-drive",'
        '"requested_tool_names":["connector_request","skill_pack_read"],"confidence":0.96}'
    )
    registry = _registry()

    scoped = scoped_turn_tool_registry(
        registry,
        evidence=build_turn_tool_evidence(user_message="acceso a example-drive?", conversation_result=None),
        model_client=client,
        user_message="acceso a example-drive?",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert {"request_tool_scope", "connector_request", "skill_pack_read"} <= names
    assert tool_registry_allows_connector_context(scoped) is True
    assert client.calls


def test_plain_text_turn_hides_source_media_tools_even_when_exact_requested() -> None:
    registry = _registry()
    registry.register(
        _spec("audio_transcribe", ("media",)),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    registry.register(
        _spec("image_extract_text", ("media",)),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    decision = TurnToolScopeDecision(
        requested_tool_names=("audio_transcribe", "image_extract_text"),
        required_tool_names=("audio_transcribe", "image_extract_text"),
        confidence=1.0,
        valid=True,
    )

    scoped = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(has_attachments=False, context_linked=False),
        tool_scope_decision=decision,
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert "audio_transcribe" not in names
    assert "image_extract_text" not in names


def test_attachment_turn_allows_source_media_tools_when_exact_requested() -> None:
    registry = _registry()
    registry.register(
        _spec("audio_transcribe", ("media",)),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    decision = TurnToolScopeDecision(
        requested_tool_names=("audio_transcribe",),
        required_tool_names=("audio_transcribe",),
        confidence=1.0,
        valid=True,
    )

    scoped = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(has_attachments=True, context_linked=False),
        tool_scope_decision=decision,
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert "audio_transcribe" in names


def test_context_linked_turn_does_not_expose_all_tools_without_structured_scope() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=TurnToolEvidence(
            context_linked=True,
            prior_tool_scopes=("web", "scheduler", "connector", "skill_pack"),
        ),
        tool_scope_decision=TurnToolScopeDecision(valid=True),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}

    assert "request_tool_scope" in names
    assert "list_crons" in names
    assert "connector_request" in names
    assert "skill_pack_read" in names
    assert "browser_navigate" not in names
    assert "browser_extract_text" not in names
    assert "file_search" not in names
    assert "terminal_exec" in names
    assert "spreadsheet_create" not in names
    assert "document_create" not in names


def test_plain_turn_scope_tool_contract_requires_scope_before_unavailable_reply() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral independent turn", conversation_result=None),
        model_client=None,
        user_message="neutral independent turn",
    )

    definitions = {definition["name"]: definition for definition in scoped.list_tool_definitions()}
    request_scope = definitions["request_tool_scope"]
    description = request_scope["description"]
    capability_description = request_scope["input_schema"]["properties"]["capabilities"]["description"]
    artifact_description = request_scope["input_schema"]["properties"]["artifact_extensions"]["description"]
    embedded_media_description = request_scope["input_schema"]["properties"]["embedded_media_artifact_extensions"]["description"]

    assert "Call this before saying a registered capability is unavailable" in description
    assert "web" in description
    assert "public source data" in description
    assert "connector" in description
    assert "local_shell" in description
    assert "scheduler_run" in description
    assert "scheduler_read/run/mutate" in capability_description
    assert "local_files" in capability_description
    assert "local_shell" in capability_description
    assert "conversation_history" in description
    assert "embedded_media_artifact_extensions" in description
    assert "artifact_extensions alone" in artifact_description
    assert "embedded media bytes" in description
    assert "inside the artifact itself" in embedded_media_description
    assert "not merely linked" in embedded_media_description
    assert "conversation_history" in request_scope["input_schema"]["properties"]["capabilities"]["items"]["enum"]
    assert "chat_history_search" in request_scope["input_schema"]["properties"]["tool_names"]["items"]["enum"]
    assert "file_search" in request_scope["input_schema"]["properties"]["tool_names"]["items"]["enum"]
    assert "file_patch" in request_scope["input_schema"]["properties"]["tool_names"]["items"]["enum"]
    assert "workspace_summary" in request_scope["input_schema"]["properties"]["tool_names"]["items"]["enum"]
    assert "toggle_cron" in request_scope["input_schema"]["properties"]["tool_names"]["items"]["enum"]
    assert "set_reminder" in request_scope["input_schema"]["properties"]["tool_names"]["items"]["enum"]
    assert "delete_reminder" in request_scope["input_schema"]["properties"]["tool_names"]["items"]["enum"]
    assert "update_reminder" in request_scope["input_schema"]["properties"]["tool_names"]["items"]["enum"]
    assert "browser_open" in request_scope["input_schema"]["properties"]["tool_names"]["items"]["enum"]
    assert "browser_image_collect" in request_scope["input_schema"]["properties"]["tool_names"]["items"]["enum"]
    assert "browser_extract_detail" in request_scope["input_schema"]["properties"]["tool_names"]["items"]["enum"]
    assert "browser_extract_items" in request_scope["input_schema"]["properties"]["tool_names"]["items"]["enum"]
    assert "browser_run_js" in request_scope["input_schema"]["properties"]["tool_names"]["items"]["enum"]
    assert ".html" in request_scope["input_schema"]["properties"]["embedded_media_artifact_extensions"]["items"]["enum"]


def test_ordinary_chat_keeps_safe_local_media_available_without_classifier_latency() -> None:
    client = _ScopeClient('{"web_action":"none","scheduler_action":"none","skill_pack_action":"none","confidence":0.91}')

    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral independent creative turn", conversation_result=None),
        model_client=client,
        user_message="neutral independent creative turn",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert names == {"request_tool_scope", "terminal_exec", "chat_history_search"}
    assert "image_generate" not in names
    assert "weather_forecast" not in names
    assert "file_read" not in names
    assert "terminal_exec" in names
    assert "run_cron" not in names
    assert "create_cron" not in names
    assert "delete_cron" not in names
    assert client.calls == []


def test_ordinary_chat_keeps_read_only_public_web_research_available_without_classifier_latency() -> None:
    client = _ScopeClient('{"web_action":"none","scheduler_action":"none","skill_pack_action":"none","confidence":0.91}')

    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="current public information", conversation_result=None),
        model_client=client,
        user_message="current public information",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert names == {"request_tool_scope", "terminal_exec", "chat_history_search"}
    assert "browser_navigate" not in names
    assert "connector_request" not in names
    assert "list_crons" not in names
    assert "skill_pack_read" not in names
    assert "file_read" not in names
    assert "terminal_exec" in names
    assert "run_cron" not in names
    assert "create_cron" not in names
    assert client.calls == []


def test_prior_tool_scopes_do_not_force_scope_classifier_without_structured_context() -> None:
    evidence = build_turn_tool_evidence(
        user_message="neutral independent turn",
        conversation_result=None,
        prior_tool_scopes=("connector", "scheduler", "web"),
    )

    assert evidence.prior_tool_scopes == ("connector", "scheduler", "web")
    assert turn_tool_scope_decision_may_apply(evidence, user_message="neutral independent turn") is False


def test_cached_active_connector_app_id_can_make_scope_decision_apply(monkeypatch) -> None:
    monkeypatch.setattr(
        turn_context_policy,
        "cached_active_connector_app_id_providers",
        lambda *args, **kwargs: ({"active_app_ids": ("example-drive",)},),
    )
    evidence = build_turn_tool_evidence(user_message="acceso a example-drive?", conversation_result=None)

    assert turn_tool_scope_decision_may_apply(evidence, user_message="acceso a example-drive?") is True


def test_cached_active_connector_provider_does_not_wake_scope_classifier_without_signal(monkeypatch) -> None:
    monkeypatch.setattr(
        turn_context_policy,
        "active_connector_provider_context_snapshot",
        lambda *, allow_runtime_load=True: (
            {
                "provider_id": "google-mail",
                "active_app_ids": ("google-mail",),
                "structured_tools": ("email_search", "email_read"),
            },
        ),
    )
    monkeypatch.setattr(
        turn_context_policy,
        "cached_active_connector_app_id_providers",
        lambda *args, **kwargs: ({"active_app_ids": ("google-mail",)},),
    )
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: (_ for _ in ()).throw(AssertionError("provider context load must stay off hot path")),
    )
    evidence = build_turn_tool_evidence(user_message="neutral independent turn", conversation_result=None)

    assert turn_tool_scope_decision_may_apply(evidence, user_message="neutral independent turn") is False


def test_uncached_connector_app_ids_do_not_load_provider_context_on_hot_path(monkeypatch) -> None:
    cache_allow_persistent_values: list[bool] = []

    def fake_cached_active_connector_app_id_providers(
        *,
        allow_persistent: bool = True,
    ) -> tuple[dict[str, tuple[str, ...]], ...]:
        cache_allow_persistent_values.append(allow_persistent)
        if allow_persistent:
            raise AssertionError("persistent connector cache read must stay off hot path")
        return ()

    monkeypatch.setattr(
        turn_context_policy,
        "active_connector_provider_context_snapshot",
        lambda *, allow_runtime_load=True: (),
    )
    monkeypatch.setattr(
        turn_context_policy,
        "cached_active_connector_app_id_providers",
        fake_cached_active_connector_app_id_providers,
    )
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: (_ for _ in ()).throw(AssertionError("provider context load must stay off hot path")),
    )
    evidence = build_turn_tool_evidence(user_message="neutral independent turn", conversation_result=None)

    assert turn_tool_scope_decision_may_apply(evidence, user_message="neutral independent turn") is False
    assert cache_allow_persistent_values == [False]


def test_cached_connector_app_fragment_does_not_make_scope_decision_apply(monkeypatch) -> None:
    monkeypatch.setattr(
        turn_context_policy,
        "cached_active_connector_app_id_providers",
        lambda *args, **kwargs: ({"active_app_ids": ("google-calendar",)},),
    )
    evidence = build_turn_tool_evidence(
        user_message="what is on my calendar tomorrow",
        conversation_result=None,
    )

    assert turn_tool_scope_decision_may_apply(
        evidence,
        user_message="what is on my calendar tomorrow",
    ) is False


def test_explicit_connector_tool_identifier_can_select_scope_without_app_id(monkeypatch) -> None:
    provider = {
        "active_app_ids": ("google-mail",),
        "structured_tools": ("email_search", "email_read", "email_attachment_read"),
    }
    monkeypatch.setattr(
        turn_context_policy,
        "cached_active_connector_app_id_providers",
        lambda *args, **kwargs: (provider,),
    )
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [provider],
    )
    client = _ScopeClient('{"web_action":"none","scheduler_action":"none","confidence":0.95}')
    message = "email_search"
    evidence = build_turn_tool_evidence(user_message=message, conversation_result=None)

    assert turn_tool_scope_decision_may_apply(evidence, user_message=message) is True

    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=evidence,
        model_client=client,
        user_message=message,
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert {"request_tool_scope", "email_search", "email_read"} <= names
    assert "email_attachment_read" not in names
    assert client.calls == []


def test_explicit_connector_attachment_scope_can_select_email_attachment_read(monkeypatch) -> None:
    provider = {
        "active_app_ids": ("google-mail",),
        "structured_tools": ("email_search", "email_read", "email_attachment_read"),
    }
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [provider],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="opaque connector source request", conversation_result=None),
        tool_scope_decision=TurnToolScopeDecision(
            skill_pack_action="connector",
            connector_app_ids=("google-mail",),
            connector_source_user_requested=True,
            requested_tool_names=("email_attachment_read",),
            required_tool_names=("email_attachment_read",),
            confidence=1.0,
            valid=True,
        ),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert "email_attachment_read" in names


def test_explicit_connector_identifier_overrides_wrong_weather_scope(monkeypatch) -> None:
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(None, False),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        turn_context_policy,
        "cached_active_connector_app_id_providers",
        lambda *args, **kwargs: ({"active_app_ids": ("google-calendar",)},),
    )
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_example_gateway_skill",
                "structured_tools": ["connector_request", "calendar_list"],
                "active_app_ids": ["google-calendar"],
            }
        ],
    )
    registry = _registry()
    client = _ScopeClient(
        '{"web_action":"none","scheduler_action":"none","skill_pack_action":"none",'
        '"requested_tool_names":["weather_forecast"],"confidence":0.95}'
    )

    scoped = scoped_turn_tool_registry(
        registry,
        evidence=build_turn_tool_evidence(user_message="agenda google-calendar manana", conversation_result=None),
        model_client=client,
        user_message="agenda google-calendar manana",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert client.calls
    assert "calendar_list" in names
    assert "weather_forecast" not in names


def test_saved_conversation_history_is_searchable_from_an_independent_turn() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="neutral independent turn",
            conversation_result=None,
            saved_history_available=True,
        ),
        model_client=None,
        user_message="neutral independent turn",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}

    assert names == {"request_tool_scope", "terminal_exec", "chat_history_search"}


def test_context_linked_saved_conversation_history_exposes_history_search_without_classifier() -> None:
    from nullion.conversation_runtime import ConversationTurnDisposition

    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="neutral followup",
            conversation_result=SimpleNamespace(
                turn=SimpleNamespace(parent_turn_id="turn-1", disposition=ConversationTurnDisposition.CONTINUE)
            ),
            saved_history_available=True,
        ),
        model_client=None,
        user_message="neutral followup",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}

    assert names == {"request_tool_scope", "chat_history_search", "terminal_exec"}


def test_prior_tool_scopes_do_not_force_scope_classifier_with_structured_context() -> None:
    from nullion.conversation_runtime import ConversationTurnDisposition

    conversation_result = SimpleNamespace(
        turn=SimpleNamespace(parent_turn_id="turn-1", disposition=ConversationTurnDisposition.CONTINUE)
    )
    evidence = build_turn_tool_evidence(
        user_message="structured followup",
        conversation_result=conversation_result,
        prior_tool_scopes=("connector",),
    )

    assert evidence.context_linked is True
    assert turn_tool_scope_decision_may_apply(evidence, user_message="structured followup") is False


def test_prior_skill_scopes_do_not_load_full_skill_pack_prompt_context() -> None:
    from nullion.conversation_runtime import ConversationTurnDisposition

    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="structured followup",
            conversation_result=SimpleNamespace(
                turn=SimpleNamespace(parent_turn_id="turn-1", disposition=ConversationTurnDisposition.CONTINUE)
            ),
            prior_tool_scopes=("skill_pack", "connector"),
        ),
        model_client=_ScopeClient('{"web_action":"none","scheduler_action":"none","skill_pack_action":"none"}'),
        user_message="structured followup",
    )

    assert tool_registry_allows_skill_pack_context(scoped) is True
    assert tool_registry_allows_skill_pack_prompt_context(scoped) is False


def test_structured_scope_decision_allows_requested_special_tool_families() -> None:
    client = _ScopeClient('{"web_action":"live_research","scheduler_action":"run","skill_pack_action":"reference","confidence":0.88}')

    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="https://scope-decision.example.com/research", conversation_result=None),
        model_client=client,
        user_message="https://scope-decision.example.com/research",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert {"list_crons", "run_cron", "skill_pack_read", "browser_navigate"} <= names
    assert "file_read" not in names
    assert tool_registry_allows_skill_pack_context(scoped) is True
    assert tool_registry_allows_skill_pack_prompt_context(scoped) is True
    assert tool_registry_allows_connector_context(scoped) is False


def test_tool_scope_classifier_uses_short_timeout(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_TOOL_SCOPE_CLASSIFIER_TIMEOUT_SECONDS", "2.5")
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(None, False),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)
    client = _ScopeClient('{"web_action":"live_research","scheduler_action":"none","skill_pack_action":"none","confidence":0.88}')

    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="https://scope-timeout-contract.example.com",
            conversation_result=None,
        ),
        model_client=client,
        user_message="https://scope-timeout-contract.example.com",
    )

    assert "browser_navigate" in {definition["name"] for definition in scoped.list_tool_definitions()}
    assert client.calls
    assert client.calls[0]["timeout"] == 2.5


def test_scheduler_scope_does_not_use_prompt_job_reference_fallback() -> None:
    client = _ScopeClient('{"web_action":"none","scheduler_action":"run","skill_pack_action":"none","confidence":0.9}')

    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="Trigger my Nulliøn tracker", conversation_result=None),
        model_client=client,
        user_message="Trigger my Nulliøn tracker",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert names == {"request_tool_scope", "terminal_exec", "chat_history_search"}
    assert client.calls == []


def test_scheduler_job_reference_requires_structured_scope_request_without_model_classifier() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="Run 5b4505db", conversation_result=None),
        model_client=None,
        user_message="Run 5b4505db",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert names == {"request_tool_scope", "terminal_exec", "chat_history_search"}
    assert "delete_cron" not in names


def test_internal_scope_request_structurally_adds_scheduler_tools_without_prompt_tokens() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["scheduler_run"]},
        )
    )

    assert result.status == "completed"
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {"request_tool_scope", "list_crons", "run_cron"} <= names
    assert "delete_cron" not in names


def test_scheduler_run_exact_scope_keeps_list_crons_dependency() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["scheduler_run"], "tool_names": ["run_cron"]},
        )
    )

    assert result.status == "completed"
    assert {"list_crons", "run_cron"} <= set(result.output["available_tools"])
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {"request_tool_scope", "list_crons", "run_cron"} <= names
    assert "delete_cron" not in names


def test_scheduler_action_run_upgrades_read_scope_to_run_tools() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["scheduler_read"],
                "scheduler_action": "run",
                "scheduler_selection_policy": "user_selected",
            },
        )
    )

    assert result.status == "completed"
    assert result.output["scheduler_action"] == "run"
    assert result.output["scheduler_selection_policy"] == "user_selected"
    assert {"list_crons", "run_cron"} <= set(result.output["available_tools"])
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {"request_tool_scope", "list_crons", "run_cron"} <= names
    assert "delete_cron" not in names


def test_scheduler_inspect_scope_filters_conflicting_run_tool() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="inspect",
            requested_tool_names=("list_crons", "run_cron"),
            required_tool_names=("run_cron",),
            confidence=1.0,
            valid=True,
        ),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    denied = scoped.invoke(ToolInvocation("run", "run_cron", "telegram:1", {"name": "task-a"}))

    assert names == {"request_tool_scope", "list_crons", "terminal_exec", "chat_history_search"}
    assert denied.status == "denied"
    assert denied.output["reason"] == "tool_requires_structured_turn_scope"


def test_scheduler_scope_validator_drops_run_tool_from_inspect_decision(monkeypatch) -> None:
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(None, False),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)
    client = _ScopeClient(
        '{"web_action":"none","scheduler_action":"inspect","skill_pack_action":"none",'
        '"requested_tool_names":["list_crons","run_cron"],"required_tool_names":["run_cron"],"confidence":0.91}'
    )

    decision = build_turn_tool_scope_decision(
        model_client=client,
        user_message="structured scheduled inventory",
        evidence=build_turn_tool_evidence(user_message="structured scheduled inventory", conversation_result=None),
        registry=_registry(),
        force_model_decision=True,
    )

    assert decision.scheduler_action == "inspect"
    assert decision.requested_tool_names == ("list_crons",)
    assert decision.required_tool_names == ()


def test_scheduler_inspect_scope_rejects_run_effect_escalation_inside_same_turn() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            scheduler_action="inspect",
            requested_tool_names=("list_crons",),
            confidence=1.0,
            valid=True,
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope-run",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["scheduler_run"],
                "tool_names": ["run_cron"],
                "scheduler_selection_policy": "user_selected",
            },
        )
    )

    assert result.status == "denied"
    assert result.output["reason"] == "turn_effect_not_authorized"
    assert widened is scoped
    assert not widened.can_invoke_tool("run_cron")


def test_context_linked_scheduler_mutation_followup_stays_read_only() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=TurnToolEvidence(
            context_linked=True,
            prior_tool_scopes=("scheduler", "scheduler_mutate"),
        ),
        tool_scope_decision=TurnToolScopeDecision(valid=True),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope-run",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["scheduler_run"],
                "tool_names": ["run_cron"],
                "required_tool_names": ["run_cron"],
                "scheduler_selection_policy": "delegate_one",
            },
        )
    )

    assert result.status == "completed"
    assert result.output["capabilities"] == ["scheduler_read"]
    assert result.output["scheduler_action"] == "inspect"
    assert result.output["scheduler_selection_policy"] == "none"
    assert "list_crons" in result.output["available_tools"]
    assert "run_cron" not in result.output["available_tools"]
    assert "run_cron" not in result.output["required_tool_names"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "list_crons" in names
    assert "run_cron" not in names


def test_prior_scheduler_mutation_followup_stays_read_only_without_context_link() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=TurnToolEvidence(
            prior_tool_scopes=("scheduler", "scheduler_mutate"),
        ),
        tool_scope_decision=TurnToolScopeDecision(valid=True),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope-run",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["scheduler_run"],
                "tool_names": ["run_cron"],
                "required_tool_names": ["run_cron"],
                "scheduler_selection_policy": "delegate_one",
            },
        )
    )

    assert result.status == "completed"
    assert result.output["capabilities"] == ["scheduler_read"]
    assert "run_cron" not in result.output["available_tools"]
    assert "run_cron" not in result.output["required_tool_names"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "list_crons" in names
    assert "run_cron" not in names


def test_prior_scheduler_mutation_followup_blocks_user_selected_run_scope() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=TurnToolEvidence(
            prior_tool_scopes=("scheduler", "scheduler_mutate"),
        ),
        tool_scope_decision=TurnToolScopeDecision(valid=True),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope-run",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["scheduler_run"],
                "tool_names": ["list_crons", "run_cron"],
                "required_tool_names": ["run_cron"],
                "scheduler_selection_policy": "user_selected",
            },
        )
    )

    assert result.status == "completed"
    assert result.output["capabilities"] == ["scheduler_read"]
    assert result.output["scheduler_action"] == "inspect"
    assert result.output["scheduler_selection_policy"] == "none"
    assert "list_crons" in result.output["available_tools"]
    assert "run_cron" not in result.output["available_tools"]
    assert "run_cron" not in result.output["required_tool_names"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "list_crons" in names
    assert "run_cron" not in names


def test_existing_named_artifact_does_not_hide_current_artifact_tool_scope() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=TurnToolEvidence(
            requested_extensions=(".csv",),
            existing_named_artifact_extensions=(".csv",),
        ),
        tool_scope_decision=TurnToolScopeDecision(valid=True),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert "file_write" in names


def test_requested_browser_tools_require_web_scope_or_url_evidence() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            web_action="none",
            requested_tool_names=("browser_navigate", "file_download"),
            confidence=1.0,
            valid=True,
        ),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    denied = scoped.invoke(ToolInvocation("browser", "browser_navigate", "telegram:1", {"url": "https://example.test"}))

    assert "browser_navigate" not in names
    assert "file_download" not in names
    assert denied.status == "denied"
    assert denied.output["reason"] == "tool_requires_structured_turn_scope"


def test_url_png_artifact_contract_exposes_browser_screenshot_without_prior_context() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="https://example.test/page",
            conversation_result=None,
            requested_extensions=(".png",),
        ),
        tool_scope_decision=TurnToolScopeDecision(
            requested_tool_names=("browser_screenshot",),
            requested_artifact_extensions=(".png",),
            confidence=1.0,
            valid=True,
        ),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    result = scoped.invoke(ToolInvocation("shot", "browser_screenshot", "telegram:1", {}))

    assert "browser_screenshot" in names
    assert result.status == "completed"


def test_requested_artifact_creator_requires_format_contract() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            requested_tool_names=("pdf_create",),
            confidence=1.0,
            valid=True,
        ),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    denied = scoped.invoke(ToolInvocation("pdf", "pdf_create", "telegram:1", {"output_path": "report.pdf"}))

    assert "pdf_create" not in names
    assert denied.status == "denied"
    assert denied.output["reason"] == "tool_requires_structured_turn_scope"


def test_requested_skill_pack_read_requires_skill_pack_scope() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            skill_pack_action="none",
            requested_tool_names=("skill_pack_read",),
            confidence=1.0,
            valid=True,
        ),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    denied = scoped.invoke(ToolInvocation("skill", "skill_pack_read", "telegram:1", {"path": "README.md"}))

    assert "skill_pack_read" not in names
    assert denied.status == "denied"
    assert denied.output["reason"] == "tool_requires_structured_turn_scope"


def test_scheduler_scope_prompt_distinguishes_run_from_inspect(monkeypatch) -> None:
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(None, False),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)
    client = _ScopeClient('{"web_action":"none","scheduler_action":"run","skill_pack_action":"none","requested_tool_names":["run_cron"],"confidence":0.9}')

    decision = build_turn_tool_scope_decision(
        model_client=client,
        user_message="neutral scheduler request",
        evidence=build_turn_tool_evidence(user_message="neutral scheduler request", conversation_result=None),
        registry=_registry(),
        force_model_decision=True,
    )

    system_text = str(client.calls[0].get("system") or "") + " " + " ".join(
        str(block.get("text") or "")
        for message in client.calls[0]["messages"]
        for block in (message.get("content") or [])
        if isinstance(block, dict)
    )
    assert "execute existing scheduled tasks" in system_text
    assert "run_cron" in system_text
    assert decision.scheduler_action == "run"
    assert decision.requested_tool_names == ("run_cron",)


def test_internal_scheduler_mutate_scope_exposes_registered_toggle_tool() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["scheduler_mutate"]},
        )
    )

    assert result.status == "completed"
    assert "toggle_cron" in result.output["available_tools"]
    assert "set_reminder" in result.output["available_tools"]
    assert "delete_reminder" not in result.output["available_tools"]
    assert "delete_cron" not in result.output["available_tools"]
    assert "update_reminder" in result.output["available_tools"]
    assert "run_cron" not in result.output["available_tools"]
    assert "enable_cron" not in result.output["available_tools"]
    assert "disable_cron" not in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {"request_tool_scope", "list_crons", "toggle_cron", "set_reminder", "update_reminder"} <= names
    assert "delete_cron" not in names and "delete_reminder" not in names
    assert "run_cron" not in names
    assert "enable_cron" not in names
    assert "disable_cron" not in names


def test_scheduler_mutate_scope_preserves_all_workspace_toggle_target() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["scheduler_mutate"],
                "scheduler_action": "mutate",
                "scheduler_toggle_enabled": True,
                "scheduler_target_scope": "all_current_workspace",
                "tool_names": ["list_crons", "toggle_cron"],
                "required_tool_names": ["toggle_cron"],
            },
        )
    )

    assert result.status == "completed"
    assert result.output["scheduler_action"] == "mutate"
    assert result.output["scheduler_toggle_enabled"] is True
    assert result.output["scheduler_target_scope"] == "all_current_workspace"
    assert "toggle_cron" in result.output["required_tool_names"]
    decision = widened.turn_tool_scope_decision
    assert decision.scheduler_action == "mutate"
    assert decision.scheduler_toggle_enabled is True
    assert decision.scheduler_target_scope == "all_current_workspace"
    assert decision.required_tool_names == ("toggle_cron",)


def test_plain_turn_scope_requires_structured_scope_for_reminder_tools() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}

    assert names == {"request_tool_scope", "terminal_exec", "chat_history_search"}
    assert "set_reminder" not in names
    assert "list_reminders" not in names
    assert "delete_reminder" not in names
    assert "update_reminder" not in names
    assert "create_cron" not in names
    assert "delete_cron" not in names
    assert "run_cron" not in names


def test_scheduler_mutate_exact_scope_exposes_delete_reminder_with_read_dependency() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["scheduler_mutate"], "tool_names": ["delete_reminder"]},
        )
    )

    assert result.status == "completed"
    assert result.output["required_tool_names"] == ["delete_reminder"]
    assert {"list_reminders", "delete_reminder"} <= set(result.output["available_tools"])
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {"request_tool_scope", "list_reminders", "delete_reminder"} <= names


def test_scheduler_mutate_exact_scope_exposes_update_reminder_with_read_dependency() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["scheduler_mutate"], "tool_names": ["update_reminder"]},
        )
    )

    assert result.status == "completed"
    assert result.output["required_tool_names"] == ["update_reminder"]
    assert {"list_reminders", "update_reminder"} <= set(result.output["available_tools"])
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {"request_tool_scope", "list_reminders", "update_reminder"} <= names


def test_internal_scope_request_structurally_adds_local_shell_tool_without_prompt_tokens() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["local_shell"]},
        )
    )

    assert result.status == "completed"
    assert result.output["available_tools"] == ["terminal_exec"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {"request_tool_scope", "terminal_exec"} <= names
    assert "browser_navigate" not in names


def test_local_file_scope_uses_terminal_exec_only_when_dedicated_tools_are_unavailable() -> None:
    registry = ToolRegistry()
    registry.register(_spec("terminal_exec"), lambda invocation: None)
    scoped = scoped_turn_tool_registry(
        registry,
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["local_files"]},
        )
    )

    assert result.status == "completed"
    assert result.output["available_tools"] == ["terminal_exec"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {"request_tool_scope", "terminal_exec"} <= names


def test_internal_scope_request_structurally_adds_local_file_search_tool() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["local_files"]},
        )
    )

    assert result.status == "completed"
    assert {
        "file_read",
        "file_search",
        "terminal_exec",
        "workspace_summary",
    } <= set(result.output["available_tools"])
    assert "archive_create" not in result.output["available_tools"]
    assert "file_patch" not in result.output["available_tools"]
    assert "file_write" not in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {
        "request_tool_scope",
        "file_read",
        "file_search",
        "terminal_exec",
        "workspace_summary",
    } <= names
    assert "archive_extract" not in names
    assert "archive_create" not in names
    assert "file_patch" not in names
    assert "file_write" not in names
    assert "browser_navigate" not in names


def test_internal_scope_request_uses_typed_artifact_extension_for_spreadsheet_tool() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="neutral",
            conversation_result=None,
            requested_extensions=(".xlsx",),
        ),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["local_files"], "artifact_extensions": [".xlsx"]},
        )
    )

    assert result.status == "completed"
    assert result.output["artifact_extensions"] == [".xlsx"]
    assert "spreadsheet_create" in result.output["available_tools"]
    assert "file_write" not in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "spreadsheet_create" in names
    assert "file_write" not in names


def test_internal_scope_request_uses_file_write_only_for_csv_artifact_extension() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["local_files"], "artifact_extensions": [".csv"]},
        )
    )

    assert result.status == "completed"
    assert result.output["artifact_extensions"] == [".csv"]
    assert "file_write" in result.output["available_tools"]
    assert "spreadsheet_create" not in result.output["available_tools"]
    assert "terminal_exec" not in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "file_write" in names
    assert "spreadsheet_create" not in names
    assert "terminal_exec" in names


def test_local_file_scope_cannot_promote_listing_target_into_artifact_delivery() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="Find the PDF file named stage_file_search_delivery_guard_1234.pdf",
            conversation_result=None,
        ),
        model_client=None,
        user_message="Find the PDF file named stage_file_search_delivery_guard_1234.pdf",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["local_files"],
                "tool_names": ["file_search", "pdf_create"],
                "artifact_extensions": [".pdf"],
            },
        )
    )

    assert result.status == "completed"
    assert result.output["artifact_extensions"] == []
    assert "file_search" in result.output["available_tools"]
    assert "pdf_create" not in result.output["available_tools"]
    assert widened.turn_tool_scope_decision.requested_artifact_extensions == ()
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "file_search" in names
    assert "pdf_create" not in names


def test_internal_scope_request_uses_embedded_media_extension_as_artifact_contract() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["local_files"], "embedded_media_artifact_extensions": [".xlsx"]},
        )
    )

    assert result.status == "completed"
    assert result.output["artifact_extensions"] == [".xlsx"]
    assert result.output["embedded_media_artifact_extensions"] == [".xlsx"]
    assert "spreadsheet_create" in result.output["available_tools"]
    assert widened.turn_tool_scope_decision.required_embedded_media_extensions == (".xlsx",)
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "spreadsheet_create" in names
    assert "file_write" not in names


def test_internal_scope_request_accepts_required_embedded_media_alias() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["local_files"], "required_embedded_media_extensions": [".xlsx"]},
        )
    )

    assert result.status == "completed"
    assert result.output["artifact_extensions"] == [".xlsx"]
    assert result.output["embedded_media_artifact_extensions"] == [".xlsx"]
    assert widened.turn_tool_scope_decision.required_embedded_media_extensions == (".xlsx",)


def test_scope_request_drops_unrealizable_generated_media_contract_without_image_tool() -> None:
    registry = _registry()
    registry.unregister("image_generate")
    scoped = scoped_turn_tool_registry(
        registry,
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["image_generation"],
                "artifact_extensions": [".pdf", ".html"],
                "embedded_media_artifact_extensions": [".pdf", ".html"],
            },
        )
    )

    assert result.status == "completed"
    assert result.output["artifact_extensions"] == [".pdf", ".html"]
    assert result.output["embedded_media_artifact_extensions"] == []
    assert "image_generate" not in result.output["available_tools"]
    assert {"pdf_create", "file_write"} <= set(result.output["available_tools"])
    assert widened.turn_tool_scope_decision.requested_artifact_extensions == (".pdf", ".html")
    assert widened.turn_tool_scope_decision.required_embedded_media_extensions == ()


def test_initial_scope_decision_drops_unrealizable_embedded_media_contract_without_source_tool() -> None:
    registry = _registry()
    registry.unregister("image_generate")
    client = _ScopeClient(
        '{"web_action":"none","scheduler_action":"none","skill_pack_action":"none",'
        '"requested_tool_names":["pdf_create","file_write"],'
        '"requested_artifact_extensions":[".pdf",".html"],'
        '"required_embedded_media_extensions":[".pdf",".html"],'
        '"confidence":0.93}'
    )

    decision = build_turn_tool_scope_decision(
        model_client=client,
        user_message="structured artifact request",
        evidence=TurnToolEvidence(requested_extensions=(".pdf", ".pdf", ".html")),
        registry=registry,
        force_model_decision=True,
    )

    assert {"pdf_create", "file_write"} <= set(decision.requested_tool_names)
    assert decision.required_embedded_media_extensions == ()
    assert {".pdf", ".html"} <= set(decision.requested_artifact_extensions)


def test_scope_request_preserves_spreadsheet_embedded_media_contract_with_producer_tool() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=TurnToolEvidence(requested_extensions=(".xlsx",)),
        model_client=None,
        user_message="structured spreadsheet artifact turn",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-embedded-xlsx",
            "request_tool_scope",
            "web:1",
            {
                "capabilities": ["local_files"],
                "tool_names": ["spreadsheet_create"],
                "artifact_extensions": [".xlsx"],
                "embedded_media_artifact_extensions": [".xlsx"],
            },
        )
    )

    assert result.status == "completed"
    assert result.output["embedded_media_artifact_extensions"] == [".xlsx"]
    assert widened.turn_tool_scope_decision.required_embedded_media_extensions == (".xlsx",)


def test_embedded_web_media_scope_uses_image_collector_instead_of_shell() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="https://example.com/image.png",
            conversation_result=None,
        ),
        model_client=None,
        user_message="https://example.com/image.png",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["web", "local_shell"],
                "embedded_media_artifact_extensions": [".pdf"],
            },
        )
    )

    assert result.status == "completed"
    assert result.output["embedded_media_artifact_extensions"] == [".pdf"]
    assert "browser_image_collect" in result.output["available_tools"]
    assert "pdf_create" in result.output["available_tools"]
    assert "terminal_exec" not in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {"browser_image_collect", "pdf_create"} <= names
    assert "terminal_exec" in names


def test_web_typed_artifact_scope_does_not_collect_images_without_embedded_media_contract() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="https://example.com/",
            conversation_result=None,
        ),
        model_client=None,
        user_message="https://example.com/",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["web", "local_shell"],
                "artifact_extensions": [".xlsx"],
            },
        )
    )

    assert result.status == "completed"
    assert "browser_image_collect" not in result.output["available_tools"]
    assert "spreadsheet_create" in result.output["available_tools"]
    assert "terminal_exec" not in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "spreadsheet_create" in names
    assert "browser_image_collect" not in names
    assert "terminal_exec" in names


def test_exact_web_artifact_scope_does_not_keep_image_collector_without_embedded_media_contract() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="neutral",
            conversation_result=None,
        ),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["web", "local_files"],
                "tool_names": ["browser_navigate", "browser_extract_items"],
                "artifact_extensions": [".xlsx"],
            },
        )
    )

    assert result.status == "completed"
    assert "browser_extract_items" in result.output["available_tools"]
    assert "browser_image_collect" not in result.output["available_tools"]
    assert "spreadsheet_create" in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {"browser_extract_items", "spreadsheet_create"} <= names
    assert "browser_image_collect" not in names


def test_exact_visual_browser_tools_are_blocked_without_visual_artifact_contract() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="https://example.com",
            conversation_result=None,
        ),
        model_client=None,
        user_message="https://example.com",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["web"],
                "tool_names": ["browser_screenshot", "browser_image_collect", "browser_navigate"],
            },
        )
    )

    assert result.status == "completed"
    assert "browser_navigate" in result.output["available_tools"]
    assert "browser_screenshot" not in result.output["available_tools"]
    assert "browser_image_collect" not in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "browser_navigate" in names
    assert "browser_screenshot" not in names
    assert "browser_image_collect" not in names


def test_web_media_spreadsheet_scope_includes_browser_extraction_companions() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message=(
                "https://example.com/catalogo productos imagenes precios enlaces "
                "en archivo .xlsx"
            ),
            conversation_result=None,
        ),
        model_client=None,
        user_message="https://example.com/catalogo productos imagenes precios",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["local_files"],
                "artifact_extensions": [".xlsx"],
                "embedded_media_artifact_extensions": [".xlsx"],
                "tool_names": ["spreadsheet_create"],
            },
        )
    )

    assert result.status == "completed"
    assert "spreadsheet_create" in result.output["available_tools"]
    assert "browser_image_collect" in result.output["available_tools"]
    assert "browser_navigate" in result.output["available_tools"]
    assert "browser_extract_items" in result.output["available_tools"]
    assert "browser_run_js" in result.output["available_tools"]
    assert "terminal_exec" not in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {
        "browser_navigate",
        "browser_extract_items",
        "browser_run_js",
        "browser_image_collect",
        "spreadsheet_create",
    } <= names
    assert "terminal_exec" in names


def test_embedded_html_media_scope_includes_browser_image_collection_and_file_write() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="https://example.com/catalog",
            conversation_result=None,
        ),
        model_client=None,
        user_message="https://example.com/catalog",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["web", "local_files", "local_shell"],
                "artifact_extensions": [".html"],
                "embedded_media_artifact_extensions": [".html"],
                "tool_names": ["file_write"],
            },
        )
    )

    assert result.status == "completed"
    assert result.output["artifact_extensions"] == [".html"]
    assert result.output["embedded_media_artifact_extensions"] == [".html"]
    assert "file_write" in result.output["available_tools"]
    assert "browser_image_collect" in result.output["available_tools"]
    assert "browser_extract_items" in result.output["available_tools"]
    assert "browser_run_js" in result.output["available_tools"]
    assert "terminal_exec" not in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {
        "file_write",
        "browser_navigate",
        "browser_extract_items",
        "browser_run_js",
        "browser_image_collect",
    } <= names
    assert "terminal_exec" in names


@pytest.mark.parametrize(
    ("extension", "artifact_tool"),
    [
        (".docx", "document_create"),
        (".pdf", "pdf_create"),
        (".pptx", "presentation_create"),
        (".xlsx", "spreadsheet_create"),
    ],
)
def test_web_media_artifact_scope_includes_browser_extraction_companions(
    extension: str,
    artifact_tool: str,
) -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message=f"https://example.com/catalogo productos imagenes precios archivo {extension}",
            conversation_result=None,
        ),
        model_client=None,
        user_message=f"https://example.com/catalogo productos imagenes precios archivo {extension}",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["local_files"],
                "artifact_extensions": [extension],
                "embedded_media_artifact_extensions": [extension],
                "tool_names": [artifact_tool],
            },
        )
    )

    assert result.status == "completed"
    assert artifact_tool in result.output["available_tools"]
    assert "browser_image_collect" in result.output["available_tools"]
    assert "browser_navigate" in result.output["available_tools"]
    assert "browser_extract_items" in result.output["available_tools"]
    assert "browser_run_js" in result.output["available_tools"]
    assert "terminal_exec" not in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {
        artifact_tool,
        "browser_navigate",
        "browser_extract_items",
        "browser_run_js",
        "browser_image_collect",
    } <= names
    assert "terminal_exec" in names


def test_exact_image_collector_scope_for_web_media_artifact_keeps_browser_companions() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="https://example.com/catalogo productos imagenes precios",
            conversation_result=None,
        ),
        tool_scope_decision=TurnToolScopeDecision(
            requested_tool_names=("browser_image_collect", "spreadsheet_create"),
            requested_artifact_extensions=(".xlsx",),
            required_embedded_media_extensions=(".xlsx",),
            valid=True,
        ),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert {
        "browser_navigate",
        "browser_extract_items",
        "browser_run_js",
        "browser_image_collect",
        "spreadsheet_create",
    } <= names
    assert "terminal_exec" in names


def test_embedded_web_media_registry_keeps_core_shell_available() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="https://example.com/image.png",
            conversation_result=None,
        ),
        tool_scope_decision=TurnToolScopeDecision(
            requested_tool_names=("terminal_exec",),
            requested_artifact_extensions=(".docx",),
            required_embedded_media_extensions=(".docx",),
            valid=True,
        ),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert {"browser_image_collect", "document_create"} <= names
    assert "terminal_exec" in names


def test_embedded_media_artifact_scope_exposes_browser_image_collection_without_url_target() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="create a booklet and deck with embedded relevant current-turn media",
            conversation_result=None,
        ),
        tool_scope_decision=TurnToolScopeDecision(
            requested_tool_names=("document_create", "presentation_create"),
            requested_artifact_extensions=(".docx", ".pptx"),
            required_embedded_media_extensions=(".docx", ".pptx"),
            valid=True,
        ),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert {
        "browser_image_collect",
        "browser_navigate",
        "browser_extract_items",
        "browser_run_js",
        "document_create",
        "presentation_create",
    } <= names
    assert "terminal_exec" in names


def test_internal_scope_request_uses_typed_artifact_extension_for_document_tool() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["local_files"], "artifact_extensions": [".docx"]},
        )
    )

    assert result.status == "completed"
    assert result.output["artifact_extensions"] == [".docx"]
    assert "document_create" in result.output["available_tools"]
    assert "file_write" not in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "document_create" in names
    assert "file_write" not in names


@pytest.mark.parametrize("extension", [".htm", ".html", ".json", ".md", ".svg", ".txt", ".yaml", ".yml"])
def test_internal_scope_request_exposes_file_write_for_text_artifact_extensions(extension: str) -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["local_files"], "artifact_extensions": [extension]},
        )
    )

    assert result.status == "completed"
    assert result.output["artifact_extensions"] == [extension]
    assert "file_write" in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "file_write" in names


def test_internal_scope_request_exposes_conversation_history_tool() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral independent turn", conversation_result=None),
        model_client=None,
        user_message="neutral independent turn",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            invocation_id="scope-history",
            tool_name="request_tool_scope",
            principal_id="test",
            arguments={"capabilities": ["conversation_history"]},
        )
    )

    assert result.status == "completed"
    assert result.output["available_tools"] == ["chat_history_search"]
    assert "chat_history_search" in {definition["name"] for definition in widened.list_tool_definitions()}


def test_internal_local_file_scope_does_not_let_exact_hint_drop_file_search() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["local_files", "local_shell"],
                "tool_names": ["file_read", "file_write", "terminal_exec"],
            },
        )
    )

    assert result.status == "completed"
    assert {"file_read", "file_search", "file_write", "workspace_summary"} <= set(result.output["available_tools"])
    assert "terminal_exec" in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {"file_read", "file_search", "file_write", "workspace_summary"} <= names
    assert "terminal_exec" in names
    assert "file_patch" not in names


def test_tool_scope_prompt_advertises_local_files_before_shell(monkeypatch) -> None:
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(None, False),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)
    client = _ScopeClient(
        '{"web_action":"none","scheduler_action":"none","skill_pack_action":"none",'
        '"requested_tool_names":["file_search"],"confidence":0.91}'
    )

    turn_context_policy.build_turn_tool_scope_decision(
        model_client=client,
        user_message="structured local file task",
        evidence=build_turn_tool_evidence(user_message="structured local file task", conversation_result=None),
        registry=_registry(),
        force_model_decision=True,
    )

    payload = client.calls[0]["messages"][0]["content"][0]["text"]
    system = client.calls[0]["system"]
    assert "local_files" in payload
    assert 'requested_tool_names=["file_search"]' in system
    assert 'requested_tool_names=["file_patch"]' in system
    assert 'requested_tool_names=["workspace_summary"]' in system
    assert "dedicated file tools are unavailable or insufficient" in system


def test_tool_scope_recovery_prompt_includes_no_tool_draft(monkeypatch) -> None:
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(None, False),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)
    client = _ScopeClient(
        '{"web_action":"none","scheduler_action":"mutate","scheduler_toggle_enabled":true,'
        '"skill_pack_action":"none","requested_tool_names":["toggle_cron"],'
        '"required_tool_names":["toggle_cron"],"confidence":0.95}'
    )

    decision = build_turn_tool_scope_decision(
        model_client=client,
        user_message="structured scheduler mutation request",
        evidence=TurnToolEvidence(),
        registry=_registry(),
        force_model_decision=True,
        assistant_no_tool_draft="No verified scheduler mutation happened in the previous draft.",
    )

    prompt_payload = client.calls[0]["messages"][0]["content"][0]["text"]
    assert "assistant_no_tool_draft" in prompt_payload
    assert "No verified scheduler mutation happened" in prompt_payload
    assert '"available_direct_read_tools": ["calendar_list", "market_quote", "weather_forecast"]' in prompt_payload
    assert "available_direct_read_tools is authoritative structured runtime evidence" in client.calls[0]["system"]
    assert "assistant_no_tool_draft is diagnostic evidence only" in client.calls[0]["system"]
    assert "assistant_no_tool_draft is present" in client.calls[0]["system"]
    assert "after a scheduler read-only result" in client.calls[0]["system"]
    assert "do not leave the action as inspect" in client.calls[0]["system"]
    assert decision.scheduler_action == "mutate"
    assert decision.requested_tool_names == ("toggle_cron",)
    assert decision.required_tool_names == ("toggle_cron",)


def test_tool_scope_recovery_bypasses_stale_cached_inspect_decision(monkeypatch) -> None:
    cache_get_calls: list[object] = []
    cache_set_calls: list[object] = []

    def stale_cached_inspect(*args, **kwargs):  # noqa: ANN002, ANN003
        cache_get_calls.append((args, kwargs))
        return RuntimeCacheResult(
            {
                "web_action": "none",
                "scheduler_action": "inspect",
                "skill_pack_action": "none",
                "requested_tool_names": ["list_crons"],
                "confidence": 0.99,
                "valid": True,
            },
            True,
        )

    monkeypatch.setattr(turn_context_policy.runtime_cache, "get_json", stale_cached_inspect)
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "set_json",
        lambda *args, **kwargs: cache_set_calls.append((args, kwargs)),
    )
    client = _ScopeClient(
        '{"web_action":"none","scheduler_action":"mutate","scheduler_toggle_enabled":true,'
        '"scheduler_target_scope":"all_current_workspace","skill_pack_action":"none",'
        '"requested_tool_names":["list_crons","toggle_cron"],'
        '"required_tool_names":["toggle_cron"],"confidence":0.95}'
    )

    decision = build_turn_tool_scope_decision(
        model_client=client,
        user_message="structured scheduler mutation recovery",
        evidence=TurnToolEvidence(),
        registry=_registry(),
        force_model_decision=True,
        assistant_no_tool_draft="Scheduled-task inventory:\n\n1. Example - disabled",
    )

    decision_cache_sets = [
        call
        for call in cache_set_calls
        if call[0] and call[0][0] == "tool_scope.decision"
    ]
    assert cache_get_calls == []
    assert decision_cache_sets == []
    assert len(client.calls) == 1
    assert decision.scheduler_action == "mutate"
    assert decision.scheduler_toggle_enabled is True
    assert decision.scheduler_target_scope == "all_current_workspace"
    assert decision.required_tool_names == ("toggle_cron",)


def test_model_scope_cannot_self_authorize_screenshot_without_artifact_evidence(monkeypatch) -> None:
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(None, False),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)
    client = _ScopeClient(
        '{"web_action":"live_research","scheduler_action":"none","skill_pack_action":"none",'
        '"requested_tool_names":["browser_extract_text","browser_screenshot"],'
        '"requested_artifact_extensions":[".png"],"confidence":0.92}'
    )

    decision = build_turn_tool_scope_decision(
        model_client=client,
        user_message="https://github.com/sample-team/sample-app/pulls",
        evidence=TurnToolEvidence(has_url_target=True),
        registry=_registry(),
        force_model_decision=True,
    )

    assert "browser_extract_text" in decision.requested_tool_names
    assert "browser_screenshot" not in decision.requested_tool_names


def test_internal_web_scope_exposes_text_browser_tools_without_screenshot() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["web"]},
        )
    )

    assert result.status == "completed"
    assert "browser_extract_text" in result.output["available_tools"]
    assert "browser_open" in result.output["available_tools"]
    assert "browser_navigate" in result.output["available_tools"]
    assert "browser_extract_items" in result.output["available_tools"]
    assert "browser_click_element" in result.output["available_tools"]
    assert "browser_type_field" in result.output["available_tools"]
    assert "browser_assert_page_state" in result.output["available_tools"]
    assert "browser_image_collect" not in result.output["available_tools"]
    assert "file_download" in result.output["available_tools"]
    assert "browser_screenshot" not in result.output["available_tools"]
    assert "browser_run_js" in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {
        "request_tool_scope",
        "browser_open",
        "browser_navigate",
        "browser_extract_items",
        "browser_extract_text",
        "browser_click_element",
        "browser_type_field",
        "browser_run_js",
        "file_download",
    } <= names
    assert "browser_image_collect" not in names
    assert "browser_screenshot" not in names


def test_internal_web_scope_filters_stale_specs_without_handlers() -> None:
    registry = _registry()
    registry._handlers.pop("browser_snapshot", None)
    registry._specs["browser_snapshot"] = _spec("browser_snapshot")
    scoped = scoped_turn_tool_registry(
        registry,
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["web"]},
        )
    )

    assert result.status == "completed"
    assert "browser_snapshot" not in result.output["available_tools"]
    assert "browser_snapshot" not in {definition["name"] for definition in widened.list_tool_definitions()}


def test_internal_browser_capability_alias_exposes_web_browser_tools() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["browser"]},
        )
    )

    assert result.status == "completed"
    assert result.output["capabilities"] == ["web"]
    assert "browser_open" in result.output["available_tools"]
    assert "browser_navigate" in result.output["available_tools"]
    assert "browser_extract_text" in result.output["available_tools"]
    assert "No registered tools matched" not in result.output["message"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {"browser_open", "browser_navigate", "browser_extract_text"} <= names


def test_exact_file_download_scope_exposes_local_artifact_companions_without_shell() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=TurnToolEvidence(has_url_target=True, requested_extensions=(".xlsx",)),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["web"], "tool_names": ["file_download"]},
        )
    )

    assert result.status == "completed"
    assert {
        "file_download",
        "file_read",
        "workspace_summary",
        "file_search",
        "archive_extract",
    } <= set(result.output["available_tools"])
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {
        "file_download",
        "file_read",
        "workspace_summary",
        "file_search",
        "archive_extract",
    } <= names
    assert "terminal_exec" in names


def test_web_scope_with_prior_browser_context_keeps_screenshot_exact_only() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="neutral",
            conversation_result=None,
            prior_tool_scopes=("web",),
        ),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["web"]},
        )
    )

    assert result.status == "completed"
    assert "browser_screenshot" not in result.output["available_tools"]
    assert "browser_assert_page_state" in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "browser_screenshot" not in names
    assert "browser_assert_page_state" in names


def test_exact_web_scope_blocks_screenshot_without_png_artifact_contract() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["web"], "tool_names": ["browser_screenshot", "browser_assert_page_state"]},
        )
    )

    assert result.status == "completed"
    assert "browser_screenshot" not in result.output["available_tools"]
    assert "browser_assert_page_state" in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "browser_screenshot" not in names
    assert "browser_assert_page_state" in names


def test_exact_web_scope_can_request_screenshot_with_png_artifact_contract() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=TurnToolEvidence(
            has_url_target=True,
            requested_extensions=(".png",),
        ),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["web"],
                "tool_names": ["browser_screenshot", "browser_assert_page_state"],
                "artifact_extensions": [".png"],
            },
        )
    )

    assert result.status == "completed"
    assert "browser_screenshot" in result.output["available_tools"]
    assert "browser_assert_page_state" in result.output["available_tools"]
    assert result.output["artifact_extensions"] == [".png"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "browser_screenshot" in names
    assert "browser_assert_page_state" in names


def test_exact_browser_web_scope_keeps_interaction_tools_available() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["web"], "tool_names": ["browser_navigate", "browser_extract_text"]},
        )
    )

    assert result.status == "completed"
    assert "browser_navigate" in result.output["available_tools"]
    assert "browser_extract_items" in result.output["available_tools"]
    assert "browser_extract_text" in result.output["available_tools"]
    assert "browser_click_element" in result.output["available_tools"]
    assert "browser_type_field" in result.output["available_tools"]
    assert "browser_wait_for" in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {"browser_navigate", "browser_extract_items", "browser_extract_text", "browser_click_element", "browser_type_field"} <= names


def test_exact_browser_web_scope_routes_typed_detail_extractor() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["web"],
                "tool_names": ["browser_extract_detail"],
                "required_tool_names": ["browser_extract_detail"],
            },
        )
    )

    assert result.status == "completed"
    assert "browser_extract_detail" in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "browser_extract_detail" in names


def test_web_scope_exact_unavailable_search_tool_falls_back_to_registered_browser_tools() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["web"], "tool_names": ["web_search"]},
        )
    )

    assert result.status == "completed"
    assert "web_search" not in result.output["available_tools"]
    assert "browser_navigate" in result.output["available_tools"]
    assert "browser_extract_items" in result.output["available_tools"]
    assert "browser_extract_text" in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert {"request_tool_scope", "browser_navigate", "browser_extract_items", "browser_extract_text"} <= names


def test_exact_web_scope_can_request_browser_js_without_defaulting_to_it() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="neutral", conversation_result=None),
        model_client=None,
        user_message="neutral",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["web"], "tool_names": ["browser_run_js"]},
        )
    )

    assert result.status == "completed"
    assert "browser_run_js" in result.output["available_tools"]
    assert "browser_navigate" in result.output["available_tools"]
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "browser_run_js" in names


def test_open_url_scope_without_exact_tools_exposes_read_only_browser_subset() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="https://github.com/sample-team/sample-app",
            conversation_result=None,
        ),
        tool_scope_decision=TurnToolScopeDecision(
            web_action="open_url",
            confidence=1.0,
            valid=True,
        ),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}

    assert {
        "request_tool_scope",
        "browser_navigate",
        "browser_extract_items",
        "browser_extract_text",
        "browser_assert_page_state",
        "file_download",
    } <= names
    assert "web_search" not in names
    assert "browser_run_js" not in names
    assert "browser_wait_for" not in names
    assert "browser_click_element" not in names
    assert "browser_snapshot" not in names
    assert "browser_screenshot" not in names


def test_explicit_url_evidence_allows_browser_without_web_scope_decision() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="https://example.com", conversation_result=None),
        model_client=_ScopeClient('{"web_action":"none","scheduler_action":"none","skill_pack_action":"none","confidence":0.71}'),
        user_message="https://example.com",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert "browser_navigate" in names
    assert "file_download" in names
    assert "terminal_exec" in names
    assert "connector_request" not in names


def test_slash_prefixed_literal_hides_file_read_without_context() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="/Users/example/Downloads/synthetic-chat-path.txt",
            conversation_result=None,
        ),
        model_client=_ScopeClient('{"web_action":"none","scheduler_action":"none","skill_pack_action":"none","confidence":0.82}'),
        user_message="/Users/example/Downloads/synthetic-chat-path.txt",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert "file_read" not in names
    denied = scoped.invoke(
        ToolInvocation(
            "tool-path",
            "file_read",
            "web:qa",
            {"path": "/Users/example/Downloads/synthetic-chat-path.txt"},
        )
    )
    assert denied.status == "denied"
    assert denied.output["slash_prefixed_literal"] is True
    assert denied.output["suppress_activity"] is True


def test_slash_prefixed_literal_with_attachment_keeps_file_read_available() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="/Users/example/Downloads/synthetic-chat-path.txt",
            conversation_result=None,
            has_attachments=True,
        ),
        model_client=_ScopeClient('{"web_action":"none","scheduler_action":"none","skill_pack_action":"none","confidence":0.82}'),
        user_message="/Users/example/Downloads/synthetic-chat-path.txt",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert "file_read" in names


def test_invalid_scope_response_fails_closed_for_special_tools() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="ordinary request", conversation_result=None),
        model_client=_ScopeClient("not json"),
        user_message="ordinary request",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert names == {"request_tool_scope", "terminal_exec", "chat_history_search"}
    denied = scoped.invoke(ToolInvocation("tool-1", "connector_request", "telegram:1", {}))
    assert denied.status == "denied"
    assert denied.output["reason"] == "tool_requires_structured_turn_scope"


def test_connector_scope_exposes_requested_calendar_typed_tool(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_example_gateway_skill",
                "structured_tools": ["connector_request", "calendar_list"],
                "active_app_ids": ["google-calendar"],
            }
        ],
    )
    registry = _registry()
    evidence = build_turn_tool_evidence(
        user_message="structured followup",
        conversation_result=SimpleNamespace(turn=SimpleNamespace(parent_turn_id="turn-1")),
        prior_tool_scopes=("connector",),
    )
    decision = build_turn_tool_scope_decision(
        model_client=_ScopeClient(
            '{"web_action":"none","scheduler_action":"none","skill_pack_action":"connector",'
            '"connector_app_ids":["google-calendar"],"connector_source_user_requested":true,'
            '"requested_tool_names":["calendar_list"],"confidence":0.91}'
        ),
        user_message="structured followup",
        evidence=evidence,
        registry=registry,
        force_model_decision=True,
    )
    scoped = ScopedTurnToolRegistry(
        registry,
        evidence=evidence,
        tool_scope_decision=decision,
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert "calendar_list" in names
    assert "email_send" not in names


def test_plain_turn_with_active_connector_keeps_account_tools_scoped(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "structured_tools": ["connector_request", "email_search", "email_read", "calendar_list", "email_send"],
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="bonjour", conversation_result=None),
        tool_scope_decision=TurnToolScopeDecision(valid=True),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}

    assert names == {"request_tool_scope", "terminal_exec", "chat_history_search"}
    assert "email_search" not in names
    assert "email_read" not in names
    assert "calendar_list" not in names
    assert "connector_request" not in names
    assert "email_send" not in names
    assert "calendar_create" not in names
    assert "calendar_update" not in names
    assert "calendar_respond" not in names
    assert "calendar_delete" not in names


def test_request_tool_scope_connector_uses_provider_structured_tools(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_example_gateway_skill",
                "structured_tools": ["connector_request"],
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="Structured numbered-option selection:\nSelected option 3:\nConnected account",
            conversation_result=None,
            numbered_option_selected=True,
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "source_evidence": "Connected account",
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert "connector_request" in names
    assert "skill_pack_read" in names
    assert "email_search" not in names
    assert "email_read" not in names
    assert "email_send" not in names
    assert "calendar_list" not in names


def test_explicit_external_upload_exposes_matching_connector_gateway(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_example_gateway_skill",
                "display_name": "Google Drive connector",
                "structured_tools": ["connector_request"],
                "active_app_ids": ["google-drive"],
                "permission_mode": "write",
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="upload this archive to my external drive", conversation_result=None),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "source_evidence": "external drive",
                "connector_app_ids": ["google-drive"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert result.output["connector_app_ids"] == ["google-drive"]
    assert result.output["source_selection_required"] is False
    assert "connector_request" in result.output["available_tools"]
    assert "connector_request" in names


def test_custom_gateway_connection_inherits_matching_installed_skill_pack_context(tmp_path, monkeypatch) -> None:
    from nullion import connections, skill_pack_installer, turn_context_policy

    pack_dir = tmp_path / "skill-packs" / "example" / "api-gateway-skill"
    (pack_dir / "references" / "example-drive").mkdir(parents=True)
    (pack_dir / "references" / "example-drive" / "README.md").write_text(
        "# Example Drive\n\nGET /example-drive/files\n",
        encoding="utf-8",
    )
    (pack_dir / "SKILL.md").write_text(
        "---\n"
        "metadata:\n"
        "  clawdbot:\n"
        "    requires:\n"
        "      env:\n"
        "        - EXAMPLE_GATEWAY_KEY\n"
        "---\n"
        "# Example API Gateway\n\n"
        "## Base URL\n\n"
        "https://gateway.example.test/{app}/{native-api-path}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        connections,
        "load_connection_registry",
        lambda *args, **kwargs: SimpleNamespace(
            connections=[
                SimpleNamespace(
                    provider_id="custom_connector_provider",
                    display_name="Example API Gateway",
                    provider_profile="https://gateway.example.test",
                    credential_ref="EXAMPLE_GATEWAY_KEY",
                    active=True,
                    permission_mode="write",
                    credential_scope="workspace",
                )
            ]
        ),
    )
    monkeypatch.setattr(
        skill_pack_installer,
        "list_installed_skill_packs",
        lambda *args, **kwargs: [
            SimpleNamespace(pack_id="example/api-gateway-skill", path=str(pack_dir), source="fixture")
        ],
    )

    providers = turn_context_policy._active_connector_provider_context()
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="workspace storage status", conversation_result=None),
    )
    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "source_evidence": "workspace storage",
                "connector_app_ids": ["example-drive"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert providers[0]["provider_id"] == "custom_connector_provider"
    assert providers[0]["skill_pack_id"] == "example/api-gateway-skill"
    assert "references/example-drive/README.md" in providers[0]["reference_paths"]
    assert "example-drive" in providers[0]["active_app_ids"]
    assert result.output["connector_app_ids"] == ["example-drive"]
    assert result.output["available_tools"] == ["connector_request", "skill_pack_read"]
    assert "connector_request" in names
    assert "skill_pack_read" in names
    assert "terminal_exec" not in result.output["available_tools"]


def test_explicit_external_upload_without_matching_connector_does_not_use_generic_gateway_or_shell(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "display_name": "Generic connector",
                "structured_tools": ["connector_request", "email_search", "email_read", "calendar_list", "email_send"],
                "permission_mode": "write",
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="upload this archive to my external drive", conversation_result=None),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector", "local_shell"],
                "source_user_requested": True,
                "source_evidence": "external drive",
                "connector_app_ids": ["google-drive"],
                "tool_names": ["connector_request", "terminal_exec"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert result.output["connector_app_ids"] == []
    assert result.output["missing_connector_app_scope"] is True
    assert "connector_request" not in result.output["available_tools"]
    assert "terminal_exec" not in result.output["available_tools"]
    assert "connector_request" not in names
    assert "terminal_exec" in names


def test_connector_scope_without_selected_source_does_not_expose_generic_gateway(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "display_name": "Generic connector",
                "structured_tools": ["connector_request", "email_search", "email_read", "calendar_list", "email_send"],
                "permission_mode": "write",
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="upload this archive to external storage", conversation_result=None),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector", "local_files", "local_shell"],
                "source_user_requested": True,
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert "connector_request" not in result.output["available_tools"]
    assert "terminal_exec" not in result.output["available_tools"]
    assert "connector_request" not in names
    assert "terminal_exec" in names


def test_connector_account_workflow_cannot_escalate_to_shell_after_failed_connector_scope(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "display_name": "Generic connector",
                "structured_tools": ["connector_request"],
                "permission_mode": "write",
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="upload this archive to my external drive", conversation_result=None),
    )
    _, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "source_evidence": "external drive",
                "connector_app_ids": ["google-drive"],
            },
        )
    )

    result, widened_again = widened.apply_scope_request(
        ToolInvocation(
            "scope-2",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["web", "local_shell"],
                "tool_names": ["terminal_exec"],
            },
        )
    )

    names = {definition["name"] for definition in widened_again.list_tool_definitions()}
    assert result.status == "completed"
    assert "terminal_exec" not in result.output["available_tools"]
    assert "terminal_exec" in names


def test_request_tool_scope_connector_without_exact_tools_requires_source_selection(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_example_gateway_skill",
                "display_name": "Example connector",
                "structured_tools": ["connector_request", "email_send", "email_search", "email_read", "calendar_list"],
                "active_app_ids": ["google-mail", "google-calendar"],
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="verificar estado externo", conversation_result=None),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["connector"], "source_user_requested": True},
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert "connector_request" not in names
    assert "skill_pack_read" not in names
    assert "email_search" not in names
    assert "email_read" not in names
    assert "calendar_list" not in names
    assert "email_send" not in names
    assert result.output["connector_app_ids"] == []
    assert result.output["source_selection_required"] is True
    assert result.output["available_sources"] == [
        {
            "display_name": "Example connector",
            "active_app_ids": ["google-mail", "google-calendar"],
            "read_tools": ["calendar_list", "email_read", "email_search"],
            "permission_mode": "read",
        }
    ]


def test_request_tool_scope_rejects_unverified_current_turn_connector_claim(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "display_name": "Mail connector",
                "structured_tools": ["connector_request", "email_search", "email_read"],
                "active_app_ids": ["google-mail"],
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="I meant for rolling tobacco",
            conversation_result=None,
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-unrelated-email",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "connector_app_ids": ["google-mail"],
                "tool_names": ["email_search", "email_read"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert result.output["connector_source_evidence_required"] is True
    assert result.output["source_evidence_verified"] is False
    assert result.output["missing_connector_app_scope"] is False
    assert "email_search" not in result.output["available_tools"]
    assert "email_read" not in result.output["available_tools"]
    assert "email_search" not in names
    assert "email_read" not in names


def test_tool_scope_classifier_rejects_connector_without_current_turn_source_evidence(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(hit=False, value=None),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "structured_tools": ["email_search", "email_read"],
                "active_app_ids": ["google-mail"],
            }
        ],
    )
    user_message = "I meant for rolling tobacco"

    decision = build_turn_tool_scope_decision(
        model_client=_ScopeClient(
            '{"web_action":"none","scheduler_action":"none","skill_pack_action":"connector",'
            '"connector_app_ids":["google-mail"],"connector_source_user_requested":true,'
            '"connector_source_evidence":"email",'
            '"requested_tool_names":["email_search","email_read"],"confidence":0.95}'
        ),
        user_message=user_message,
        evidence=build_turn_tool_evidence(user_message=user_message, conversation_result=None),
        registry=_registry(),
        force_model_decision=True,
    )

    assert decision.skill_pack_action == "none"
    assert decision.connector_source_user_requested is False
    assert decision.connector_source_evidence == ""
    assert "email_search" not in decision.requested_tool_names
    assert "email_read" not in decision.requested_tool_names


def test_request_tool_scope_accepts_language_neutral_exact_source_evidence(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "display_name": "Mail connector",
                "structured_tools": ["connector_request", "email_search", "email_read"],
                "active_app_ids": ["google-mail"],
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="Revisa mi bandeja de entrada reciente",
            conversation_result=None,
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-explicit-email",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "source_evidence": "mi bandeja de entrada",
                "connector_app_ids": ["google-mail"],
                "tool_names": ["email_search", "email_read"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert result.output["connector_source_evidence_required"] is False
    assert result.output["source_evidence_verified"] is True
    assert result.output["available_tools"] == ["email_search", "email_read"]
    assert {"email_search", "email_read"} <= names


def test_generic_connector_request_tool_still_requires_source_selection(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_example_gateway_skill",
                "display_name": "Example connector",
                "structured_tools": ["connector_request", "email_send", "email_search", "email_read", "calendar_list"],
                "active_app_ids": ["google-mail", "google-calendar"],
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="verificar estado externo", conversation_result=None),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "tool_names": ["connector_request"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert result.output["connector_app_ids"] == []
    assert result.output["source_selection_required"] is True
    assert "connector_request" not in result.output["available_tools"]
    assert "skill_pack_read" not in result.output["available_tools"]
    assert "email_search" not in result.output["available_tools"]
    assert "email_read" not in result.output["available_tools"]
    assert "calendar_list" not in result.output["available_tools"]
    assert "connector_request" not in names
    assert "skill_pack_read" not in names
    assert "email_search" not in names
    assert "email_read" not in names
    assert "calendar_list" not in names


def test_exact_connector_read_tool_scope_does_not_require_source_selection(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_example_gateway_skill",
                "display_name": "Example connector",
                "structured_tools": ["connector_request", "email_send", "email_search", "email_read", "calendar_list"],
                "active_app_ids": ["google-mail", "google-calendar"],
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="structured account read", conversation_result=None),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "source_evidence": "structured account read",
                "tool_names": ["email_search"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert result.output["source_selection_required"] is False
    assert "email_search" in result.output["available_tools"]
    assert "email_search" in names


def test_numbered_source_selection_runs_tool_scope_classifier(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(hit=False, value=None),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_example_gateway_skill",
                "display_name": "Example connector",
                "structured_tools": ["connector_request", "email_search", "email_read", "calendar_list"],
                "active_app_ids": ["google-mail", "google-calendar"],
            }
        ],
    )
    evidence = build_turn_tool_evidence(
        user_message=(
            "Structured numbered-option selection: the user replied with option 1 from the immediately "
            "previous assistant response.\nSelected option 1:\nEmail records\nFixture: numeric-source-314159"
        ),
        conversation_result=None,
        numbered_option_selected=True,
    )
    client = _ScopeClient(
        '{"web_action":"none","scheduler_action":"none","skill_pack_action":"connector",'
        '"connector_source_user_requested":true,"connector_app_ids":["google-mail"],'
        '"connector_source_evidence":"Email records",'
        '"requested_tool_names":["email_search"],"confidence":0.94}'
    )

    decision = build_turn_tool_scope_decision(
        model_client=client,
        user_message="Structured numbered-option selection:\nSelected option 1:\nEmail records\nFixture: numeric-source-314159",
        evidence=evidence,
        registry=_registry(),
        force_model_decision=True,
    )

    assert turn_tool_evidence_needs_model_scope_decision(evidence) is True
    assert client.calls
    prompt = client.calls[0]["messages"][0]["content"][0]["text"]
    assert '"numbered_option_selected": true' in prompt
    assert decision.skill_pack_action == "connector"
    assert decision.connector_source_user_requested is True
    assert decision.requested_tool_names == ("email_search",)


def test_request_tool_scope_connector_exact_account_tool_infers_matching_active_app_scope(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_example_gateway_skill",
                "structured_tools": ["connector_request", "email_search", "email_read", "calendar_list"],
                "active_app_ids": ["google-mail", "google-calendar"],
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="Structured numbered-option selection:\nSelected option 1:\nEmail records",
            conversation_result=None,
            numbered_option_selected=True,
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "source_evidence": "Email records",
                "tool_names": ["email_search", "email_read"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert result.output["available_tools"] == ["email_search", "email_read"]
    assert result.output["connector_app_ids"] == ["google-mail"]
    assert result.output["missing_connector_app_scope"] is False
    assert "connector_request" not in names
    assert "email_search" in names
    assert "email_read" in names
    assert "calendar_list" not in names


def test_request_tool_scope_connector_exact_account_tool_uses_structured_tools_without_app_ids(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "structured_tools": ["connector_request", "email_search", "email_read", "calendar_list"],
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="Structured numbered-option selection:\nSelected option 1:\nEmail records",
            conversation_result=None,
            numbered_option_selected=True,
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "source_evidence": "Email records",
                "tool_names": ["email_read"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert result.output["available_tools"] == ["email_read", "email_search"]
    assert result.output["connector_app_ids"] == []
    assert result.output["missing_connector_app_scope"] is False
    assert "email_read" in names
    assert "email_search" in names
    assert "calendar_list" not in names
    assert "connector_request" not in names


def test_numbered_source_selection_pairs_email_read_with_search(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "structured_tools": ["connector_request", "email_search", "email_read"],
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="Structured numbered-option selection:\nSelected option 1:\nEmail records",
            conversation_result=None,
            numbered_option_selected=True,
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "source_evidence": "Email records",
                "tool_names": ["email_read"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert result.output["available_tools"] == ["email_read", "email_search"]
    assert "email_search" in names
    assert "email_read" in names


def test_connector_email_search_scope_exposes_read_continuation(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "structured_tools": ["connector_request", "email_search", "email_read", "email_send"],
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="Structured numbered-option selection:\nSelected option 1:\nAccount records",
            conversation_result=None,
            numbered_option_selected=True,
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "source_evidence": "Account records",
                "tool_names": ["email_search"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert result.output["available_tools"] == ["email_search", "email_read"]
    assert "email_search" in names
    assert "email_read" in names
    assert "email_send" not in names


def test_request_tool_scope_connector_provider_id_does_not_block_exact_account_tool_without_app_ids(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "structured_tools": ["connector_request", "email_search", "email_read", "calendar_list"],
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="Structured numbered-option selection:\nSelected option 1:\nEmail records",
            conversation_result=None,
            numbered_option_selected=True,
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "source_evidence": "Email records",
                "connector_app_ids": ["custom_connector_provider"],
                "tool_names": ["email_search", "email_read"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert result.output["available_tools"] == ["email_search", "email_read"]
    assert result.output["connector_app_ids"] == []
    assert result.output["missing_connector_app_scope"] is False
    assert "email_search" in names
    assert "email_read" in names
    assert widened.invoke(ToolInvocation("email-1", "email_search", "telegram:1", {"query": "is:unread"})).status == "completed"


def test_request_tool_scope_connector_write_tool_is_required(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "structured_tools": ["connector_request", "email_send", "email_search", "email_read"],
            }
        ],
    )
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="structured account action", conversation_result=None),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "source_evidence": "structured account action",
                "tool_names": ["email_send", "email_search"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert "email_send" in result.output["available_tools"]
    assert "email_search" in result.output["available_tools"]
    assert result.output["required_tool_names"] == ["email_send"]
    assert "email_send" in names
    assert "email_search" in names


def test_request_tool_scope_can_explicitly_require_current_source_tool() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="structured current-source artifact action",
            conversation_result=None,
            requested_extensions=(".html",),
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-source",
            "request_tool_scope",
            "web:1",
            {
                "capabilities": ["web", "local_files"],
                "tool_names": ["browser_navigate", "file_write"],
                "required_tool_names": ["browser_navigate"],
                "artifact_extensions": [".html"],
            },
        )
    )

    assert result.status == "completed"
    assert set(result.output["required_tool_names"]) == {"browser_navigate", "file_write"}
    assert widened.can_invoke_tool("browser_navigate")
    assert widened.can_invoke_tool("file_write")


def test_request_tool_scope_web_capability_requires_callable_web_entry_tool() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="structured multi-source artifact action",
            conversation_result=None,
            requested_extensions=(".html",),
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-web-required",
            "request_tool_scope",
            "web:1",
            {
                "capabilities": ["web", "weather", "market_data", "local_files"],
                "tool_names": [
                    "browser_navigate",
                    "browser_extract_text",
                    "weather_forecast",
                    "market_quote",
                    "file_write",
                ],
                "required_tool_names": ["weather_forecast", "market_quote"],
                "artifact_extensions": [".html"],
            },
        )
    )

    assert result.status == "completed"
    assert set(result.output["required_tool_names"]) == {
        "browser_navigate",
        "weather_forecast",
        "market_quote",
        "file_write",
    }
    assert "browser_navigate" in widened.turn_tool_scope_decision.required_tool_names


def test_request_tool_scope_result_preserves_classifier_required_source_tools() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="structured classifier-backed source artifact action",
            conversation_result=None,
            requested_extensions=(".html",),
        ),
        tool_scope_decision=TurnToolScopeDecision(
            web_action="live_research",
            requested_tool_names=("browser_navigate", "file_write"),
            required_tool_names=("browser_navigate", "file_write"),
            requested_artifact_extensions=(".html",),
            confidence=0.99,
            valid=True,
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-merged-required",
            "request_tool_scope",
            "web:1",
            {
                "capabilities": ["weather", "market_data", "web", "local_files"],
                "tool_names": ["weather_forecast", "market_quote"],
                "required_tool_names": ["weather_forecast", "market_quote"],
                "artifact_extensions": [".html"],
            },
        )
    )

    assert result.status == "completed"
    assert set(result.output["requested_required_tool_names"]) == {
        "browser_navigate",
        "file_write",
        "weather_forecast",
        "market_quote",
    }
    assert set(result.output["required_tool_names"]) == {
        "browser_navigate",
        "file_write",
        "weather_forecast",
        "market_quote",
    }
    assert widened.turn_tool_scope_decision.required_tool_names == (
        "browser_navigate",
        "file_write",
        "weather_forecast",
        "market_quote",
    )


def test_request_tool_scope_reports_required_source_that_is_not_registered(monkeypatch) -> None:
    monkeypatch.setattr(turn_context_policy, "_active_connector_provider_context", lambda: [])
    registry = ToolRegistry()
    registry.register(
        _spec("file_write"),
        lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
    )
    scoped = scoped_turn_tool_registry(
        registry,
        evidence=build_turn_tool_evidence(
            user_message="structured unavailable-source artifact action",
            conversation_result=None,
            requested_extensions=(".html",),
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-unavailable-source",
            "request_tool_scope",
            "web:1",
            {
                "capabilities": ["connector", "local_files"],
                "source_user_requested": True,
                "tool_names": ["calendar_list", "file_write"],
                "required_tool_names": ["calendar_list"],
                "artifact_extensions": [".html"],
            },
        )
    )

    assert result.status == "completed"
    assert result.output["required_tool_names"] == ["file_write"]
    assert result.output["requested_required_tool_names"] == ["calendar_list", "file_write"]
    assert result.output["unavailable_required_tool_names"] == ["calendar_list"]
    assert result.output["active_connector_providers"] == []
    assert result.output["available_sources"] == []
    assert result.output["connector_source_unavailable"] is True
    assert result.output["unavailable_capabilities"] == ["connector"]
    assert "calendar_list" in result.output["unavailable_tools"]
    assert not widened.can_invoke_tool("calendar_list")


def test_email_address_target_exposes_email_send_scope(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "active_app_ids": ["google-mail"],
                "structured_tools": ["connector_request", "email_send", "email_search", "email_read"],
            }
        ],
    )
    user_message = "Send recipient@example.com the two artifacts from this conversation."
    evidence = build_turn_tool_evidence(
        user_message=user_message,
        conversation_result=None,
        requested_extensions=(".pdf", ".xlsx"),
        prior_tool_scopes=("local_files",),
    )

    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=evidence,
        model_client=None,
        user_message=user_message,
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert evidence.has_email_address_target is True
    assert scoped.turn_tool_scope_decision.connector_app_ids == ("google-mail",)
    assert scoped.turn_tool_scope_decision.required_tool_names == ("email_send",)
    assert "email_send" in names


def test_email_address_target_with_sentence_punctuation_exposes_email_send_scope(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "active_app_ids": ["google-mail"],
                "structured_tools": ["connector_request", "email_send", "email_search", "email_read"],
            }
        ],
    )
    user_message = (
        "Create a stage QA package and send it to recipient@example.com. "
        "Attach a generated CSV, XLSX workbook, PDF summary, and one HTML file."
    )
    evidence = build_turn_tool_evidence(
        user_message=user_message,
        conversation_result=None,
        requested_extensions=(".csv", ".xlsx", ".pdf", ".html"),
    )

    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=evidence,
        model_client=None,
        user_message=user_message,
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert evidence.has_email_address_target is True
    assert scoped.turn_tool_scope_decision.connector_app_ids == ("google-mail",)
    assert scoped.turn_tool_scope_decision.required_tool_names == ("email_send",)
    assert "email_send" in names


def test_structured_email_target_requested_send_becomes_required(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "active_app_ids": ["google-mail"],
                "structured_tools": ["connector_request", "email_send", "email_search", "email_read"],
            }
        ],
    )
    user_message = "recipient@example.com"
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message=user_message,
            conversation_result=None,
            requested_extensions=(".csv", ".xlsx", ".pdf", ".html"),
        ),
        model_client=_ScopeClient(
            '{"web_action":"none","scheduler_action":"none","skill_pack_action":"connector",'
            '"connector_app_ids":["google-mail"],"connector_source_user_requested":true,'
            f'"connector_source_evidence":"{user_message}",'
            '"requested_tool_names":["email_send","file_write","spreadsheet_create","pdf_create"],'
            '"requested_artifact_extensions":[".csv",".xlsx",".pdf",".html"],'
            '"required_tool_names":[],"confidence":0.91}'
        ),
        user_message=user_message,
    )

    decision = scoped.turn_tool_scope_decision

    assert "email_send" in {definition["name"] for definition in scoped.list_tool_definitions()}
    assert "email_send" in decision.required_tool_names
    assert {".csv", ".xlsx", ".pdf", ".html"} <= set(decision.requested_artifact_extensions)


def test_structured_email_read_target_does_not_require_send(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(hit=False, value=None),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "active_app_ids": ["google-mail"],
                "structured_tools": ["connector_request", "email_send", "email_search", "email_read"],
            }
        ],
    )
    user_message = "rosemarie@example.com"
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message=user_message, conversation_result=None),
        model_client=_ScopeClient(
            '{"web_action":"none","scheduler_action":"none","skill_pack_action":"connector",'
            '"connector_app_ids":["google-mail"],"connector_source_user_requested":true,'
            f'"connector_source_evidence":"{user_message}",'
            '"requested_tool_names":["email_search","email_read"],'
            '"required_tool_names":["email_search"],"confidence":0.91}'
        ),
        user_message=user_message,
    )

    decision = scoped.turn_tool_scope_decision

    assert "email_send" not in decision.required_tool_names


def test_scope_classifier_prompt_guides_direct_mailbox_reads(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(hit=False, value=None),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "active_app_ids": ["google-mail", "google-calendar"],
                "structured_tools": ["connector_request", "email_search", "email_read", "calendar_list"],
            }
        ],
    )
    client = _ScopeClient(
        '{"web_action":"none","scheduler_action":"none","skill_pack_action":"connector",'
        '"connector_app_ids":["google-mail"],"connector_source_user_requested":true,'
        '"connector_source_evidence":"my email",'
        '"requested_tool_names":["email_search","email_read"],"confidence":0.94}'
    )

    decision = build_turn_tool_scope_decision(
        model_client=client,
        user_message="Check my email",
        evidence=build_turn_tool_evidence(user_message="Check my email", conversation_result=None),
        registry=_registry(),
        force_model_decision=True,
    )

    request_text = repr(client.calls[0])
    assert "google-mail/email_search/email_read" in request_text
    assert decision.skill_pack_action == "connector"
    assert decision.connector_app_ids == ("google-mail",)
    assert decision.connector_source_user_requested is True
    assert decision.requested_tool_names == ("email_search", "email_read")


def test_request_tool_scope_exposes_calendar_write_tools_without_making_them_required(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "structured_tools": [
                    "connector_request",
                    "calendar_list",
                    "calendar_create",
                    "calendar_update",
                    "calendar_respond",
                    "calendar_delete",
                ],
                "permission_mode": "write",
            }
        ],
    )
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="structured calendar action", conversation_result=None),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "source_evidence": "structured calendar action",
                "tool_names": ["calendar_create", "calendar_update", "calendar_respond", "calendar_delete"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert {
        "calendar_create",
        "calendar_update",
        "calendar_respond",
        "calendar_delete",
    }.issubset(set(result.output["available_tools"]))
    assert result.output["required_tool_names"] == []
    assert {
        "calendar_create",
        "calendar_update",
        "calendar_respond",
        "calendar_delete",
    }.issubset(names)
    assert "email_send" not in names


def test_request_tool_scope_calendar_write_requires_verified_source_flag(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "custom_connector_provider",
                "display_name": "Maton API Gateway",
                "structured_tools": [
                    "connector_request",
                    "email_search",
                    "email_read",
                    "calendar_list",
                    "email_send",
                    "calendar_create",
                    "calendar_update",
                    "calendar_respond",
                    "calendar_delete",
                ],
                "permission_mode": "write",
            }
        ],
    )
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="structured calendar action", conversation_result=None),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "connector_app_ids": ["google-calendar"],
                "tool_names": ["calendar_create"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert result.output["available_tools"] == []
    assert result.output["connector_app_ids"] == []
    assert result.output["missing_connector_app_scope"] is False
    assert result.output["connector_source_evidence_required"] is True
    assert "calendar_create" not in names
    assert "email_send" not in names
    assert "connector_request" not in names


def test_request_tool_scope_connector_filters_typed_tools_by_selected_app(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_example_gateway_skill",
                "structured_tools": ["connector_request", "email_send", "email_search", "email_read", "calendar_list"],
                "active_app_ids": ["google-mail", "google-calendar"],
            }
        ],
    )
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="Structured numbered-option selection:\nSelected option 2:\nCalendar records",
            conversation_result=None,
            numbered_option_selected=True,
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope-1",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["connector"],
                "source_user_requested": True,
                "source_evidence": "Calendar records",
                "connector_app_ids": ["google-calendar"],
                "tool_names": ["calendar_list"],
            },
        )
    )

    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert result.status == "completed"
    assert "connector_request" not in names
    assert "skill_pack_read" in names
    assert "calendar_list" in names
    assert "email_search" not in names
    assert "email_read" not in names
    assert "email_send" not in names


def test_mini_agent_scope_materialization_exposes_calendar_before_deepagent(monkeypatch) -> None:
    from nullion import turn_context_policy

    monkeypatch.setattr(
        turn_context_policy,
        "_active_connector_provider_context",
        lambda: [
            {
                "provider_id": "skill_pack_connector_example_gateway_skill",
                "structured_tools": ["connector_request", "calendar_list"],
                "active_app_ids": ["google-calendar"],
            }
        ],
    )
    registry = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="revisa mi agenda conectada", conversation_result=None),
        model_client=None,
        user_message="revisa mi agenda conectada",
    )

    materialized = materialize_mini_agent_tool_scope_registry(
        registry,
        model_client=_ScopeClient(
            '{"web_action":"none","scheduler_action":"none","skill_pack_action":"connector",'
            '"connector_app_ids":["google-calendar"],"connector_source_user_requested":true,'
            '"connector_source_evidence":"mi agenda conectada",'
            '"requested_tool_names":["calendar_list"],"confidence":0.91}'
        ),
        user_message="revisa mi agenda conectada",
    )

    names = {definition["name"] for definition in materialized.list_tool_definitions()}
    assert "calendar_list" in names
    assert "request_tool_scope" in names


def test_mini_agent_scope_materialization_applies_artifact_extension_decision() -> None:
    registry = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(user_message="planner task", conversation_result=None),
        model_client=None,
        user_message="planner task",
    )

    materialized = materialize_mini_agent_tool_scope_registry(
        registry,
        model_client=_ScopeClient(
            '{"web_action":"none","scheduler_action":"none","skill_pack_action":"none",'
            '"requested_artifact_extensions":[".docx",".pptx",".xlsx"],'
            '"required_embedded_media_extensions":[".docx",".pptx",".xlsx"],'
            '"requested_tool_names":[],"confidence":0.91}'
        ),
        user_message="planner task",
    )

    names = {definition["name"] for definition in materialized.list_tool_definitions()}
    assert {"document_create", "presentation_create", "spreadsheet_create"} <= names
    assert "file_write" not in names
    decision = materialized.turn_tool_scope_decision
    assert decision.requested_artifact_extensions == (".docx", ".pptx", ".xlsx")
    assert decision.required_embedded_media_extensions == (".docx", ".pptx", ".xlsx")


def test_mini_agent_scope_materialization_does_not_stop_at_scope_request_only() -> None:
    bootstrap_registry = ScopedTurnToolRegistry(
        _registry(),
        evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(
            requested_tool_names=("request_tool_scope",),
            confidence=1.0,
            valid=True,
        ),
    )
    client = _ScopeClient(
        '{"web_action":"browser_interaction","scheduler_action":"none","skill_pack_action":"none",'
        '"requested_tool_names":["archive_extract","browser_image_collect"],'
        '"requested_artifact_extensions":[".xlsx"],'
        '"required_embedded_media_extensions":[".xlsx"],'
        '"confidence":0.91}'
    )

    materialized = materialize_mini_agent_tool_scope_registry(
        bootstrap_registry,
        model_client=client,
        user_message="planner artifact workflow",
        evidence=TurnToolEvidence(
            has_url_target=True,
            has_attachments=True,
            requested_extensions=(".xlsx",),
        ),
    )

    names = {definition["name"] for definition in materialized.list_tool_definitions()}
    assert {"request_tool_scope", "archive_extract", "browser_image_collect", "spreadsheet_create"} <= names


def test_mini_agent_scope_materialization_keeps_structured_artifact_evidence_on_empty_decision() -> None:
    registry = _registry()

    materialized = materialize_mini_agent_tool_scope_registry(
        registry,
        model_client=None,
        user_message="planner artifact workflow",
        evidence=TurnToolEvidence(requested_extensions=(".xlsx",)),
    )

    names = {definition["name"] for definition in materialized.list_tool_definitions()}
    assert {"request_tool_scope", "spreadsheet_create"} <= names
    assert "file_write" not in names


def test_mini_agent_scope_materialization_uses_planner_fallback_before_structured_empty_decision() -> None:
    materialized = materialize_mini_agent_tool_scope_registry(
        _registry(),
        model_client=None,
        user_message="structured planner artifact workflow",
        evidence=TurnToolEvidence(requested_extensions=(".xlsx",)),
        planner_requested=True,
    )

    names = {definition["name"] for definition in materialized.list_tool_definitions()}
    assert {"archive_extract", "spreadsheet_create"} <= names
    assert "workspace_summary" not in names
    assert materialized.turn_tool_scope_decision.requested_artifact_extensions == (".xlsx",)
    assert "connector_request" not in names
    assert "email_search" not in names


def test_mini_agent_scope_materialization_uses_structured_planner_fallback_without_account_tools() -> None:
    materialized = materialize_mini_agent_tool_scope_registry(
        _registry(),
        model_client=None,
        user_message="structured planner mission",
        evidence=TurnToolEvidence(),
        planner_requested=True,
    )

    names = {definition["name"] for definition in materialized.list_tool_definitions()}
    assert "archive_extract" not in names  # Planner visibility does not authorize file extraction.
    assert "request_tool_scope" in names
    assert "spreadsheet_create" not in names
    assert "browser_navigate" not in names
    assert materialized.turn_tool_scope_decision.requested_artifact_extensions == ()
    assert "connector_request" not in names
    assert "email_search" not in names
    assert "list_crons" not in names


def test_planner_scope_materialization_merges_existing_artifact_decision_with_archive_tools() -> None:
    base = ScopedTurnToolRegistry(
        _registry(),
        evidence=TurnToolEvidence(has_attachments=True,requested_extensions=(".xlsx",)),
        tool_scope_decision=TurnToolScopeDecision(
            requested_artifact_extensions=(".xlsx",),
            required_embedded_media_extensions=(".xlsx",),
            requested_tool_names=("spreadsheet_create",),
            confidence=0.9,
            valid=True,
        ),
    )

    materialized = materialize_mini_agent_tool_scope_registry(
        base,
        model_client=None,
        user_message="planeacion de artefacto",
        planner_requested=True,
    )

    names = {definition["name"] for definition in materialized.list_tool_definitions()}
    assert {"archive_extract", "file_read", "spreadsheet_create", "browser_image_collect"} <= names
    assert "email_search" not in names
    decision = materialized.turn_tool_scope_decision
    assert ".xlsx" in decision.requested_artifact_extensions
    assert decision.required_embedded_media_extensions == (".xlsx",)


def test_artifact_evidence_needs_scope_decision_without_plain_chat_cost() -> None:
    plain = build_turn_tool_evidence(user_message="hi", conversation_result=None)
    artifact = build_turn_tool_evidence(
        user_message="structured artifact turn",
        conversation_result=None,
        requested_extensions=[".docx", ".xlsx"],
    )

    assert turn_tool_evidence_needs_model_scope_decision(plain) is False
    assert turn_tool_scope_decision_may_apply(plain) is False
    assert turn_tool_evidence_needs_model_scope_decision(artifact) is True
    assert turn_tool_scope_decision_may_apply(artifact) is True


def test_artifact_evidence_fallback_exposes_only_matching_artifact_tools() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="structured artifact turn",
            conversation_result=None,
            requested_extensions=[".docx", ".pptx", ".xlsx", ".csv", ".html"],
        ),
        model_client=None,
        user_message="structured artifact turn",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert {"document_create", "presentation_create", "spreadsheet_create", "file_write"} <= names
    assert "request_tool_scope" in names
    assert "file_search" not in names
    assert "file_read" not in names
    assert "terminal_exec" in names


def test_structured_artifact_scope_prefers_dedicated_tool_over_required_shell() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="structured artifact turn",
            conversation_result=None,
            requested_extensions=[".xlsx"],
        ),
        model_client=None,
        user_message="structured artifact turn",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["local_files", "local_shell"],
                "artifact_extensions": [".xlsx"],
                "tool_names": ["terminal_exec"],
            },
        )
    )

    assert result.status == "completed"
    assert "spreadsheet_create" in result.output["available_tools"]
    assert "terminal_exec" not in result.output["available_tools"]
    assert result.output["required_tool_names"] == []
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "spreadsheet_create" in names
    assert "terminal_exec" in names


def test_structured_artifact_scope_inherits_typed_extension_when_shell_request_omits_it() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="structured artifact turn",
            conversation_result=None,
            requested_extensions=[".xlsx"],
        ),
        model_client=None,
        user_message="structured artifact turn",
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "tool-scope",
            "request_tool_scope",
            "telegram:1",
            {
                "capabilities": ["local_shell"],
                "tool_names": ["terminal_exec"],
            },
        )
    )

    assert result.status == "completed"
    assert result.output["artifact_extensions"] == [".xlsx"]
    assert "spreadsheet_create" in result.output["available_tools"]
    assert "terminal_exec" not in result.output["available_tools"]
    assert result.output["required_tool_names"] == []
    names = {definition["name"] for definition in widened.list_tool_definitions()}
    assert "spreadsheet_create" in names
    assert "terminal_exec" in names


def test_multi_format_artifact_scope_uses_structured_classifier_to_exclude_non_deliverables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(None, False),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)
    client = _ScopeClient(
        '{"web_action":"none","scheduler_action":"none","skill_pack_action":"none",'
        '"requested_tool_names":["document_create","spreadsheet_create","pdf_create","file_write"],'
        '"requested_artifact_extensions":[".docx",".xlsx",".csv",".pdf",".html"],"confidence":0.91}'
    )

    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="structured artifact turn",
            conversation_result=None,
            requested_extensions=[".docx", ".xlsx", ".csv", ".pdf", ".html", ".json", ".txt"],
        ),
        model_client=client,
        user_message="structured artifact turn",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}
    assert len(client.calls) == 1
    assert scoped.turn_tool_scope_decision.requested_artifact_extensions == (
        ".docx",
        ".xlsx",
        ".csv",
        ".pdf",
        ".html",
    )
    assert scoped.turn_tool_scope_decision.excluded_artifact_extensions == (".json", ".txt")
    assert {"document_create", "spreadsheet_create", "pdf_create", "file_write"} <= names
    assert "request_tool_scope" in names


def test_single_format_artifact_scope_avoids_pre_model_classifier_latency() -> None:
    client = _ScopeClient(
        '{"web_action":"live_research","scheduler_action":"none","skill_pack_action":"none","confidence":0.91}'
    )

    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="structured workbook turn",
            conversation_result=None,
            requested_extensions=[".xlsx"],
        ),
        model_client=client,
        user_message="structured workbook turn",
    )

    assert client.calls == []
    assert "spreadsheet_create" in {definition["name"] for definition in scoped.list_tool_definitions()}


def test_scope_request_inherits_valid_typed_final_formats_over_raw_extension_evidence() -> None:
    scoped = ScopedTurnToolRegistry(
        _registry(),
        evidence=TurnToolEvidence(
            requested_extensions=(".docx", ".xlsx", ".csv", ".pdf", ".html", ".json", ".txt"),
        ),
        tool_scope_decision=TurnToolScopeDecision(
            requested_tool_names=("document_create", "spreadsheet_create", "pdf_create", "file_write"),
            requested_artifact_extensions=(".docx", ".xlsx", ".csv", ".pdf", ".html"),
            excluded_artifact_extensions=(".json", ".txt"),
            confidence=1.0,
            valid=True,
        ),
    )

    result, widened = scoped.apply_scope_request(
        ToolInvocation(
            "scope",
            "request_tool_scope",
            "telegram:1",
            {"capabilities": ["local_files"]},
        )
    )

    assert result.output["artifact_extensions"] == [".docx", ".xlsx", ".csv", ".pdf", ".html"]
    assert widened.turn_tool_scope_decision.requested_artifact_extensions == (
        ".docx",
        ".xlsx",
        ".csv",
        ".pdf",
        ".html",
    )
    assert widened.turn_tool_scope_decision.excluded_artifact_extensions == (".json", ".txt")


def test_multi_format_scope_refines_model_that_marks_every_mentioned_format_final(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(None, False),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)

    class Client:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def create(self, **kwargs: object) -> object:
            self.calls.append(kwargs)
            text = (
                '{"final_artifact_extensions":[".docx",".xlsx",".csv",".pdf",".html"],'
                '"excluded_artifact_extensions":[".json",".txt"]}'
                if len(self.calls) == 2
                else '{"web_action":"none","scheduler_action":"none","skill_pack_action":"none",'
                '"requested_tool_names":["document_create","spreadsheet_create","pdf_create","file_write"],'
                '"requested_artifact_extensions":[".docx",".xlsx",".csv",".pdf",".html",".json",".txt"],'
                '"confidence":0.91}'
            )
            return {"stop_reason": "end_turn", "content": [{"type": "text", "text": text}]}

    client = Client()
    evidence = TurnToolEvidence(
        requested_extensions=(".docx", ".xlsx", ".csv", ".pdf", ".html", ".json", ".txt"),
    )

    decision = build_turn_tool_scope_decision(
        model_client=client,
        user_message="structured mixed artifact request",
        evidence=evidence,
        registry=_registry(),
        force_model_decision=True,
    )

    assert len(client.calls) == 2
    assert decision.requested_artifact_extensions == (".docx", ".xlsx", ".csv", ".pdf", ".html")
    assert decision.excluded_artifact_extensions == (".json", ".txt")
    assert "Partition every candidate extension" in str(client.calls[1]["system"])


def test_scope_decision_preserves_required_live_source_tools_for_artifact_turn() -> None:
    client = _ScopeClient(
        '{"web_action":"live_research","scheduler_action":"none","skill_pack_action":"none",'
        '"requested_tool_names":["browser_navigate","market_quote","weather_forecast","file_write"],'
        '"required_tool_names":["browser_navigate","market_quote","weather_forecast","file_write"],'
        '"requested_artifact_extensions":[".html"],"confidence":0.97}'
    )

    decision = build_turn_tool_scope_decision(
        model_client=client,
        user_message="structured cross-domain current-source artifact request",
        evidence=TurnToolEvidence(requested_extensions=(".html",)),
        registry=_registry(),
        force_model_decision=True,
        cache_decision=False,
    )

    assert decision.web_action == "live_research"
    assert set(decision.required_tool_names) == {
        "browser_navigate",
        "market_quote",
        "weather_forecast",
        "file_write",
    }
    assert "successful structured result is essential" in str(client.calls[0]["system"])


def test_multi_format_scope_uses_compact_partition_when_full_classifier_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(None, False),
    )
    stored: list[object] = []
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "set_json",
        lambda *args, **kwargs: stored.append((args, kwargs)),
    )

    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def create(self, **kwargs: object) -> object:
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("full classifier timed out")
            return {
                "stop_reason": "end_turn",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            '{"final_artifact_extensions":[".docx",".xlsx",".csv",".pdf",".html"],'
                            '"excluded_artifact_extensions":[".json",".txt"]}'
                        ),
                    }
                ],
            }

    client = Client()
    evidence = TurnToolEvidence(
        requested_extensions=(".docx", ".xlsx", ".csv", ".pdf", ".html", ".json", ".txt"),
    )

    decision = build_turn_tool_scope_decision(
        model_client=client,
        user_message="structured mixed artifact request",
        evidence=evidence,
        registry=_registry(),
        force_model_decision=True,
    )

    assert client.calls == 2
    assert decision.requested_artifact_extensions == (".docx", ".xlsx", ".csv", ".pdf", ".html")
    assert decision.excluded_artifact_extensions == (".json", ".txt")
    assert stored


def test_artifact_scope_decision_cannot_reenable_stale_file_search_without_context() -> None:
    scoped = scoped_turn_tool_registry(
        _registry(),
        evidence=build_turn_tool_evidence(
            user_message="structured artifact turn",
            conversation_result=None,
            requested_extensions=[".docx", ".pptx", ".xlsx", ".csv", ".html"],
        ),
        model_client=_ScopeClient(
            '{"web_action":"none","scheduler_action":"none","skill_pack_action":"none",'
            '"requested_artifact_extensions":[".docx",".pptx",".xlsx",".csv",".html"],'
            '"requested_tool_names":["file_search"],"confidence":0.91}'
        ),
        user_message="structured artifact turn",
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}

    assert {"document_create", "presentation_create", "spreadsheet_create", "file_write"} <= names
    assert "file_search" not in names
    assert "file_read" not in names
    assert "workspace_summary" not in names


def test_existing_named_artifact_scope_prefers_file_delivery_tools_over_same_format_creation() -> None:
    registry = _registry()
    scoped = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(
            requested_extensions=(".xlsx",),
            existing_named_artifact_extensions=(".xlsx",),
        ),
        tool_scope_decision=TurnToolScopeDecision(
            requested_tool_names=("file_search", "file_read", "workspace_summary", "spreadsheet_create"),
            requested_artifact_extensions=(".xlsx",),
            confidence=1.0,
            valid=True,
        ),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}

    assert {"file_search", "file_read", "workspace_summary"} <= names
    assert "spreadsheet_create" not in names


def test_existing_named_artifact_regeneration_keeps_dedicated_creator_visible() -> None:
    registry = _registry()
    scoped = ScopedTurnToolRegistry(
        registry,
        evidence=TurnToolEvidence(
            requested_extensions=(".xlsx", ".pdf"),
            existing_named_artifact_extensions=(".xlsx", ".pdf"),
            existing_named_artifact_requires_new_content=True,
        ),
        tool_scope_decision=TurnToolScopeDecision(
            requested_tool_names=("file_search", "file_read", "spreadsheet_create", "pdf_create"),
            requested_artifact_extensions=(".xlsx", ".pdf"),
            confidence=1.0,
            valid=True,
        ),
    )

    names = {definition["name"] for definition in scoped.list_tool_definitions()}

    assert {"file_search", "file_read", "spreadsheet_create", "pdf_create"} <= names


def test_tool_scope_classifier_prompt_exposes_weather_and_image_scopes(monkeypatch) -> None:
    registry = _registry()
    client = _ScopeClient(
        '{"web_action":"none","scheduler_action":"none","skill_pack_action":"none",'
        '"requested_tool_names":["weather_forecast"],"confidence":0.93}'
    )
    stored: list[dict[str, object]] = []
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(None, False),
    )
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "set_json",
        lambda *args, **kwargs: stored.append({"args": args, "kwargs": kwargs}),
    )

    decision = build_turn_tool_scope_decision(
        model_client=client,
        user_message="live atmospheric condition request",
        evidence=TurnToolEvidence(),
        registry=registry,
        force_model_decision=True,
    )

    assert decision.requested_tool_names == ("weather_forecast",)
    prompt_payload = client.calls[0]["messages"][0]["content"][0]["text"]
    assert '"weather"' in prompt_payload
    assert '"image_generation"' in prompt_payload
    assert "weather_forecast" in client.calls[0]["system"]
    assert "image_generate" in client.calls[0]["system"]
    assert "Do not choose weather merely because a request mentions a day, date, time, or location" in client.calls[0]["system"]
    assert "google-calendar/calendar_list" in client.calls[0]["system"]
    assert "eligible set discovered through list_crons" in client.calls[0]["system"]
    assert stored


def test_tool_scope_classifier_marks_unregistered_image_generation_unavailable(monkeypatch) -> None:
    registry = ToolRegistry()
    for name in ("request_tool_scope", "web_search", "browser_navigate"):
        registry.register(
            _spec(name),
            lambda invocation: ToolResult(invocation.invocation_id, invocation.tool_name, "completed", {}),
        )
    client = _ScopeClient(
        '{"requested_outcome":"generated_media","web_action":"live_research",'
        '"scheduler_action":"none","skill_pack_action":"none",'
        '"requested_tool_names":["web_search","browser_navigate"],"confidence":0.97}'
    )
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(None, False),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)

    decision = build_turn_tool_scope_decision(
        model_client=client,
        user_message="structured request for newly generated visual media",
        evidence=TurnToolEvidence(),
        registry=registry,
        force_model_decision=True,
    )

    assert decision.allow_web_tools is False
    assert decision.requested_outcome == "generated_media"
    assert decision.requested_tool_names == ()
    prompt_payload = client.calls[0]["messages"][0]["content"][0]["text"]
    assert '"available_special_tool_scopes": ["web_or_browser"]' in prompt_payload
    assert '"unavailable_special_tool_scopes": ["weather", "market_data", "image_generation"]' in prompt_payload
    assert "public web/search results are not a substitute for newly generated visual media" in client.calls[0]["system"]


def test_tool_scope_classifier_exposes_market_quote_scope(monkeypatch) -> None:
    registry = _registry()
    client = _ScopeClient(
        '{"web_action":"none","scheduler_action":"none","skill_pack_action":"none",'
        '"requested_tool_names":["market_quote"],"confidence":0.93}'
    )
    monkeypatch.setattr(
        turn_context_policy.runtime_cache,
        "get_json",
        lambda *args, **kwargs: RuntimeCacheResult(None, False),
    )
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kwargs: None)

    decision = build_turn_tool_scope_decision(
        model_client=client,
        user_message="current ticker quote request",
        evidence=TurnToolEvidence(),
        registry=registry,
        force_model_decision=True,
    )

    assert decision.requested_tool_names == ("market_quote",)
    prompt_payload = client.calls[0]["messages"][0]["content"][0]["text"]
    assert '"market_data"' in prompt_payload
    assert "market_quote" in client.calls[0]["system"]


def test_connector_provider_context_stays_vendor_neutral(monkeypatch) -> None:
    from nullion import connections, skill_pack_installer, turn_context_policy

    monkeypatch.setattr(
        connections,
        "load_connection_registry",
        lambda: SimpleNamespace(
            connections=[
                SimpleNamespace(
                    provider_id="skill_pack_connector_example_gateway_skill",
                    display_name="Example connector",
                    permission_mode="write",
                    credential_scope="workspace",
                    active=True,
                )
            ]
        ),
    )
    monkeypatch.setattr(
        skill_pack_installer,
        "list_installed_skill_packs",
        lambda: (),
    )

    providers = turn_context_policy._active_connector_provider_context()

    assert providers[0]["provider_id"] == "skill_pack_connector_example_gateway_skill"
    assert providers[0]["structured_tools"] == ["connector_request"]
    assert "active_app_ids" not in providers[0]
    assert "app_statuses" not in providers[0]


def test_connector_provider_context_derives_google_typed_tools_from_pack_references(monkeypatch) -> None:
    from nullion import connections, skill_pack_installer, turn_context_policy

    monkeypatch.setattr(
        connections,
        "load_connection_registry",
        lambda: SimpleNamespace(
            connections=[
                SimpleNamespace(
                    provider_id="skill_pack_connector_example_gateway_skill",
                    display_name="Example connector",
                    permission_mode="write",
                    credential_scope="workspace",
                    active=True,
                )
            ]
        ),
    )
    monkeypatch.setattr(
        skill_pack_installer,
        "list_installed_skill_packs",
        lambda: [SimpleNamespace(pack_id="example/gateway-skill")],
    )
    monkeypatch.setattr(
        skill_pack_installer,
        "get_installed_skill_pack",
        lambda pack_id: SimpleNamespace(pack_id=pack_id),
    )
    monkeypatch.setattr(
        skill_pack_installer,
        "list_skill_pack_reference_paths",
        lambda pack_id: [
            "references/google-mail/readme.md",
            "references/google-calendar/readme.md",
            "references/notion/readme.md",
        ],
    )

    providers = turn_context_policy._active_connector_provider_context()

    assert providers[0]["active_app_ids"] == ["google-mail", "google-calendar", "notion"]
    assert providers[0]["structured_tools"] == [
        "connector_request",
        "email_search",
        "email_read",
        "email_attachment_read",
        "calendar_list",
        "email_send",
        "calendar_create",
        "calendar_update",
        "calendar_respond",
        "calendar_delete",
    ]


def test_workspace_connection_prompt_stays_vendor_neutral(tmp_path, monkeypatch) -> None:
    from nullion import connections

    path = tmp_path / "connections.json"
    registry = connections.ConnectionRegistry(
        connections=[
            connections.ProviderConnection(
                connection_id="conn-1",
                workspace_id="workspace_admin",
                provider_id="skill_pack_connector_example_gateway_skill",
                display_name="Example connector",
                credential_ref="EXAMPLE_API_KEY",
                permission_mode="write",
            )
        ]
    )
    connections.save_connection_registry(registry, path=path)
    monkeypatch.setattr(connections, "load_connection_registry", lambda: registry)

    text = connections.format_workspace_connections_for_prompt()

    assert (
        "typed_tools=connector_request,email_search,email_read,email_attachment_read,calendar_list,email_send,"
        "calendar_create,calendar_update,calendar_respond,calendar_delete"
    ) in text
    assert "app_statuses=" not in text
    assert "route_policy=" not in text


@pytest.mark.parametrize('web_action', ['open_url', 'browser_interaction', 'live_research'])
def test_web_scope_expansion_preserves_the_existing_typed_action(web_action):
    scoped = ScopedTurnToolRegistry(_registry(), evidence=TurnToolEvidence(),
        tool_scope_decision=TurnToolScopeDecision(web_action=web_action, valid=True, confidence=1))
    result, widened = scoped.apply_scope_request(ToolInvocation('expand', 'request_tool_scope', 'member',
        {'capabilities': ['web'], 'tool_names': ['browser_navigate']}))
    assert result.status == 'completed'
    assert widened.turn_tool_scope_decision.web_action == web_action


@pytest.mark.parametrize("prompt", ["Check my email", "Revisa mi bandeja", "受信箱を確認して"])
def test_collection_scope_discards_cached_mandatory_detail_reads(monkeypatch, prompt):
    import json
    old = {"web_action": "none", "scheduler_action": "none", "skill_pack_action": "connector", "connector_app_ids": ["google-mail"],
           "connector_source_user_requested": True, "connector_source_evidence": prompt,
           "requested_tool_names": ["email_search", "email_read"],
           "required_tool_names": ["email_search", "email_read"], "confidence": 1.0, "valid": True}
    client = _ScopeClient(json.dumps({**old, "required_tool_names": ["email_search"]}))
    monkeypatch.setattr(turn_context_policy, "_active_connector_provider_context", lambda: [{
        "provider_id": "custom_connector_provider", "display_name": "Example connector",
        "active_app_ids": ["google-mail"], "structured_tools": ["email_search", "email_read"]}])
    monkeypatch.setattr(turn_context_policy.runtime_cache, "get_json", lambda *args, **kw:
                        RuntimeCacheResult(old, True) if kw["version"] == "v50" else RuntimeCacheResult(None, False))
    monkeypatch.setattr(turn_context_policy.runtime_cache, "set_json", lambda *args, **kw: None)
    decision = build_turn_tool_scope_decision(model_client=client, user_message=prompt,
        evidence=TurnToolEvidence(current_user_message=prompt), registry=_registry(), force_model_decision=True)
    assert len(client.calls) == 1
    assert set(decision.requested_tool_names) == {"email_search", "email_read"}
    assert decision.required_tool_names == ("email_search",)
