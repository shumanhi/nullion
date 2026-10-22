from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from nullion import builder_memory
from nullion.context_bus import CONTEXT_BUS_MAX_BYTES, ContextBus, ContextTimeoutError
from nullion.memory import UserMemoryEntry, UserMemoryKind


def _memory_kind_value(kind: object) -> str:
    value = str(getattr(kind, "value", kind)).lower()
    if value.endswith(".preference"):
        return "preference"
    if value.endswith(".environment_fact"):
        return "environment_fact"
    if value.endswith(".fact"):
        return "fact"
    return value


@pytest.mark.asyncio
async def test_context_bus_publish_get_wait_and_clear() -> None:
    bus = ContextBus()

    waiter = asyncio.create_task(bus.wait_for("page", group_id="g1", timeout_s=1))
    await asyncio.sleep(0)
    bus.publish("page", {"html": "<h1>Hi</h1>"}, group_id="g1", agent_id="a1", task_id="t1")

    assert await waiter == {"html": "<h1>Hi</h1>"}
    assert bus.get("page", group_id="g1") == {"html": "<h1>Hi</h1>"}
    assert bus.group_keys("g1") == ["page"]
    assert bus.get("page", group_id="other") is None

    bus.clear_group("g1")
    assert bus.group_keys("g1") == []


@pytest.mark.asyncio
async def test_context_bus_timeout_and_cleared_waiter_errors() -> None:
    bus = ContextBus()

    with pytest.raises(ContextTimeoutError, match="not published"):
        await bus.wait_for("missing", group_id="g1", timeout_s=0.001)

    waiter = asyncio.create_task(bus.wait_for("soon-cleared", group_id="g2", timeout_s=1))
    await asyncio.sleep(0)
    bus.clear_group("g2")
    with pytest.raises(ContextTimeoutError, match="set but value is None"):
        await waiter


def test_context_bus_truncates_large_text_and_bytes() -> None:
    bus = ContextBus()
    bus.publish("text", "x" * (CONTEXT_BUS_MAX_BYTES + 10), group_id="g", agent_id="a", task_id="t")
    bus.publish("bytes", b"x" * (CONTEXT_BUS_MAX_BYTES + 10), group_id="g", agent_id="a", task_id="t")

    assert bus.get("text", group_id="g").endswith(f"showing first {CONTEXT_BUS_MAX_BYTES}]")
    assert bus.get("bytes", group_id="g").endswith(b"bytes total]")


class MemoryStore:
    def __init__(self, entries: list[UserMemoryEntry]) -> None:
        self.user_facts = {entry.entry_id: entry for entry in entries if _memory_kind_value(entry.kind) == "fact"}
        self.preferences = {
            entry.entry_id: entry for entry in entries if _memory_kind_value(entry.kind) == "preference"
        }
        self.environment_facts = {
            entry.entry_id: entry for entry in entries if _memory_kind_value(entry.kind) == "environment_fact"
        }
        self.written: list[UserMemoryEntry] = []
        self.removed: list[str] = []

    def list_user_memory_entries(self) -> list[UserMemoryEntry]:
        return [
            *self.user_facts.values(),
            *self.preferences.values(),
            *self.environment_facts.values(),
        ]

    def get_user_memory_entry(self, entry_id: str) -> UserMemoryEntry | None:
        for entry in self.list_user_memory_entries() + self.written:
            if entry.entry_id == entry_id:
                return entry
        return None

    def add_user_memory_entry(self, entry: UserMemoryEntry) -> None:
        self.written.append(entry)
        self.remove_user_memory_entry(entry.entry_id)
        if _memory_kind_value(entry.kind) == "preference":
            self.preferences[entry.entry_id] = entry
        elif _memory_kind_value(entry.kind) == "environment_fact":
            self.environment_facts[entry.entry_id] = entry
        else:
            self.user_facts[entry.entry_id] = entry

    def remove_user_memory_entry(self, entry_id: str) -> bool:
        removed = False
        for collection in (self.user_facts, self.preferences, self.environment_facts):
            if entry_id in collection:
                collection.pop(entry_id, None)
                removed = True
        if removed:
            self.removed.append(entry_id)
        return removed


class Runtime:
    def __init__(self, store: MemoryStore) -> None:
        self.store = store
        self.checkpoints = 0
        self.checkpoint_count = 0

    def checkpoint(self) -> None:
        self.checkpoints += 1
        self.checkpoint_count += 1


class ModelClient:
    def __init__(self, text: str, *, reject_system_kwarg: bool = False) -> None:
        self.text = text
        self.reject_system_kwarg = reject_system_kwarg
        self.calls: list[dict] = []

    def create(self, **kwargs):
        if self.reject_system_kwarg and "system" in kwargs:
            raise TypeError("system unsupported")
        self.calls.append(kwargs)
        return {"content": [{"type": "text", "text": self.text}]}


def _entry(index: int, owner: str = "workspace:one") -> UserMemoryEntry:
    when = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=index)
    kind = [UserMemoryKind.FACT, UserMemoryKind.PREFERENCE, UserMemoryKind.ENVIRONMENT_FACT][index % 3]
    return UserMemoryEntry(
        entry_id=f"{owner}:e{index}",
        owner=owner,
        kind=kind,
        key=f"key_{index}",
        value=f"value {index}",
        created_at=when,
        updated_at=when,
    )


def test_compact_memory_skips_owners_below_threshold() -> None:
    store = MemoryStore([_entry(index) for index in range(builder_memory.COMPACTION_THRESHOLD - 1)])
    client = ModelClient('[{"key":"merged","value":"value","kind":"fact"}]')

    assert builder_memory.compact_memory(Runtime(store), client) == 0
    assert client.calls == []
    assert store.written == []


def test_compact_memory_writes_replacements_and_removes_candidates() -> None:
    entries = [_entry(index) for index in range(builder_memory.COMPACTION_THRESHOLD + 2)]
    store = MemoryStore(entries)
    client = ModelClient(
        """```json
        [
          {"key": "editor", "value": "likes compact tests", "kind": "preference"},
          {"key": "bad_kind", "value": "falls back", "kind": "surprise"}
        ]
        ```"""
    )

    written = builder_memory.compact_memory(Runtime(store), client)

    assert written == 2
    assert [entry.key for entry in store.written] == ["editor", "bad_kind"]
    assert store.written[0].kind is UserMemoryKind.PREFERENCE
    assert store.written[1].kind is UserMemoryKind.FACT
    newest_ids = {entry.entry_id for entry in sorted(entries, key=lambda item: item.updated_at, reverse=True)[:5]}
    remaining_original_ids = set(store.user_facts) | set(store.preferences) | set(store.environment_facts)
    assert newest_ids.issubset(remaining_original_ids)
    assert entries[0].entry_id not in remaining_original_ids


def test_compact_memory_supports_model_clients_without_system_kwarg() -> None:
    entries = [_entry(index) for index in range(builder_memory.COMPACTION_THRESHOLD)]
    store = MemoryStore(entries)
    client = ModelClient('[{"key":"merged","value":"value","kind":"fact"}]', reject_system_kwarg=True)

    assert builder_memory.compact_memory(Runtime(store), client) == 1
    assert client.calls[0]["messages"][0]["role"] == "system"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", []),
        ("not json", []),
        ('{"key":"not-list"}', []),
        ('prefix [{"key":"inside","value":"ok","kind":"fact"}] suffix', [{"key": "inside", "value": "ok", "kind": "fact"}]),
        ('[{"key":"ok"}, "drop"]', [{"key": "ok"}]),
    ],
)
def test_parse_compacted_entries_tolerates_model_output_shapes(raw: str, expected: list[dict]) -> None:
    assert builder_memory._parse_compacted_entries(raw) == expected


def test_turn_memory_parser_rejects_runtime_identifier_state() -> None:
    raw = """
    {
      "entries": [
        {
          "key": "cnn_cron_current_id",
          "value": "CNN cron is named with current ID 93570269.",
          "kind": "preference",
          "confidence": 0.99
        },
        {
          "key": "correo_previsualizacion_preferencia",
          "value": "Mostrar vista previa del correo y esperar aprobacion explicita antes de enviar.",
          "kind": "preference",
          "content_type": "durable_memory",
          "confidence": 0.99
        }
      ]
    }
    """

    entries = builder_memory._parse_turn_memory_entries(raw)

    assert entries == [
        {
            "key": "correo_previsualizacion_preferencia",
            "value": "Mostrar vista previa del correo y esperar aprobacion explicita antes de enviar.",
            "kind": UserMemoryKind.PREFERENCE,
        }
    ]


def test_turn_memory_parser_rejects_operational_schedule_and_runtime_state() -> None:
    raw = """
    {
      "entries": [
        {
          "key": "nullion_monitor_schedule",
          "value": "Active Nullion AI social + GitHub detailed-mentions monitor d4f7034e is enabled hourly on cron 0 * * * *.",
          "kind": "preference",
          "confidence": 0.99
        },
        {
          "key": "telegram_unavailable_runtime",
          "value": "Telegram delivery is unavailable in the runtime.",
          "kind": "environment_fact",
          "confidence": 0.99
        },
        {
          "key": "reminders_due_iso",
          "value": "2026-05-06T10:30:00+00:00",
          "kind": "environment_fact",
          "confidence": 0.99
        },
        {
          "key": "preferencia_de_informes",
          "value": "Prefiere informes concisos con enlaces de origen verificables.",
          "kind": "preference",
          "content_type": "durable_memory",
          "confidence": 0.99
        }
      ]
    }
    """

    entries = builder_memory._parse_turn_memory_entries(raw)

    assert entries == [
        {
            "key": "preferencia_de_informes",
            "value": "Prefiere informes concisos con enlaces de origen verificables.",
            "kind": UserMemoryKind.PREFERENCE,
        }
    ]


def test_turn_memory_parser_rejects_scheduled_task_delivery_state() -> None:
    raw = """
    {
      "entries": [
        {
          "key": "scheduled_email_deadline_triage_maton_gmail",
          "value": "Scheduled task \\"Email deadline triage — Maton Gmail\\" can be triggered on request; delivery saves to configured destination.",
          "kind": "environment_fact",
          "confidence": 0.99
        },
        {
          "key": "report_style",
          "value": "Prefiere informes concisos con decisiones destacadas.",
          "kind": "preference",
          "content_type": "durable_memory",
          "confidence": 0.99
        }
      ]
    }
    """

    entries = builder_memory._parse_turn_memory_entries(raw)

    assert entries == [
        {
            "key": "report_style",
            "value": "Prefiere informes concisos con decisiones destacadas.",
            "kind": UserMemoryKind.PREFERENCE,
        }
    ]


def test_turn_memory_parser_rejects_operational_metrics_and_account_memory() -> None:
    raw = """
    {
      "entries": [
        {
          "key": "nullion_report_format_preferences",
          "value": "Nullion scheduled monitor should produce complete mobile HTML with no empty graphs and clearly report failed metric sources.",
          "kind": "preference",
          "confidence": 0.99
        },
        {
          "key": "nullion_monitor_use_gh_cli",
          "value": "For Nullion GitHub audience metrics, use local GitHub CLI read-only `gh api` calls, not web scraping.",
          "kind": "preference",
          "confidence": 0.99
        },
        {
          "key": "nullion_monitor_github_traffic_method",
          "value": "For Nullion GitHub traffic metrics, use local GitHub CLI gh api calls, not web scraping; report auth/scope failures clearly.",
          "kind": "preference",
          "confidence": 0.99
        },
        {
          "key": "nullion_monitor_exclude_dev_history",
          "value": "Nullion public awareness/GitHub audience reports should exclude all Git history and development info.",
          "kind": "preference",
          "confidence": 0.99
        },
        {
          "key": "gmail_account_address",
          "value": "User's Gmail account is profile@example.com.",
          "kind": "preference",
          "confidence": 0.99
        },
        {
          "key": "preferencia_de_resumenes",
          "value": "Prefiere resumenes breves con decisiones destacadas.",
          "kind": "preference",
          "content_type": "durable_memory",
          "confidence": 0.99
        }
      ]
    }
    """

    entries = builder_memory._parse_turn_memory_entries(raw)

    assert entries == [
        {
            "key": "preferencia_de_resumenes",
            "value": "Prefiere resumenes breves con decisiones destacadas.",
            "kind": UserMemoryKind.PREFERENCE,
        }
    ]


def test_turn_memory_parser_allows_tool_names_in_general_preferences() -> None:
    raw = """
    {
      "entries": [
        {
          "key": "github_research_preference",
          "value": "For GitHub research, prefer read-only gh api calls over page scraping when the GitHub CLI is available.",
          "kind": "preference",
          "content_type": "durable_memory",
          "confidence": 0.99
        }
      ]
    }
    """

    entries = builder_memory._parse_turn_memory_entries(raw)

    assert entries == [
        {
            "key": "github_research_preference",
            "value": "For GitHub research, prefer read-only gh api calls over page scraping when the GitHub CLI is available.",
            "kind": UserMemoryKind.PREFERENCE,
        }
    ]


def test_turn_memory_parser_discards_product_observations_but_keeps_style_memory() -> None:
    raw = """
    {
      "entries": [
        {
          "key": "calendar_event_lookup_guard",
          "value": "Do not dump unrelated calendar rows when answering calendar event lookup questions.",
          "kind": "preference",
          "content_type": "task_observation",
          "confidence": 0.99
        },
        {
          "key": "reply_style_emoji_structure",
          "value": "Use more emojis where useful and keep time-sensitive replies well structured.",
          "kind": "preference",
          "content_type": "durable_memory",
          "confidence": 0.99
        },
        {
          "key": "artifact_delivery_failure",
          "value": "A Croton Gorge Park PDF failed to attach during this task.",
          "kind": "fact",
          "content_type": "task_observation",
          "confidence": 0.99
        }
      ]
    }
    """

    entries = builder_memory._parse_turn_memory_entries(raw)

    assert entries == [
        {
            "key": "reply_style_emoji_structure",
            "value": "Use more emojis where useful and keep time-sensitive replies well structured.",
            "kind": UserMemoryKind.PREFERENCE,
        }
    ]


def test_turn_memory_parser_keeps_date_like_durable_facts() -> None:
    raw = """
    {
      "entries": [
        {
          "key": "birthday",
          "value": "Birthday is 19900101.",
          "kind": "preference",
          "content_type": "durable_memory",
          "confidence": 0.99
        }
      ]
    }
    """

    assert builder_memory._parse_turn_memory_entries(raw)[0]["key"] == "birthday"


def test_memory_prompt_selection_ignores_runtime_identifier_entries() -> None:
    stable = UserMemoryEntry(
        entry_id="workspace:one:stable",
        owner="workspace:one",
        kind=UserMemoryKind.PREFERENCE,
        key="correo_previsualizacion_preferencia",
        value="Mostrar vista previa del correo y esperar aprobacion explicita antes de enviar.",
        use_score=10,
    )
    runtime_id = UserMemoryEntry(
        entry_id="workspace:one:cnn_cron_current_id",
        owner="workspace:one",
        kind=UserMemoryKind.PREFERENCE,
        key="cnn_cron_current_id",
        value="CNN cron is named with current ID 93570269.",
        use_score=100,
    )

    selected = builder_memory.select_memory_entries_for_prompt([runtime_id, stable])

    assert selected == [stable]


def test_memory_prompt_selection_ignores_reinforced_operational_state() -> None:
    stable = UserMemoryEntry(
        entry_id="workspace:one:stable",
        owner="workspace:one",
        kind=UserMemoryKind.PREFERENCE,
        key="preferencia_de_informes",
        value="Prefiere informes concisos con enlaces de origen verificables.",
        use_score=1,
    )
    operational = UserMemoryEntry(
        entry_id="workspace:one:nullion_monitor_schedule",
        owner="workspace:one",
        kind=UserMemoryKind.PREFERENCE,
        key="nullion_monitor_schedule",
        value="Active Nullion AI social + GitHub detailed-mentions monitor d4f7034e is enabled hourly on cron 0 * * * *.",
        use_score=100,
    )

    selected = builder_memory.select_memory_entries_for_prompt([operational, stable])

    assert selected == [stable]


def test_memory_prompt_selection_ignores_scheduled_task_delivery_state() -> None:
    stable = UserMemoryEntry(
        entry_id="workspace:one:stable",
        owner="workspace:one",
        kind=UserMemoryKind.PREFERENCE,
        key="report_style",
        value="Prefiere informes concisos con decisiones destacadas.",
        use_score=1,
    )
    scheduled = UserMemoryEntry(
        entry_id="workspace:one:scheduled_email_deadline_triage_maton_gmail",
        owner="workspace:one",
        kind=UserMemoryKind.ENVIRONMENT_FACT,
        key="scheduled_email_deadline_triage_maton_gmail",
        value='Scheduled task "Email deadline triage — Maton Gmail" can be triggered on request; delivery saves to configured destination.',
        use_score=100,
    )

    selected = builder_memory.select_memory_entries_for_prompt([scheduled, stable])

    assert selected == [stable]


def test_builder_reflection_skip_helper_uses_structured_scheduler_tool_names() -> None:
    assert builder_memory.should_skip_builder_reflection_for_tool_results(
        tool_names=["list_crons", "run_cron"],
        tool_results=[],
    )
    assert builder_memory.should_skip_builder_reflection_for_tool_results(
        tool_names=[],
        tool_results=[
            SimpleNamespace(tool_name="list_crons"),
            {"tool_name": "run_cron"},
        ],
    )
    assert not builder_memory.should_skip_builder_reflection_for_tool_results(
        tool_names=["run_cron", "browser_navigate"],
        tool_results=[],
    )


def test_explicit_turn_memory_capture_uses_structured_model_output() -> None:
    store = MemoryStore([])
    runtime = Runtime(store)
    client = ModelClient(
        """
        {
          "entries": [
            {
              "key": "color_favorito",
              "value": "El color favorito del usuario es verde.",
              "kind": "preference",
              "content_type": "durable_memory",
              "confidence": 0.99
            }
          ]
        }
        """
    )

    result = builder_memory.capture_turn_memory_claims(
        runtime,
        client,
        owner="workspace:one",
        user_message="mi color favorito es verde",
        assistant_reply="Entendido.",
    )

    assert result.written == 1
    assert store.preferences["workspace:one:color_favorito"].value == "El color favorito del usuario es verde."
    assert runtime.checkpoints == 1


def test_explicit_turn_memory_capture_preserves_future_style_preferences() -> None:
    store = MemoryStore([])
    runtime = Runtime(store)
    client = ModelClient(
        """
        {
          "entries": [
            {
              "key": "reply_style_emoji_structure",
              "value": "Use more emojis where useful and keep time-sensitive replies well structured.",
              "kind": "preference",
              "content_type": "durable_memory",
              "confidence": 0.99
            }
          ]
        }
        """
    )

    result = builder_memory.capture_turn_memory_claims(
        runtime,
        client,
        owner="workspace:one",
        user_message="Going forward use more emojis wherever it makes sense, especially for time sensitive stuff.",
        assistant_reply="Understood. I will use more emojis and clearer structure for urgent items.",
    )

    assert result.written == 1
    entry = store.preferences["workspace:one:reply_style_emoji_structure"]
    assert _memory_kind_value(entry.kind) == "preference"
    assert "emojis" in entry.value
    assert "time-sensitive" in entry.value


def test_explicit_turn_memory_prompt_names_style_preferences_as_durable() -> None:
    assert "emoji use" in builder_memory._EXPLICIT_TURN_MEMORY_SYSTEM_PROMPT
    assert "scan-friendly layout" in builder_memory._EXPLICIT_TURN_MEMORY_SYSTEM_PROMPT


def test_explicit_turn_memory_capture_rejects_runtime_identifier_output() -> None:
    store = MemoryStore([])
    runtime = Runtime(store)
    client = ModelClient(
        """
        {
          "entries": [
            {
              "key": "cnn_cron_current_id",
              "value": "CNN cron is named with current ID 93570269.",
              "kind": "preference",
              "confidence": 0.99
            }
          ]
        }
        """
    )

    result = builder_memory.capture_turn_memory_claims(
        runtime,
        client,
        owner="workspace:one",
        user_message="cnn schedule update",
        assistant_reply="Done.",
    )

    assert result.skipped == "empty"
    assert store.written == []


def test_compact_memory_fails_silently_when_store_raises() -> None:
    class BrokenStore:
        def list_user_memory_entries(self):
            raise RuntimeError("boom")

    assert builder_memory.compact_memory(Runtime(BrokenStore()), ModelClient("[]")) == 0


def test_manage_turn_memory_rewrites_one_workspace_with_structured_model_output() -> None:
    other = _entry(1, owner="workspace:two")
    stale = UserMemoryEntry(
        entry_id="workspace:one:stale_detail",
        owner="workspace:one",
        kind=UserMemoryKind.FACT,
        key="stale_detail",
        value="remove me",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    store = MemoryStore([other, stale])
    client = ModelClient(
        """
        {
          "entries": [
            {"key": "reply_style", "value": "Prefers concise direct answers", "kind": "preference", "content_type": "durable_memory", "confidence": 0.91},
            {"key": "project_repo", "value": "Nullion app work lives in the app repo", "kind": "environment_fact", "content_type": "durable_memory", "confidence": 0.86}
          ]
        }
        """
    )

    result = builder_memory.manage_turn_memory(
        Runtime(store),
        client,
        owner="workspace:one",
        user_message="me gusta respuestas cortas",
        assistant_reply="Entendido.",
    )

    assert result.written == 2
    assert result.removed == 1
    assert "workspace:one:stale_detail" in store.removed
    assert "workspace:two:e1" in store.preferences
    assert store.preferences["workspace:one:reply_style"].value == "Prefers concise direct answers"
    assert store.environment_facts["workspace:one:project_repo"].owner == "workspace:one"


def test_manage_turn_memory_preserves_existing_preferences_omitted_by_rewrite() -> None:
    existing_preference = UserMemoryEntry(
        entry_id="workspace:one:reply_emoji_preference",
        owner="workspace:one",
        kind=UserMemoryKind.PREFERENCE,
        key="reply_emoji_preference",
        value="User prefers expressive emoji use in replies.",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    stale_fact = UserMemoryEntry(
        entry_id="workspace:one:stale_fact",
        owner="workspace:one",
        kind=UserMemoryKind.FACT,
        key="stale_fact",
        value="remove me",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    store = MemoryStore([existing_preference, stale_fact])
    client = ModelClient(
        """
        {
          "entries": [
            {"key": "project_repo", "value": "Nullion app work lives in the app repo", "kind": "environment_fact", "content_type": "durable_memory", "confidence": 0.86}
          ]
        }
        """
    )

    result = builder_memory.manage_turn_memory(
        Runtime(store),
        client,
        owner="workspace:one",
        user_message="workspace context update",
        assistant_reply="Done.",
    )

    assert result.written == 1
    assert result.removed == 1
    assert "workspace:one:stale_fact" in store.removed
    assert existing_preference.entry_id in store.preferences
    assert store.preferences[existing_preference.entry_id].value == "User prefers expressive emoji use in replies."
    assert store.environment_facts["workspace:one:project_repo"].owner == "workspace:one"


def test_manage_turn_memory_keeps_prompt_bounded_and_drops_low_confidence() -> None:
    entries = [_entry(index, owner="workspace:one") for index in range(30)]
    store = MemoryStore(entries)
    client = ModelClient(
        """
        {"entries": [
          {"key": "keep", "value": "stable", "kind": "fact", "content_type": "durable_memory", "confidence": 0.9},
          {"key": "drop", "value": "maybe", "kind": "fact", "content_type": "durable_memory", "confidence": 0.2}
        ]}
        """
    )

    result = builder_memory.manage_turn_memory(
        Runtime(store),
        client,
        owner="workspace:one",
        user_message="x" * 5000,
        assistant_reply="y" * 5000,
    )

    assert result.written == 1
    assert list(store.user_facts.values())[0].key == "keep"
    payload = client.calls[0]["messages"][1]["content"][0]["text"]
    assert len(payload) < 5000
    assert "xxxxx" in payload


def test_manage_turn_memory_enforces_configured_memory_buckets(monkeypatch) -> None:
    monkeypatch.setenv("NULLION_MEMORY_LONG_TERM_LIMIT", "1")
    monkeypatch.setenv("NULLION_MEMORY_MID_TERM_LIMIT", "1")
    monkeypatch.setenv("NULLION_MEMORY_SHORT_TERM_LIMIT", "1")
    store = MemoryStore([])
    client = ModelClient(
        """
        {"entries": [
          {"key": "stable_preference", "value": "pref one", "kind": "preference", "content_type": "durable_memory", "confidence": 0.9},
          {"key": "extra_preference", "value": "pref two", "kind": "preference", "content_type": "durable_memory", "confidence": 0.9},
          {"key": "workspace_repo", "value": "project one", "kind": "environment_fact", "content_type": "durable_memory", "confidence": 0.9},
          {"key": "extra_workspace_repo", "value": "project two", "kind": "environment_fact", "content_type": "durable_memory", "confidence": 0.9},
          {"key": "active_detail", "value": "working one", "kind": "fact", "content_type": "durable_memory", "confidence": 0.9},
          {"key": "extra_active_detail", "value": "working two", "kind": "fact", "content_type": "durable_memory", "confidence": 0.9}
        ]}
        """
    )

    result = builder_memory.manage_turn_memory(
        Runtime(store),
        client,
        owner="workspace:one",
        user_message="estructurado",
        assistant_reply="listo",
    )

    assert result.written == 3
    assert set(store.preferences) == {"workspace:one:stable_preference"}
    assert set(store.environment_facts) == {"workspace:one:workspace_repo"}
    assert set(store.user_facts) == {"workspace:one:active_detail"}
    payload = client.calls[0]["messages"][1]["content"][0]["text"]
    assert '"long_term_memory": 1' in payload
    assert '"mid_term_memory": 1' in payload
    assert '"short_term_memory": 1' in payload


def test_memory_policy_from_env_clamps_bad_values() -> None:
    policy = builder_memory.memory_policy_from_env(
        {
            "NULLION_MEMORY_LONG_TERM_LIMIT": "bad",
            "NULLION_MEMORY_MID_TERM_LIMIT": "99",
            "NULLION_MEMORY_SHORT_TERM_LIMIT": "-4",
        }
    )

    assert policy.long_term_limit == builder_memory.DEFAULT_MEMORY_LONG_TERM_LIMIT
    assert policy.mid_term_limit == 50
    assert policy.short_term_limit == builder_memory.DEFAULT_MEMORY_SHORT_TERM_LIMIT


def test_memory_policy_from_env_recovers_zero_limits_to_defaults() -> None:
    policy = builder_memory.memory_policy_from_env(
        {
            "NULLION_MEMORY_LONG_TERM_LIMIT": "0",
            "NULLION_MEMORY_MID_TERM_LIMIT": "0",
            "NULLION_MEMORY_SHORT_TERM_LIMIT": "0",
        }
    )

    assert policy.long_term_limit == builder_memory.DEFAULT_MEMORY_LONG_TERM_LIMIT
    assert policy.mid_term_limit == builder_memory.DEFAULT_MEMORY_MID_TERM_LIMIT
    assert policy.short_term_limit == builder_memory.DEFAULT_MEMORY_SHORT_TERM_LIMIT


def test_recalled_memory_gets_higher_survival_score() -> None:
    old = _entry(1, owner="workspace:one")
    hot = _entry(2, owner="workspace:one")
    old.kind = UserMemoryKind.FACT
    hot.kind = UserMemoryKind.FACT
    hot.use_score = 7.0
    hot.use_count = 3
    selected = builder_memory.select_memory_entries_for_prompt(
        [old, hot],
        policy=builder_memory.MemoryPolicy(long_term_limit=0, mid_term_limit=0, short_term_limit=1),
    )
    store = MemoryStore([old, hot])

    assert selected == [hot]
    assert builder_memory.reinforce_memory_entries(store, selected) == 1
    reinforced = store.environment_facts[hot.entry_id]
    assert reinforced.use_score == 8.0
    assert reinforced.use_count == 4
    assert reinforced.last_used_at is not None


def test_recalled_memory_promotes_toward_long_term() -> None:
    short = _entry(3, owner="workspace:one")
    short.kind = UserMemoryKind.FACT
    short.use_score = 2.0
    mid = _entry(4, owner="workspace:one")
    mid.kind = UserMemoryKind.ENVIRONMENT_FACT
    mid.use_score = 6.0
    store = MemoryStore([short, mid])

    assert builder_memory.reinforce_memory_entries(store, [short, mid]) == 2
    assert short.entry_id in store.environment_facts
    assert mid.entry_id in store.preferences


def test_memory_pressure_and_smart_cleanup_preserve_recalled_items() -> None:
    cold = _entry(1, owner="workspace:one")
    hot = _entry(2, owner="workspace:one")
    cold.kind = UserMemoryKind.PREFERENCE
    hot.kind = UserMemoryKind.PREFERENCE
    cold.use_score = 0.0
    hot.use_score = 9.0
    hot.last_used_at = datetime(2026, 1, 10, tzinfo=UTC)
    policy = builder_memory.MemoryPolicy(long_term_limit=2, mid_term_limit=5, short_term_limit=5)
    store = MemoryStore([cold, hot])

    pressure = builder_memory.memory_pressure_for_owner(store, "workspace:one", policy=policy)
    assert pressure["full"] is True
    assert pressure["full_buckets"][0]["label"] == "Long-term"

    result = builder_memory.smart_cleanup_owner_memory(store, "workspace:one", policy=policy)

    assert result.removed == 1
    assert hot.entry_id in store.preferences
    assert cold.entry_id not in store.preferences


def test_manage_turn_memory_can_auto_cleanup_without_user_prompt(monkeypatch) -> None:
    cold = _entry(1, owner="workspace:one")
    hot = _entry(2, owner="workspace:one")
    cold.kind = UserMemoryKind.PREFERENCE
    hot.kind = UserMemoryKind.PREFERENCE
    hot.use_score = 8.0
    monkeypatch.setenv("NULLION_MEMORY_LONG_TERM_LIMIT", "2")
    monkeypatch.setenv("NULLION_MEMORY_MID_TERM_LIMIT", "5")
    monkeypatch.setenv("NULLION_MEMORY_SHORT_TERM_LIMIT", "5")
    monkeypatch.setenv("NULLION_MEMORY_SMART_CLEANUP_ENABLED", "true")
    runtime = Runtime(MemoryStore([cold, hot]))
    client = ModelClient('{"entries": []}')

    result = builder_memory.manage_turn_memory(
        runtime,
        client,
        owner="workspace:one",
        user_message="structured runtime turn",
        assistant_reply="done",
    )

    assert result.removed == 1
    assert runtime.checkpoint_count == 1
    assert hot.entry_id in runtime.store.preferences
    assert cold.entry_id not in runtime.store.preferences


def test_builder_preserves_reinforced_memory_over_newer_unrecalled_memory() -> None:
    cold_new = _entry(40, owner="workspace:one")
    hot_old = _entry(1, owner="workspace:one")
    cold_new.kind = UserMemoryKind.FACT
    hot_old.kind = UserMemoryKind.FACT
    hot_old.use_score = 12.0
    hot_old.last_used_at = datetime(2026, 1, 10, tzinfo=UTC)

    selected = builder_memory.select_memory_entries_for_prompt(
        [cold_new, hot_old],
        policy=builder_memory.MemoryPolicy(long_term_limit=0, mid_term_limit=0, short_term_limit=1),
    )

    assert selected == [hot_old]


def test_manage_turn_memory_does_not_wipe_existing_memory_on_empty_rewrite() -> None:
    existing = _entry(1, owner="workspace:one")
    store = MemoryStore([existing])
    client = ModelClient('{"entries": []}')

    result = builder_memory.manage_turn_memory(
        Runtime(store),
        client,
        owner="workspace:one",
        user_message="ordinary follow up",
        assistant_reply="done",
    )

    assert result.skipped == "empty_rewrite"
    assert existing.entry_id in store.preferences
    assert store.removed == []
