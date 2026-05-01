from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
import re
from typing import TypedDict
from uuid import uuid4

from langgraph.graph import END, START, StateGraph

from nullion.deep_agent_profiles import (
    deep_agent_profile_names_for_task,
    deep_agent_skills_for_task,
    deep_agent_subagents_for_task,
)
from nullion.missions import MissionContinuationPolicy, MissionRecord, MissionStatus, MissionStep
from nullion.task_frames import TaskFrame


_COMPOSER_MODE_INSTRUCTION_RE = re.compile(
    r"^\s*Mode:\s*(?:Build|Diagnose|Remember)\.\s*"
    r"(?:"
    r"Treat this as an implementation mission\.|"
    r"Investigate the system, explain evidence, and recommend fixes\.|"
    r"Extract durable preferences or project context if appropriate\."
    r")\s*",
    re.IGNORECASE,
)


def strip_composer_mode_instruction(text: str) -> str:
    """Remove web composer mode hints before deterministic task planning."""
    stripped = _COMPOSER_MODE_INSTRUCTION_RE.sub("", str(text or ""), count=1).strip()
    return stripped or str(text or "").strip()


@dataclass(slots=True, frozen=True)
class PlannerConfig:
    max_steps: int = 20


class PlanDisposition(str, Enum):
    SINGLE_TURN = "single_turn"
    CLARIFICATION = "clarification"
    SEQUENTIAL_MISSION = "sequential_mission"
    PARALLEL_MISSION = "parallel_mission"


class PlanDispatchMode(str, Enum):
    NONE = "none"
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"


@dataclass(slots=True, frozen=True)
class ExecutionPlan:
    mission: MissionRecord
    disposition: PlanDisposition
    dispatch_mode: PlanDispatchMode
    needs_clarification: bool = False
    clarification_question: str | None = None

    @property
    def can_dispatch_mini_agents(self) -> bool:
        return self.dispatch_mode is PlanDispatchMode.PARALLEL and not self.needs_clarification

    @property
    def can_run_mission(self) -> bool:
        return self.dispatch_mode is not PlanDispatchMode.NONE and not self.needs_clarification


@dataclass(frozen=True, slots=True)
class ToolScopeDecision:
    tool_scope: tuple[str, ...] = ()
    evidence: tuple[str, ...] = ()


class _StepMetadataState(TypedDict, total=False):
    clause: str
    step_index: int
    active_task_frame: TaskFrame | None
    tool_scope_decision: ToolScopeDecision
    metadata: dict[str, object]


class TaskPlanner:
    def __init__(self, *, config: PlannerConfig | None = None) -> None:
        self._config = config or PlannerConfig()

    def build_execution_plan(
        self,
        *,
        user_message: str,
        principal_id: str,
        active_task_frame: TaskFrame | None,
        config: PlannerConfig | None = None,
    ) -> ExecutionPlan:
        effective_config = config or self._config
        normalized_message = strip_composer_mode_instruction(user_message)
        if not normalized_message:
            raise ValueError("user_message is required")
        steps = self._build_steps(
            normalized_message,
            max_steps=effective_config.max_steps,
            active_task_frame=active_task_frame,
        )
        disposition, dispatch_mode = _classify_plan_shape(steps)
        title = self._plan_title(normalized_message, steps)
        mission = MissionRecord(
            mission_id=f"mission-{uuid4().hex[:12]}",
            owner=principal_id.strip(),
            title=title,
            goal=normalized_message,
            status=MissionStatus.PENDING,
            continuation_policy=MissionContinuationPolicy.AUTO_FINISH,
            steps=steps,
        )
        return ExecutionPlan(
            mission=mission,
            disposition=disposition,
            dispatch_mode=dispatch_mode,
        )

    def plan(
        self,
        *,
        user_message: str,
        principal_id: str,
        active_task_frame: TaskFrame | None,
        config: PlannerConfig | None = None,
    ) -> MissionRecord:
        return self.build_execution_plan(
            user_message=user_message,
            principal_id=principal_id,
            active_task_frame=active_task_frame,
            config=config,
        ).mission


    def _build_steps(
        self,
        user_message: str,
        *,
        max_steps: int,
        active_task_frame: TaskFrame | None,
    ) -> tuple[MissionStep, ...]:
        clauses = [clause for clause in _split_clauses(user_message) if clause]
        if not clauses:
            clauses = [user_message]
        steps: list[MissionStep] = []
        for index, clause in enumerate(clauses[:max_steps], start=1):
            steps.append(
                MissionStep(
                    step_id=str(index),
                    title=_step_title(clause),
                    status="pending",
                    kind="tool",
                    metadata=_step_metadata(
                        clause,
                        step_index=index,
                        active_task_frame=active_task_frame,
                    ),
                )
            )
        return tuple(steps)

    def _plan_title(self, user_message: str, steps: tuple[MissionStep, ...]) -> str:
        if steps:
            return _truncate_title(steps[0].title)
        return _truncate_title(user_message)


def _split_clauses(message: str) -> list[str]:
    normalized = message.replace("\n", " ").strip()
    if not normalized:
        return []
    separators = [" and then ", " then ", ";", " and "]
    clauses = [normalized]
    for separator in separators:
        if separator in normalized.lower():
            lowered = normalized
            pieces: list[str] = []
            remaining = lowered
            while True:
                index = remaining.lower().find(separator)
                if index < 0:
                    pieces.append(remaining)
                    break
                left = remaining[:index]
                if left.strip():
                    pieces.append(left)
                remaining = remaining[index + len(separator):]
            clauses = pieces or [normalized]
            break
    cleaned: list[str] = []
    for clause in clauses:
        stripped = clause.strip().strip(".,;:")
        if not stripped:
            continue
        if stripped.lower().startswith("and then "):
            stripped = stripped[9:]
        elif stripped.lower().startswith("then "):
            stripped = stripped[5:]
        cleaned.append(stripped)
    if len(cleaned) > 1 and separator == " and " and not _substantial_parallel_clauses(cleaned):
        return [normalized]
    return cleaned


def _step_title(clause: str) -> str:
    stripped = clause.strip().strip(".,;:")
    if not stripped:
        return "Step"
    return stripped[0].upper() + stripped[1:]


def _truncate_title(title: str, limit: int = 60) -> str:
    compact = " ".join(title.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1].rstrip() + "…"


def _substantial_parallel_clauses(clauses: list[str]) -> bool:
    return all(len(clause.split()) >= 3 for clause in clauses)


def _classify_plan_shape(steps: tuple[MissionStep, ...]) -> tuple[PlanDisposition, PlanDispatchMode]:
    if len(steps) <= 1:
        return PlanDisposition.SINGLE_TURN, PlanDispatchMode.NONE
    if any(step.metadata.get("checkpoint_before") for step in steps if isinstance(step.metadata, dict)):
        return PlanDisposition.SEQUENTIAL_MISSION, PlanDispatchMode.SEQUENTIAL
    return PlanDisposition.PARALLEL_MISSION, PlanDispatchMode.PARALLEL


CHECKPOINT_PHRASES = (
    "check with me before",
    "let me know before",
    "ask me first",
    "confirm before",
    "pause before",
)


def _step_metadata(clause: str, *, step_index: int, active_task_frame: TaskFrame | None) -> dict[str, object]:
    final_state = _compiled_step_metadata_graph().invoke(
        {
            "clause": clause,
            "step_index": step_index,
            "active_task_frame": active_task_frame,
            "metadata": {},
        },
        config={"configurable": {"thread_id": "task-planner-step-metadata"}},
    )
    return dict(final_state.get("metadata") or {})


def _step_base_metadata_node(state: _StepMetadataState) -> dict[str, object]:
    metadata: dict[str, object] = {
        "step_index": int(state["step_index"]),
        "source_clause": state["clause"],
    }
    return {"metadata": metadata}


def _step_scope_metadata_node(state: _StepMetadataState) -> dict[str, object]:
    metadata = dict(state.get("metadata") or {})
    decision = state.get("tool_scope_decision") or ToolScopeDecision()
    metadata["tool_scope"] = list(decision.tool_scope)
    if decision.evidence:
        metadata["tool_scope_evidence"] = list(decision.evidence)
    profile_task = _PlannerProfileTask(state["clause"], list(decision.tool_scope))
    profiles = deep_agent_profile_names_for_task(profile_task)
    if profiles:
        metadata["deep_agent_profiles"] = profiles
        metadata["deep_agent_skills"] = deep_agent_skills_for_task(profile_task)
        metadata["deep_agent_subagents"] = [
            str(subagent["name"])
            for subagent in deep_agent_subagents_for_task(profile_task)
            if isinstance(subagent.get("name"), str)
        ]
    metadata["checkpoint_before"] = _contains_checkpoint_phrase(state["clause"])
    return {"metadata": metadata}


def _step_tool_scope_node(state: _StepMetadataState) -> dict[str, object]:
    return {"tool_scope_decision": _tool_scope_decision_for_clause(state["clause"])}


@dataclass(frozen=True, slots=True)
class _PlannerProfileTask:
    description: str
    allowed_tools: list[str]

    @property
    def title(self) -> str:
        return self.description


def _step_active_frame_metadata_node(state: _StepMetadataState) -> dict[str, object]:
    metadata = dict(state.get("metadata") or {})
    active_task_frame = state.get("active_task_frame")
    if active_task_frame is not None:
        metadata["active_task_frame_id"] = active_task_frame.frame_id
        metadata["active_task_frame_status"] = active_task_frame.status.value
        if active_task_frame.operation.value:
            metadata["active_task_frame_operation"] = active_task_frame.operation.value
    return {"metadata": metadata}


@lru_cache(maxsize=1)
def _compiled_step_metadata_graph():
    graph = StateGraph(_StepMetadataState)
    graph.add_node("base", _step_base_metadata_node)
    graph.add_node("tool_scope", _step_tool_scope_node)
    graph.add_node("scope", _step_scope_metadata_node)
    graph.add_node("active_frame", _step_active_frame_metadata_node)
    graph.add_edge(START, "base")
    graph.add_edge("base", "tool_scope")
    graph.add_edge("tool_scope", "scope")
    graph.add_edge("scope", "active_frame")
    graph.add_edge("active_frame", END)
    return graph.compile()


def _contains_checkpoint_phrase(text: str) -> bool:
    lowered = text.lower()
    return any(phrase in lowered for phrase in CHECKPOINT_PHRASES)


def _normalized_clause_words(clause: str) -> tuple[str, ...]:
    return tuple(re.findall(r"[a-z0-9]+", clause.lower()))


def _clause_has_url_target(clause: str) -> bool:
    lowered = clause.lower()
    return bool(
        "http://" in lowered
        or "https://" in lowered
        or re.search(r"\bwww\.[a-z0-9.-]+\.[a-z]{2,}\b", lowered)
    )


def _tool_scope_decision_for_clause(clause: str) -> ToolScopeDecision:
    words = _normalized_clause_words(clause)
    word_set = set(words)
    scope: list[str] = []
    evidence: list[str] = []
    if word_set & {"email", "mail"}:
        scope.append("email_send")
        evidence.append("communication_target")
    if word_set & {"fetch", "open", "visit", "browse"} or _clause_has_url_target(clause):
        scope.append("web_fetch")
        evidence.append("web_source_target")
    elif word_set & {"search", "find"}:
        scope.append("web_search")
        evidence.append("search_intent")
    return ToolScopeDecision(
        tool_scope=tuple(dict.fromkeys(scope)),
        evidence=tuple(dict.fromkeys(evidence)),
    )


__all__ = [
    "ExecutionPlan",
    "PlanDispatchMode",
    "PlanDisposition",
    "PlannerConfig",
    "TaskPlanner",
]
