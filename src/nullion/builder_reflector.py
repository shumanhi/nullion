"""Builder reflector — LLM-powered skill induction from turn outcomes.

After a multi-tool turn completes, the reflector asks the model to look at
what it just did and decide if that work is worth capturing as a reusable
skill. If yes, it generates the full skill definition — title, trigger,
ordered steps, tags — grounded in the actual work that happened.

This is intentionally lightweight:
- Only fires when tools were actually used (≥2 distinct tools)
- Uses a tight JSON schema so the model can't ramble
- Validates the output before creating a proposal
- Fails silently on any error (never blocks the main turn)

The result is proposals that are specific and accurate, not boilerplate.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

from nullion.builder import BuilderDecisionType, BuilderProposal
from nullion.builder_observer import PatternSignal, TurnSignal

logger = logging.getLogger(__name__)

_MIN_TOOLS_FOR_REFLECTION = 2     # don't reflect on single-tool turns
_MAX_REPLY_TOKENS = 350

_REFLECTION_SYSTEM_PROMPT = """\
You are the Builder component of an AI assistant system called Nullion.
Your job is to look at a completed turn and decide if the work is worth
capturing as a reusable skill — a procedure the assistant can follow
automatically next time the user asks for something similar.

A skill is worth capturing when:
- The assistant used 2+ tools in a non-trivial sequence
- The task is clearly repeatable (not a one-off unique request)
- The steps form a coherent, teachable procedure

A skill is NOT worth capturing when:
- It was a simple single-step lookup
- The task was purely conversational
- The steps are too context-specific to reuse

Respond ONLY with valid JSON matching exactly one of these schemas:

If a skill should be captured:
{
  "should_propose": true,
  "title": "Short skill name (≤8 words, sentence case)",
  "trigger": "Use when the user wants to ...",
  "steps": [
    "Step 1: ...",
    "Step 2: ...",
    "Step 3: ..."
  ],
  "tags": ["tag1", "tag2"],
  "confidence": 0.82
}

If no skill should be captured:
{
  "should_propose": false
}

No markdown, no explanation, JSON only.\
"""


@dataclass(slots=True)
class ReflectionResult:
    should_propose: bool
    proposal: BuilderProposal | None = None
    raw_json: str | None = None


def reflect_on_turn(
    *,
    model_client,
    user_message: str,
    assistant_reply: str | None,
    turn_signal: TurnSignal,
) -> ReflectionResult:
    """Ask the LLM whether this turn should become a skill.

    Returns a ReflectionResult. Always succeeds — errors return
    should_propose=False and are logged at DEBUG level.
    """
    if turn_signal.tool_count < _MIN_TOOLS_FOR_REFLECTION:
        return ReflectionResult(should_propose=False)
    if turn_signal.outcome.value != "success":
        return ReflectionResult(should_propose=False)

    distinct_tools = list(dict.fromkeys(turn_signal.tool_names))   # ordered dedup
    tool_list = ", ".join(distinct_tools)
    reply_preview = (assistant_reply or "")[:400]

    user_content = (
        f"User request: {user_message}\n\n"
        f"Tools used (in order): {tool_list}\n\n"
        f"Assistant reply: {reply_preview}"
    )

    try:
        response = model_client.create(
            messages=[
                {
                    "role": "system",
                    "content": [{"type": "text", "text": _REFLECTION_SYSTEM_PROMPT}],
                },
                {
                    "role": "user",
                    "content": [{"type": "text", "text": user_content}],
                },
            ],
            tools=[],
            max_tokens=_MAX_REPLY_TOKENS,
        )
    except Exception as exc:
        logger.debug("Builder reflection model call failed: %s", exc)
        return ReflectionResult(should_propose=False)

    content = response.get("content") or []
    raw = "".join(
        block.get("text", "")
        for block in content
        if isinstance(block, dict) and block.get("type") == "text"
    ).strip()

    return _parse_reflection_response(raw, user_message=user_message)


def reflect_on_pattern(
    *,
    model_client,
    pattern: PatternSignal,
) -> ReflectionResult:
    """Ask the LLM to generate a skill from a detected repeated pattern.

    Called when the Pattern Detector finds a sequence used ≥N times.
    """
    tool_list = " → ".join(pattern.tool_sequence)
    examples = "\n".join(f"- {msg}" for msg in pattern.example_user_messages)
    confidence_pct = int(pattern.confidence * 100)

    user_content = (
        f"Repeated tool sequence detected ({pattern.occurrence_count}x): {tool_list}\n\n"
        f"Example user messages that triggered this pattern:\n{examples}\n\n"
        f"Pattern confidence: {confidence_pct}%\n\n"
        f"Generate a reusable skill for this workflow."
    )

    try:
        response = model_client.create(
            messages=[
                {
                    "role": "system",
                    "content": [{"type": "text", "text": _REFLECTION_SYSTEM_PROMPT}],
                },
                {
                    "role": "user",
                    "content": [{"type": "text", "text": user_content}],
                },
            ],
            tools=[],
            max_tokens=_MAX_REPLY_TOKENS,
        )
    except Exception as exc:
        logger.debug("Builder pattern reflection failed: %s", exc)
        return ReflectionResult(should_propose=False)

    content = response.get("content") or []
    raw = "".join(
        block.get("text", "")
        for block in content
        if isinstance(block, dict) and block.get("type") == "text"
    ).strip()

    # For patterns, we synthesize the user_message from example messages
    example_msg = pattern.example_user_messages[0] if pattern.example_user_messages else str(pattern.tool_sequence)
    return _parse_reflection_response(raw, user_message=example_msg)


def _parse_reflection_response(raw: str, *, user_message: str) -> ReflectionResult:
    try:
        # Strip markdown code fences if the model wrapped it
        clean = raw.strip()
        if clean.startswith("```"):
            lines = clean.splitlines()
            clean = "\n".join(
                line for line in lines
                if not line.strip().startswith("```")
            ).strip()

        parsed = json.loads(clean)
    except (json.JSONDecodeError, ValueError) as exc:
        logger.debug("Builder reflection response is not valid JSON: %s — raw=%r", exc, raw[:200])
        return ReflectionResult(should_propose=False, raw_json=raw)

    if not parsed.get("should_propose"):
        return ReflectionResult(should_propose=False, raw_json=raw)

    title = str(parsed.get("title") or "").strip()
    trigger = str(parsed.get("trigger") or "").strip()
    steps_raw = parsed.get("steps") or []
    tags_raw = parsed.get("tags") or []
    confidence_raw = parsed.get("confidence", 0.7)

    if not title or not trigger or not steps_raw:
        logger.debug("Builder reflection missing required fields: title=%r trigger=%r steps=%r", title, trigger, steps_raw)
        return ReflectionResult(should_propose=False, raw_json=raw)

    steps = tuple(str(s).strip() for s in steps_raw if str(s).strip())
    if not steps:
        return ReflectionResult(should_propose=False, raw_json=raw)

    tags = tuple(str(t).strip().lower() for t in tags_raw if str(t).strip())
    confidence = float(max(0.0, min(1.0, confidence_raw)))

    proposal = BuilderProposal(
        decision_type=BuilderDecisionType.SKILL_PROPOSAL,
        title=title,
        summary=f"Captured from: {user_message[:100]}",
        confidence=confidence,
        approval_mode="skill",
        suggested_skill_title=title,
        suggested_trigger=trigger,
        suggested_steps=steps,
        suggested_tags=tags,
    )
    return ReflectionResult(should_propose=True, proposal=proposal, raw_json=raw)


__all__ = [
    "ReflectionResult",
    "reflect_on_pattern",
    "reflect_on_turn",
]
