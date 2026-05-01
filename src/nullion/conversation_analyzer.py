"""Conversation analyser — mines chat history to propose automatable skills.

The analyser reads recent messages from the ChatStore, sends them to the LLM
with a structured detection prompt, and returns a ranked list of
``ConversationAnalysis`` proposals that can be turned into ``SkillRecord``
entries via ``runtime.create_skill()``.

Typical call-sites
------------------
* ``/auto-skill`` slash command — on-demand from any chat interface.
* Background worker in ``web_app.py`` — fires silently after every
  ``AUTO_SKILL_INTERVAL`` new messages in a conversation.

Design rules
------------
- Fails silently: all exceptions are caught and logged; callers always get a
  (possibly empty) list back.
- Never writes skills itself — it only proposes.  The caller decides whether
  to show an approval bubble or auto-accept.
- Deduplicates against existing skill titles (case-insensitive) so it won't
  re-propose patterns already captured.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from nullion.chat_store import ChatStore

logger = logging.getLogger(__name__)

# Minimum messages needed to attempt analysis (avoids noise from short convs)
MIN_MESSAGES = 6
# Max messages to look back (keeps the prompt a reasonable size)
LOOKBACK_MESSAGES = 60
# Auto-trigger threshold: run silently after this many new messages
AUTO_SKILL_INTERVAL = 20

_SYSTEM_PROMPT = """\
You are a workflow-detection assistant for an AI operating system called Nullion.

Your job is to read a conversation transcript between a human operator ("user")
and the AI agent ("bot") and identify *automatable workflows* — repeating or
multi-step patterns that could be captured as a named, reusable skill.

A skill is a named procedure with:
  - A short title (≤ 8 words)
  - A one-sentence summary of what it does
  - A trigger phrase the user would type to invoke it (≤ 20 words)
  - An ordered list of 2–8 concrete steps (imperative sentences, ≤ 20 words each)
  - Up to 4 tags (single lowercase words or short hyphenated phrases)

Detection heuristics:
  1. REPEATED REQUEST — the user asked for the same type of action more than once.
  2. MULTI-STEP DELEGATION — the user described a sequence of actions for the agent
     to perform, the agent executed them successfully, and the user approved.
  3. EXPLICIT INTENT — the user said things like "every time", "automatically",
     "remember to", "always do this", or "make this a workflow".
  4. PATTERN COMPLETION — the agent completed a recognisable operational task
     (deploy, diagnose, backup, report, etc.) that could clearly be templated.

Rules:
  - Only propose skills with CONFIDENCE ≥ 0.7 (scale 0.0–1.0).
  - Do not propose skills for purely conversational exchanges or one-off lookups.
  - Each proposal must be genuinely distinct; do not split a single workflow into
    multiple near-identical skills.
  - Keep steps concrete and action-oriented, not vague ("Run health check via
    /diagnose", not "Check the system").
  - Return ONLY a JSON array (no prose).  If no patterns qualify, return [].

Output schema (JSON array of objects):
[
  {
    "title": "...",
    "summary": "...",
    "trigger": "...",
    "steps": ["...", "..."],
    "tags": ["...", "..."],
    "confidence": 0.85,
    "evidence": "Short sentence explaining which part of the conversation
                 triggered this proposal."
  }
]
"""


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class ConversationAnalysis:
    """A proposed skill extracted from conversation history."""
    title: str
    summary: str
    trigger: str
    steps: list[str]
    tags: list[str] = field(default_factory=list)
    confidence: float = 0.0
    evidence: str = ""

    def to_skill_kwargs(self) -> dict[str, Any]:
        """Return kwargs suitable for ``runtime.create_skill(**kwargs)``."""
        return {
            "title": self.title,
            "summary": self.summary,
            "trigger": self.trigger,
            "steps": self.steps,
            "tags": self.tags,
        }


# ── Core analysis ─────────────────────────────────────────────────────────────

def analyze_conversation(
    chat_store: "ChatStore",
    model_client: Any,
    conv_id: str,
    *,
    lookback: int = LOOKBACK_MESSAGES,
    existing_skill_titles: list[str] | None = None,
) -> list[ConversationAnalysis]:
    """Analyse a conversation and return proposed skill definitions.

    Args:
        chat_store:           The ChatStore instance to read from.
        model_client:         Any model client with a ``.create()`` method.
        conv_id:              Conversation ID to analyse.
        lookback:             Maximum number of recent messages to include.
        existing_skill_titles: Titles of already-existing skills (for dedup).

    Returns:
        List of ``ConversationAnalysis`` proposals, sorted by confidence desc.
        Returns an empty list on any failure or if nothing qualifies.
    """
    try:
        return _analyze_inner(
            chat_store, model_client, conv_id,
            lookback=lookback,
            existing_skill_titles=existing_skill_titles or [],
        )
    except Exception as exc:
        logger.warning("conversation_analyzer: analysis failed: %s", exc)
        return []


def analyze_all_recent(
    chat_store: "ChatStore",
    model_client: Any,
    *,
    max_conversations: int = 5,
    lookback: int = LOOKBACK_MESSAGES,
    existing_skill_titles: list[str] | None = None,
) -> list[ConversationAnalysis]:
    """Analyse the N most-recent conversations and merge proposals.

    Deduplicates across conversations by title (case-insensitive).
    """
    try:
        convs = chat_store.list_conversations(status="active", limit=max_conversations)
        seen_titles: set[str] = {t.lower() for t in (existing_skill_titles or [])}
        all_proposals: list[ConversationAnalysis] = []
        for conv in convs:
            proposals = analyze_conversation(
                chat_store, model_client, conv["id"],
                lookback=lookback,
                existing_skill_titles=list(seen_titles),
            )
            for p in proposals:
                key = p.title.lower()
                if key not in seen_titles:
                    seen_titles.add(key)
                    all_proposals.append(p)
        all_proposals.sort(key=lambda p: p.confidence, reverse=True)
        return all_proposals
    except Exception as exc:
        logger.warning("conversation_analyzer: multi-conv analysis failed: %s", exc)
        return []


# ── Internal helpers ──────────────────────────────────────────────────────────

def _analyze_inner(
    chat_store: "ChatStore",
    model_client: Any,
    conv_id: str,
    *,
    lookback: int,
    existing_skill_titles: list[str],
) -> list[ConversationAnalysis]:
    messages = chat_store.load_messages(conv_id, limit=lookback)
    if len(messages) < MIN_MESSAGES:
        logger.debug(
            "conversation_analyzer: skipping conv %s — only %d messages",
            conv_id, len(messages),
        )
        return []

    transcript = _build_transcript(messages)
    dedup_hint = ""
    if existing_skill_titles:
        titles_str = ", ".join(f'"{t}"' for t in existing_skill_titles[:20])
        dedup_hint = (
            f"\n\nSkills already captured (do NOT re-propose these or close variants): {titles_str}."
        )

    user_content = (
        "Analyse this conversation transcript and propose automatable skills.\n\n"
        f"<transcript>\n{transcript}\n</transcript>"
        + dedup_hint
    )

    try:
        response = model_client.create(
            messages=[{"role": "user", "content": [{"type": "text", "text": user_content}]}],
            tools=[],
            max_tokens=2048,
            system=_SYSTEM_PROMPT,
        )
    except TypeError:
        # Clients that don't accept system kwarg
        response = model_client.create(
            messages=[
                {"role": "system", "content": [{"type": "text", "text": _SYSTEM_PROMPT}]},
                {"role": "user", "content": [{"type": "text", "text": user_content}]},
            ],
            tools=[],
            max_tokens=2048,
        )

    raw_text = _extract_text(response)
    proposals = _parse_proposals(raw_text)

    # Filter by confidence and deduplicate against existing titles
    existing_lower = {t.lower() for t in existing_skill_titles}
    filtered = [
        p for p in proposals
        if p.confidence >= 0.70 and p.title.lower() not in existing_lower
    ]
    filtered.sort(key=lambda p: p.confidence, reverse=True)
    return filtered


def _sanitize_message_text(text: str) -> str:
    """Escape XML-like tags so user-controlled content cannot escape the
    <transcript>…</transcript> wrapper in the LLM prompt.

    We replace ``<`` with the HTML entity ``&lt;`` which is unambiguous to
    both the LLM and any downstream parser, and strip ASCII control characters
    that could confuse tokenizers.
    """
    # Strip control characters (keep newlines and tabs for readability)
    sanitized = "".join(
        ch for ch in text if ch >= " " or ch in ("\n", "\t")
    )
    # Neutralise XML/HTML tag delimiters so </transcript> cannot close the tag
    sanitized = sanitized.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return sanitized


def _build_transcript(messages: list[dict]) -> str:
    lines = []
    for m in messages:
        role = "USER" if m.get("role") == "user" else "BOT"
        text = (m.get("text") or "").strip()
        if text:
            # Truncate very long messages to avoid bloating the prompt
            if len(text) > 400:
                text = text[:400] + "…"
            # Sanitize to prevent prompt injection via XML tag escape
            text = _sanitize_message_text(text)
            lines.append(f"{role}: {text}")
    return "\n".join(lines)


def _extract_text(response: Any) -> str:
    """Pull text out of whatever shape the model client returns."""
    if isinstance(response, str):
        return response
    if isinstance(response, dict):
        # Anthropic-style parsed response dict
        content = response.get("content", [])
        if isinstance(content, list):
            parts = [
                block.get("text", "")
                for block in content
                if isinstance(block, dict) and block.get("type") == "text"
            ]
            return "\n".join(parts)
        if isinstance(content, str):
            return content
        # OpenAI-style
        choices = response.get("choices", [])
        if choices:
            msg = choices[0].get("message", {})
            return msg.get("content", "") or ""
    return str(response)


def _parse_proposals(raw: str) -> list[ConversationAnalysis]:
    """Extract the JSON array from the LLM response and parse it."""
    # Find the first [...] block (the LLM may add preamble/postamble)
    match = re.search(r"\[.*\]", raw, re.DOTALL)
    if not match:
        return []
    try:
        items = json.loads(match.group())
    except json.JSONDecodeError:
        logger.debug("conversation_analyzer: JSON parse failed for: %.200s", raw)
        return []

    proposals: list[ConversationAnalysis] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or "").strip()
        summary = str(item.get("summary") or "").strip()
        trigger = str(item.get("trigger") or "").strip()
        steps = [str(s).strip() for s in item.get("steps", []) if s]
        if not (title and summary and trigger and steps):
            continue
        tags = [str(t).strip().lower() for t in item.get("tags", []) if t][:4]
        try:
            confidence = float(item.get("confidence", 0.0))
        except (TypeError, ValueError):
            confidence = 0.0
        evidence = str(item.get("evidence") or "").strip()
        proposals.append(ConversationAnalysis(
            title=title,
            summary=summary,
            trigger=trigger,
            steps=steps,
            tags=tags,
            confidence=confidence,
            evidence=evidence,
        ))
    return proposals


# ── Session-level proposal cache (in-process, volatile) ──────────────────────
# Stored here so slash commands can reference proposals by index number.

_proposal_cache: dict[str, list[ConversationAnalysis]] = {}  # conv_id → proposals


def cache_proposals(conv_id: str, proposals: list[ConversationAnalysis]) -> None:
    _proposal_cache[conv_id] = proposals


def get_cached_proposals(conv_id: str) -> list[ConversationAnalysis]:
    return _proposal_cache.get(conv_id, [])


def clear_cached_proposals(conv_id: str) -> None:
    _proposal_cache.pop(conv_id, None)
