"""Tiny intent router for Nullion chat entrypoints."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
import re
from typing import Callable, TypedDict

from langgraph.graph import END, START, StateGraph

from .conversation_runtime import ConversationTurnDisposition


class IntentLabel(str, Enum):
    CHITCHAT = "chitchat"
    ACTIONABLE = "actionable"
    AMBIGUOUS = "ambiguous"


@dataclass(slots=True)
class IntentClassification:
    label: IntentLabel
    intent_key: str
    confidence: float


@dataclass(slots=True)
class ConversationTurnDispositionDecision:
    disposition: ConversationTurnDisposition
    reason: str


@dataclass(frozen=True, slots=True)
class IntentSignals:
    action_terms: tuple[str, ...] = ()
    information_targets: tuple[str, ...] = ()
    communication_targets: tuple[str, ...] = ()
    artifact_targets: tuple[str, ...] = ()
    has_url: bool = False

    @property
    def actionable(self) -> bool:
        return bool(
            self.action_terms
            or self.information_targets
            or self.has_url
            or ("send" in self.action_terms and (self.communication_targets or self.artifact_targets))
        )


TurnDispositionAmbiguityFallback = Callable[[str, bool], ConversationTurnDisposition | None]
TurnDispositionAmbiguityClassifier = Callable[[str, "TurnDispositionAmbiguityContext"], ConversationTurnDisposition | None]


@dataclass(frozen=True, slots=True)
class TurnDispositionAmbiguityContext:
    active_branch_exists: bool
    previous_assistant_message: str | None = None


class _IntentClassificationState(TypedDict, total=False):
    text: str
    normalized: str
    signals: IntentSignals
    classification: IntentClassification | None


class _TurnDispositionState(TypedDict, total=False):
    text: str
    normalized: str
    active_branch_exists: bool
    previous_assistant_message: str | None
    ambiguity_fallback: TurnDispositionAmbiguityFallback | None
    ambiguity_classifier: TurnDispositionAmbiguityClassifier | None
    decision: ConversationTurnDispositionDecision | None


_SOCIAL_PHRASES = {
    "hi",
    "hello",
    "hey",
    "yo",
    "hiya",
    "hi there",
    "hello there",
    "hey there",
    "hey man",
    "hey nullion",
    "howdy",
    "sup",
    "whats up",
    "wassup",
    "wats up",
    "whsts up baby",
    "hows it going",
}
_GRATITUDE_PHRASES = {"thanks", "thank you", "thx", "ty", "thank you so much"}
_ACK_PHRASES = {"ok", "okay", "kk", "sounds good", "got it"}
_MORNING_PHRASES = {"gm", "good morning"}
_FAREWELL_PHRASES = {"bye", "goodbye", "good night", "goodnight", "gn", "night", "cya", "see ya", "ttyl"}
_ACTION_TERMS = {
    "get",
    "give",
    "show",
    "pull",
    "fetch",
    "read",
    "check",
    "summarize",
    "analyze",
    "find",
    "search",
    "open",
    "deploy",
    "delete",
    "buy",
    "sell",
}
_SEND_TERMS = {"send", "email", "message"}
_INFORMATION_TARGET_TERMS = {
    "news",
    "headline",
    "weather",
    "stock",
    "market",
}
_COMMUNICATION_TARGET_TERMS = {
    "email",
    "mail",
    "message",
    "dm",
    "slack",
    "discord",
    "telegram",
}
_ARTIFACT_TARGET_TERMS = {
    "file",
    "pdf",
    "doc",
    "docx",
    "spreadsheet",
    "xlsx",
    "csv",
    "attachment",
    "artifact",
}
_URL_TERMS = ("http://", "https://", "www.")
_SOCIAL_OPENERS = {"hey", "hi", "hello", "yo", "howdy"}
_CONTINUE_ANYWHERE_PHRASES = ("that one", "around here", "near me")
_CONTINUE_ANYWHERE_PATTERN = re.compile(
    r"\b(?:more|another|same|meant|too|as well|only|continue)\b",
    re.IGNORECASE,
)
_INTERRUPT_OR_REVISE_PREFIXES = (
    ("wait",),
    ("actually",),
    ("instead",),
    ("no",),
    ("change", "that"),
)
_CONTINUE_PREFIXES = (
    ("also",),
    ("and",),
    ("plus",),
    ("same",),
    ("what", "about"),
    ("how", "about"),
)
_REFERENTIAL_AUXILIARY_VERBS = {
    "is",
    "isnt",
    "was",
    "were",
    "does",
    "do",
    "did",
    "can",
    "could",
    "would",
    "should",
    "will",
    "wont",
    "has",
    "have",
    "had",
    "are",
    "am",
}
_REFERENTIAL_SUBJECTS = {"that", "this", "it", "those", "these"}
_REFERENTIAL_QUESTION_WORDS = {"why", "how", "when", "where"}


def _normalize(text: str) -> str:
    lowered = text.strip().lower()
    lowered = re.sub(r"[!?.',]+", "", lowered)
    lowered = re.sub(r"\s+", " ", lowered)
    return lowered.strip()


def _words(normalized_text: str) -> list[str]:
    return normalized_text.split()


def _starts_with_phrase(words: list[str], phrase: tuple[str, ...]) -> bool:
    if len(words) < len(phrase):
        return False
    return tuple(words[: len(phrase)]) == phrase


def _starts_with_any_phrase(words: list[str], phrases: tuple[tuple[str, ...], ...]) -> bool:
    return any(_starts_with_phrase(words, phrase) for phrase in phrases)


def _intent_signals(normalized_text: str) -> IntentSignals:
    words = tuple(_words(normalized_text))
    word_set = set(words)
    action_terms = tuple(word for word in words if word in _ACTION_TERMS)
    send_terms = tuple(word for word in words if word in _SEND_TERMS)
    information_targets = tuple(word for word in words if word in _INFORMATION_TARGET_TERMS)
    communication_targets = tuple(word for word in words if word in _COMMUNICATION_TARGET_TERMS)
    artifact_targets = tuple(word for word in words if word in _ARTIFACT_TARGET_TERMS)
    if send_terms and (communication_targets or artifact_targets):
        action_terms = (*action_terms, *send_terms)
    return IntentSignals(
        action_terms=tuple(dict.fromkeys(action_terms)),
        information_targets=tuple(dict.fromkeys(information_targets)),
        communication_targets=tuple(dict.fromkeys(communication_targets)),
        artifact_targets=tuple(dict.fromkeys(artifact_targets)),
        has_url=any(term in normalized_text for term in _URL_TERMS) or any("." in word for word in word_set),
    )


def _has_social_open_prefix(normalized_text: str) -> bool:
    words = _words(normalized_text)
    return bool(words) and words[0] in _SOCIAL_OPENERS


def _is_referential_follow_up(normalized_text: str) -> bool:
    words = _words(normalized_text)
    if len(words) < 2:
        return False

    first = words[0]
    if first.startswith("c") and first[1:] in _REFERENTIAL_QUESTION_WORDS:
        first = first[1:]
    if first in _REFERENTIAL_AUXILIARY_VERBS:
        return words[1] in _REFERENTIAL_SUBJECTS

    if first not in _REFERENTIAL_QUESTION_WORDS:
        return False

    reference_index = 1
    if words[1] in _REFERENTIAL_AUXILIARY_VERBS:
        reference_index = 2

    return reference_index < len(words) and any(word in _REFERENTIAL_SUBJECTS for word in words[reference_index:])


def _intent_normalize_node(state: _IntentClassificationState) -> dict[str, object]:
    return {"normalized": _normalize(state.get("text") or "")}


def _intent_empty_node(state: _IntentClassificationState) -> dict[str, object]:
    normalized = state.get("normalized") or ""
    if not normalized:
        return {"classification": IntentClassification(label=IntentLabel.AMBIGUOUS, intent_key="empty", confidence=0.0)}
    return {}


def _intent_signal_node(state: _IntentClassificationState) -> dict[str, object]:
    if state.get("classification") is not None:
        return {}
    return {"signals": _intent_signals(state.get("normalized") or "")}


def _intent_social_node(state: _IntentClassificationState) -> dict[str, object]:
    if state.get("classification") is not None:
        return {}
    normalized = state.get("normalized") or ""
    if normalized in _GRATITUDE_PHRASES:
        return {"classification": IntentClassification(label=IntentLabel.CHITCHAT, intent_key="gratitude", confidence=0.99)}
    if normalized in _ACK_PHRASES:
        return {"classification": IntentClassification(label=IntentLabel.CHITCHAT, intent_key="acknowledgment", confidence=0.99)}
    if normalized in _MORNING_PHRASES:
        return {"classification": IntentClassification(label=IntentLabel.CHITCHAT, intent_key="morning", confidence=0.99)}
    if normalized in _FAREWELL_PHRASES:
        return {"classification": IntentClassification(label=IntentLabel.CHITCHAT, intent_key="farewell", confidence=0.99)}
    if normalized in _SOCIAL_PHRASES or re.fullmatch(r"(hi|hello|hey|yo|hiya)(?: \d+)?", normalized):
        return {"classification": IntentClassification(label=IntentLabel.CHITCHAT, intent_key="greeting", confidence=0.98)}
    return {}


def _intent_actionable_node(state: _IntentClassificationState) -> dict[str, object]:
    if state.get("classification") is not None:
        return {}
    normalized = state.get("normalized") or ""
    signals = state.get("signals") or _intent_signals(normalized)
    has_action = signals.actionable
    has_social_open = _has_social_open_prefix(normalized)

    if has_social_open and has_action:
        return {"classification": IntentClassification(label=IntentLabel.AMBIGUOUS, intent_key="mixed", confidence=0.6)}
    if has_action:
        return {"classification": IntentClassification(label=IntentLabel.ACTIONABLE, intent_key="request", confidence=0.9)}
    return {}


def _intent_default_node(state: _IntentClassificationState) -> dict[str, object]:
    if state.get("classification") is not None:
        return {}
    return {"classification": IntentClassification(label=IntentLabel.AMBIGUOUS, intent_key="unknown", confidence=0.4)}


@lru_cache(maxsize=1)
def _compiled_intent_classification_graph():
    graph = StateGraph(_IntentClassificationState)
    graph.add_node("normalize", _intent_normalize_node)
    graph.add_node("empty", _intent_empty_node)
    graph.add_node("signals", _intent_signal_node)
    graph.add_node("social", _intent_social_node)
    graph.add_node("actionable", _intent_actionable_node)
    graph.add_node("default", _intent_default_node)
    graph.add_edge(START, "normalize")
    graph.add_edge("normalize", "empty")
    graph.add_edge("empty", "signals")
    graph.add_edge("signals", "social")
    graph.add_edge("social", "actionable")
    graph.add_edge("actionable", "default")
    graph.add_edge("default", END)
    return graph.compile()


def classify_intent(text: str) -> IntentClassification:
    final_state = _compiled_intent_classification_graph().invoke(
        {"text": text, "classification": None},
        config={"configurable": {"thread_id": "intent-classification"}},
    )
    classification = final_state.get("classification")
    if isinstance(classification, IntentClassification):
        return classification
    return IntentClassification(label=IntentLabel.AMBIGUOUS, intent_key="unknown", confidence=0.0)


def _matches_interrupt_or_revise_prefix(normalized_text: str) -> bool:
    return _starts_with_any_phrase(_words(normalized_text), _INTERRUPT_OR_REVISE_PREFIXES)


def _matches_continue_prefix(normalized_text: str) -> bool:
    return _starts_with_any_phrase(_words(normalized_text), _CONTINUE_PREFIXES)


def _looks_like_additive_independent_request(normalized_text: str) -> bool:
    words = _words(normalized_text)
    if len(words) < 2 or words[0] not in {"and", "also", "plus"}:
        return False
    if any(word in _REFERENTIAL_SUBJECTS for word in words[1:]):
        return False
    remainder = " ".join(words[1:])
    signals = _intent_signals(remainder)
    return bool(signals.actionable and (signals.information_targets or signals.communication_targets or signals.has_url))


def _is_short_social_message(normalized_text: str) -> bool:
    token_count = len(normalized_text.split())
    if token_count > 4:
        return False

    classification = classify_intent(normalized_text)
    return classification.label is IntentLabel.CHITCHAT


def _looks_like_short_follow_up_answer(normalized_text: str) -> bool:
    if not normalized_text:
        return False
    if len(normalized_text) > 80:
        return False
    words = normalized_text.split()
    return 1 <= len(words) <= 8


def _continues_previous_assistant_question(
    normalized_text: str,
    *,
    active_branch_exists: bool,
    previous_assistant_message: str | None,
) -> bool:
    if not active_branch_exists:
        return False
    if not isinstance(previous_assistant_message, str) or not previous_assistant_message.strip():
        return False
    if not previous_assistant_message.rstrip().endswith("?"):
        return False
    return _looks_like_short_follow_up_answer(normalized_text)


def _continues_previous_assistant_statement_by_reference(
    normalized_text: str,
    *,
    active_branch_exists: bool,
    previous_assistant_message: str | None,
) -> bool:
    if not active_branch_exists:
        return False
    if not isinstance(previous_assistant_message, str) or not previous_assistant_message.strip():
        return False
    if previous_assistant_message.rstrip().endswith("?"):
        return False
    if not _looks_like_short_follow_up_answer(normalized_text):
        return False
    return _is_referential_follow_up(normalized_text)


def classify_turn_disposition(
    text: str,
    active_branch_exists: bool,
    ambiguity_fallback: TurnDispositionAmbiguityFallback | None = None,
    previous_assistant_message: str | None = None,
    ambiguity_classifier: TurnDispositionAmbiguityClassifier | None = None,
) -> ConversationTurnDisposition:
    return classify_turn_disposition_with_reason(
        text=text,
        active_branch_exists=active_branch_exists,
        ambiguity_fallback=ambiguity_fallback,
        previous_assistant_message=previous_assistant_message,
        ambiguity_classifier=ambiguity_classifier,
    ).disposition


def _turn_disposition_normalize_node(state: _TurnDispositionState) -> dict[str, object]:
    return {"normalized": _normalize(state.get("text") or "")}


def _turn_disposition_marker_node(state: _TurnDispositionState) -> dict[str, object]:
    normalized = state.get("normalized") or ""
    if not normalized:
        return {"decision": ConversationTurnDispositionDecision(
            disposition=ConversationTurnDisposition.CHATTER,
            reason="empty_message",
        )}

    active_branch_exists = bool(state.get("active_branch_exists"))
    if active_branch_exists and _matches_interrupt_or_revise_prefix(normalized):
        return {"decision": ConversationTurnDispositionDecision(
            disposition=ConversationTurnDisposition.INTERRUPT,
            reason="interrupt_marker",
        )}

    if active_branch_exists and _looks_like_additive_independent_request(normalized):
        return {"decision": ConversationTurnDispositionDecision(
            disposition=ConversationTurnDisposition.INDEPENDENT,
            reason="additive_independent_request",
        )}

    if active_branch_exists and (
        _matches_continue_prefix(normalized)
        or any(phrase in normalized for phrase in _CONTINUE_ANYWHERE_PHRASES)
        or _CONTINUE_ANYWHERE_PATTERN.search(normalized) is not None
    ):
        return {"decision": ConversationTurnDispositionDecision(
            disposition=ConversationTurnDisposition.CONTINUE,
            reason="continue_marker",
        )}
    return {}


def _turn_disposition_social_node(state: _TurnDispositionState) -> dict[str, object]:
    if state.get("decision") is not None:
        return {}
    normalized = state.get("normalized") or ""
    if _is_short_social_message(normalized):
        return {"decision": ConversationTurnDispositionDecision(
            disposition=ConversationTurnDisposition.CHATTER,
            reason="social_message",
        )}
    return {}


def _turn_disposition_followup_node(state: _TurnDispositionState) -> dict[str, object]:
    if state.get("decision") is not None:
        return {}
    normalized = state.get("normalized") or ""
    active_branch_exists = bool(state.get("active_branch_exists"))
    previous_assistant_message = state.get("previous_assistant_message")
    if _continues_previous_assistant_question(
        normalized,
        active_branch_exists=active_branch_exists,
        previous_assistant_message=previous_assistant_message,
    ):
        return {"decision": ConversationTurnDispositionDecision(
            disposition=ConversationTurnDisposition.CONTINUE,
            reason="question_follow_up",
        )}

    if _continues_previous_assistant_statement_by_reference(
        normalized,
        active_branch_exists=active_branch_exists,
        previous_assistant_message=previous_assistant_message,
    ):
        return {"decision": ConversationTurnDispositionDecision(
            disposition=ConversationTurnDisposition.CONTINUE,
            reason="referential_follow_up",
        )}

    if active_branch_exists and _is_referential_follow_up(normalized):
        return {"decision": ConversationTurnDispositionDecision(
            disposition=ConversationTurnDisposition.CONTINUE,
            reason="referential_follow_up",
        )}
    return {}


def _turn_disposition_fallback_node(state: _TurnDispositionState) -> dict[str, object]:
    if state.get("decision") is not None:
        return {}
    text = state.get("text") or ""
    active_branch_exists = bool(state.get("active_branch_exists"))
    ambiguity_fallback = state.get("ambiguity_fallback")
    if ambiguity_fallback is not None:
        try:
            fallback_disposition = ambiguity_fallback(text, active_branch_exists)
        except Exception:
            return {"decision": ConversationTurnDispositionDecision(
                disposition=ConversationTurnDisposition.INDEPENDENT,
                reason="ambiguity_fallback_error",
            )}
        if fallback_disposition is not None:
            if isinstance(fallback_disposition, ConversationTurnDisposition):
                return {"decision": ConversationTurnDispositionDecision(
                    disposition=fallback_disposition,
                    reason="ambiguity_fallback",
                )}
            return {"decision": ConversationTurnDispositionDecision(
                disposition=ConversationTurnDisposition.INDEPENDENT,
                reason="ambiguity_fallback_invalid_return",
            )}
    return {}


def _turn_disposition_classifier_node(state: _TurnDispositionState) -> dict[str, object]:
    if state.get("decision") is not None:
        return {}
    text = state.get("text") or ""
    active_branch_exists = bool(state.get("active_branch_exists"))
    previous_assistant_message = state.get("previous_assistant_message")
    ambiguity_classifier = state.get("ambiguity_classifier")
    if ambiguity_classifier is not None:
        try:
            classifier_disposition = ambiguity_classifier(
                text,
                TurnDispositionAmbiguityContext(
                    active_branch_exists=active_branch_exists,
                    previous_assistant_message=previous_assistant_message,
                ),
            )
        except Exception:
            return {"decision": ConversationTurnDispositionDecision(
                disposition=ConversationTurnDisposition.INDEPENDENT,
                reason="ambiguity_classifier_error",
            )}
        if classifier_disposition is not None:
            if isinstance(classifier_disposition, ConversationTurnDisposition):
                return {"decision": ConversationTurnDispositionDecision(
                    disposition=classifier_disposition,
                    reason="ambiguity_classifier",
                )}
            return {"decision": ConversationTurnDispositionDecision(
                disposition=ConversationTurnDisposition.INDEPENDENT,
                reason="ambiguity_classifier_invalid_return",
            )}
    return {}


def _turn_disposition_default_node(state: _TurnDispositionState) -> dict[str, object]:
    if state.get("decision") is not None:
        return {}
    return {"decision": ConversationTurnDispositionDecision(
        disposition=ConversationTurnDisposition.INDEPENDENT,
        reason="default_independent",
    )}


@lru_cache(maxsize=1)
def _compiled_turn_disposition_graph():
    graph = StateGraph(_TurnDispositionState)
    graph.add_node("normalize", _turn_disposition_normalize_node)
    graph.add_node("markers", _turn_disposition_marker_node)
    graph.add_node("social", _turn_disposition_social_node)
    graph.add_node("followup", _turn_disposition_followup_node)
    graph.add_node("fallback", _turn_disposition_fallback_node)
    graph.add_node("classifier", _turn_disposition_classifier_node)
    graph.add_node("default", _turn_disposition_default_node)
    graph.add_edge(START, "normalize")
    graph.add_edge("normalize", "markers")
    graph.add_edge("markers", "social")
    graph.add_edge("social", "followup")
    graph.add_edge("followup", "fallback")
    graph.add_edge("fallback", "classifier")
    graph.add_edge("classifier", "default")
    graph.add_edge("default", END)
    return graph.compile()


def classify_turn_disposition_with_reason(
    text: str,
    active_branch_exists: bool,
    ambiguity_fallback: TurnDispositionAmbiguityFallback | None = None,
    previous_assistant_message: str | None = None,
    ambiguity_classifier: TurnDispositionAmbiguityClassifier | None = None,
) -> ConversationTurnDispositionDecision:
    final_state = _compiled_turn_disposition_graph().invoke(
        {
            "text": text,
            "active_branch_exists": active_branch_exists,
            "previous_assistant_message": previous_assistant_message,
            "ambiguity_fallback": ambiguity_fallback,
            "ambiguity_classifier": ambiguity_classifier,
            "decision": None,
        },
        config={"configurable": {"thread_id": "turn-disposition"}},
    )
    decision = final_state.get("decision")
    if isinstance(decision, ConversationTurnDispositionDecision):
        return decision
    return ConversationTurnDispositionDecision(
        disposition=ConversationTurnDisposition.INDEPENDENT,
        reason="default_independent",
    )


def split_compound_intent(text: str) -> list[str]:
    normalized_original = text.strip()
    if not normalized_original:
        return [text]

    parts = re.split(r"\band then\b", normalized_original, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) != 2:
        return [text]

    left, right = (part.strip(" ,") for part in parts)
    if not left or not right:
        return [text]

    left_classification = classify_intent(left)
    right_classification = classify_intent(right)
    if left_classification.label is not IntentLabel.ACTIONABLE:
        return [text]
    if right_classification.label is not IntentLabel.ACTIONABLE:
        return [text]

    return [left, right]


__all__ = [
    "ConversationTurnDisposition",
    "ConversationTurnDispositionDecision",
    "TurnDispositionAmbiguityContext",
    "IntentClassification",
    "IntentLabel",
    "classify_intent",
    "classify_turn_disposition",
    "classify_turn_disposition_with_reason",
    "split_compound_intent",
]
