"""Provider-backed model client adapters for Nullion."""

from __future__ import annotations

import base64
from dataclasses import dataclass
import json
from typing import Any

try:  # pragma: no cover - import guard
    from openai import OpenAI
except Exception:  # pragma: no cover - import guard
    OpenAI = None

try:  # pragma: no cover - import guard
    import anthropic as _anthropic
except Exception:  # pragma: no cover - import guard
    _anthropic = None  # type: ignore[assignment]

try:  # pragma: no cover - import guard
    import httpx as _httpx
except Exception:  # pragma: no cover - import guard
    _httpx = None  # type: ignore[assignment]


def _image_block_data_url(block: dict[str, Any]) -> str | None:
    source = block.get("source")
    if not isinstance(source, dict):
        return None
    if source.get("type") != "base64":
        return None
    media_type = str(source.get("media_type") or "image/png")
    data = source.get("data")
    if not isinstance(data, str) or not data:
        return None
    return f"data:{media_type};base64,{data}"


def _extract_jwt_claim(token: str, claim: str) -> str | None:
    """Decode a JWT payload (no signature verification) and return a claim value."""
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload_b64 = parts[1]
        padding = (4 - len(payload_b64) % 4) % 4
        payload = json.loads(base64.urlsafe_b64decode(payload_b64 + "=" * padding))
        return payload.get(claim)
    except Exception:
        return None


def _extract_chatgpt_account_id(token: str) -> str | None:
    """Extract chatgpt_account_id from a Codex OAuth JWT."""
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload_b64 = parts[1]
        padding = (4 - len(payload_b64) % 4) % 4
        payload = json.loads(base64.urlsafe_b64decode(payload_b64 + "=" * padding))
        auth_info = payload.get("https://api.openai.com/auth", {})
        return auth_info.get("chatgpt_account_id")
    except Exception:
        return None


def _jwt_is_expired(token: str, *, leeway_seconds: int = 60) -> bool:
    """Check if a JWT's `exp` claim has passed (plus a small leeway).

    No signature verification — we only inspect the payload to decide if a
    refresh is worth trying before sending a doomed request.
    Returns False on any parse error so we don't refresh unnecessarily.
    """
    try:
        import time as _time
        parts = token.split(".")
        if len(parts) < 2:
            return False
        payload_b64 = parts[1]
        padding = (4 - len(payload_b64) % 4) % 4
        payload = json.loads(base64.urlsafe_b64decode(payload_b64 + "=" * padding))
        exp = payload.get("exp")
        if not isinstance(exp, (int, float)):
            return False
        return _time.time() >= (float(exp) - leeway_seconds)
    except Exception:
        return False


def _refresh_codex_access_token(refresh_token: str) -> str:
    """Exchange a Codex OAuth refresh token for a fresh access token."""
    if _httpx is None:  # pragma: no cover
        raise ModelClientConfigurationError("httpx package is not installed")
    try:
        response = _httpx.post(
            "https://auth.openai.com/oauth/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": "app_EMoamEEZ73f0CkXaXp7hrann",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15.0,
        )
    except Exception as exc:
        raise ModelClientConfigurationError(f"Could not refresh Codex OAuth token: {exc}") from exc
    if response.status_code != 200:
        detail = response.text[:500] if hasattr(response, "text") else ""
        raise ModelClientConfigurationError(
            f"Could not refresh Codex OAuth token: HTTP {response.status_code}. {detail}".strip()
        )
    payload = response.json()
    access_token = payload.get("access_token")
    if not isinstance(access_token, str) or not access_token.strip():
        raise ModelClientConfigurationError("Codex OAuth refresh did not return an access token.")
    return access_token.strip()


class ModelClientConfigurationError(RuntimeError):
    """Raised when model client configuration cannot be resolved."""


def _coerce_tool_input(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {"arguments": value}


def _normalize_reasoning_effort(value: str | None) -> str | None:
    effort = (value or "").strip().lower()
    return effort if effort in {"low", "medium", "high"} else None


def _chat_reasoning_kwargs(provider: str | None, model: str, effort: str | None) -> dict[str, Any]:
    effort = _normalize_reasoning_effort(effort)
    if effort is None:
        return {}
    provider_key = (provider or "").strip().lower()
    model_key = model.strip().lower()

    if provider_key in {"openrouter", "openrouter-key"}:
        return {"extra_body": {"reasoning": {"effort": effort}}}
    if provider_key in {"openai", "gemini", "groq", "together"}:
        return {"reasoning_effort": effort}
    if provider_key == "mistral" and model_key == "mistral-small-latest":
        return {"reasoning_effort": effort}
    return {}


def _anthropic_thinking_kwargs(effort: str | None, max_tokens: int) -> dict[str, Any]:
    effort = _normalize_reasoning_effort(effort)
    if effort is None:
        return {}
    budget = {"low": 1024, "medium": 2048, "high": 3072}[effort]
    if max_tokens <= budget:
        budget = max(1024, max_tokens - 512)
    if budget < 1024 or max_tokens <= budget:
        return {}
    return {"thinking": {"type": "enabled", "budget_tokens": budget}}


def parse_tool_arguments(arguments: Any) -> dict[str, Any]:
    if isinstance(arguments, dict):
        return dict(arguments)
    if not isinstance(arguments, str):
        return {"arguments": arguments}
    try:
        parsed = json.loads(arguments or "{}")
    except Exception:
        return {"arguments": arguments}
    return _coerce_tool_input(parsed)


_parse_tool_arguments = parse_tool_arguments


@dataclass(slots=True)
class OpenAIChatCompletionsModelClient:
    """OpenAI chat-completions adapter that speaks the internal orchestrator protocol."""

    client: Any
    model: str
    provider: str | None = None
    reasoning_effort: str | None = None

    @staticmethod
    def _serialize_text_content(content: Any) -> str:
        if content is None:
            return ""
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for block in content:
                if not isinstance(block, dict):
                    continue
                block_type = block.get("type")
                if block_type == "text":
                    text = block.get("text")
                    if isinstance(text, str):
                        parts.append(text)
                elif block_type == "tool_result":
                    result_content = block.get("content")
                    if isinstance(result_content, list):
                        parts.append(OpenAIChatCompletionsModelClient._serialize_text_content(result_content))
                    elif isinstance(result_content, str):
                        parts.append(result_content)
                    else:
                        parts.append(json.dumps(result_content, ensure_ascii=False, default=str))
            return "".join(parts)
        return str(content)

    @staticmethod
    def _serialize_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        system_messages: list[dict[str, Any]] = []
        serialized: list[dict[str, Any]] = []
        for message in messages:
            role = message.get("role")
            content = message.get("content")
            if role == "system":
                system_messages.append({"role": "system", "content": OpenAIChatCompletionsModelClient._serialize_text_content(content)})
                continue
            if role == "user":
                if isinstance(content, list):
                    content_parts: list[dict[str, Any]] = []
                    for block in content:
                        if not isinstance(block, dict):
                            continue
                        if block.get("type") == "text":
                            text = block.get("text")
                            if isinstance(text, str):
                                content_parts.append({"type": "text", "text": text})
                        elif block.get("type") == "image":
                            data_url = _image_block_data_url(block)
                            if data_url:
                                content_parts.append({"type": "image_url", "image_url": {"url": data_url}})
                        elif block.get("type") == "tool_result":
                            tool_call_id = block.get("tool_use_id")
                            if not isinstance(tool_call_id, str) or not tool_call_id:
                                continue
                            result_content = block.get("content")
                            serialized.append(
                                {
                                    "role": "tool",
                                    "tool_call_id": tool_call_id,
                                    "content": OpenAIChatCompletionsModelClient._serialize_text_content(result_content),
                                }
                            )
                    if content_parts:
                        serialized.append({"role": "user", "content": content_parts})
                else:
                    serialized.append({"role": "user", "content": OpenAIChatCompletionsModelClient._serialize_text_content(content)})
                continue
            if role == "assistant":
                tool_calls: list[dict[str, Any]] = []
                text_parts: list[str] = []
                if isinstance(content, list):
                    for block in content:
                        if not isinstance(block, dict):
                            continue
                        block_type = block.get("type")
                        if block_type == "text":
                            text = block.get("text")
                            if isinstance(text, str):
                                text_parts.append(text)
                        elif block_type == "tool_use":
                            tool_name = block.get("name")
                            tool_use_id = block.get("id")
                            tool_input = block.get("input")
                            if not isinstance(tool_name, str) or not isinstance(tool_use_id, str):
                                continue
                            tool_calls.append(
                                {
                                    "id": tool_use_id,
                                    "type": "function",
                                    "function": {
                                        "name": tool_name,
                                        "arguments": json.dumps(tool_input or {}, ensure_ascii=False, default=str),
                                    },
                                }
                            )
                else:
                    text_content = OpenAIChatCompletionsModelClient._serialize_text_content(content)
                    if text_content:
                        text_parts.append(text_content)
                assistant_message: dict[str, Any] = {"role": "assistant"}
                if text_parts:
                    assistant_message["content"] = "".join(text_parts)
                elif not tool_calls:
                    assistant_message["content"] = ""
                if tool_calls:
                    assistant_message["tool_calls"] = tool_calls
                serialized.append(assistant_message)
                continue
            if role == "tool":
                serialized.append(
                    {
                        "role": "tool",
                        "tool_call_id": message.get("tool_call_id"),
                        "content": OpenAIChatCompletionsModelClient._serialize_text_content(content),
                    }
                )
                continue
            serialized.append({"role": role, "content": OpenAIChatCompletionsModelClient._serialize_text_content(content)})
        return [*system_messages, *serialized]

    @staticmethod
    def _serialize_tool_definitions(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
        serialized_tools: list[dict[str, Any]] = []
        for tool in tools:
            if not isinstance(tool, dict):
                continue
            name = tool.get("name")
            if not isinstance(name, str) or not name:
                continue
            description = tool.get("description")
            input_schema = tool.get("input_schema")
            if not isinstance(input_schema, dict):
                input_schema = {"type": "object", "properties": {}, "additionalProperties": True}
            serialized_tools.append(
                {
                    "type": "function",
                    "function": {
                        "name": name,
                        "description": description if isinstance(description, str) else "",
                        "parameters": input_schema,
                    },
                }
            )
        return serialized_tools

    @staticmethod
    def _content_blocks_from_choice(choice: Any) -> tuple[str, list[dict[str, Any]]]:
        message = getattr(choice, "message", None)
        finish_reason = getattr(choice, "finish_reason", None)
        tool_calls = getattr(message, "tool_calls", None) or []
        thinking_text = (
            getattr(message, "reasoning_content", None)
            or getattr(message, "reasoning", None)
            or getattr(message, "thinking", None)
        )
        thinking_blocks = (
            [{"type": "thinking", "text": str(thinking_text)}]
            if isinstance(thinking_text, str) and thinking_text.strip()
            else []
        )
        if tool_calls:
            blocks: list[dict[str, Any]] = [*thinking_blocks]
            for tool_call in tool_calls:
                function = getattr(tool_call, "function", None)
                if function is None:
                    continue
                name = getattr(function, "name", None)
                call_id = getattr(tool_call, "id", None)
                arguments = getattr(function, "arguments", "{}")
                if not isinstance(name, str) or not isinstance(call_id, str):
                    continue
                parsed_input = _parse_tool_arguments(arguments)
                blocks.append({"type": "tool_use", "id": call_id, "name": name, "input": parsed_input})
            if blocks:
                return "tool_use", blocks
        text = getattr(message, "content", None)
        text_value = "" if text is None else str(text)
        text_blocks = [{"type": "text", "text": text_value}] if text_value else []
        return "end_turn", [*thinking_blocks, *text_blocks]

    def create(self, *, messages, tools):
        # Only attach `tools` / `tool_choice` when we actually have tools.
        # OpenRouter's provider routing treats `tool_choice="auto"` as a
        # required-feature signal — sending it with an empty tool list makes
        # it filter out providers that don't support function calling, which
        # surfaces as a confusing 404 "No allowed providers are available
        # for the selected model" for any model whose only providers happen
        # to lack tool support (e.g. mistralai/mistral-nemo on free tiers).
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": self._serialize_messages(list(messages)),
        }
        kwargs.update(_chat_reasoning_kwargs(self.provider, self.model, self.reasoning_effort))
        serialized_tools = self._serialize_tool_definitions(list(tools))
        if serialized_tools:
            kwargs["tools"] = serialized_tools
            kwargs["tool_choice"] = "auto"
        response = self.client.chat.completions.create(**kwargs)
        choice = response.choices[0]
        stop_reason, content = self._content_blocks_from_choice(choice)
        return {"stop_reason": stop_reason, "content": content}


@dataclass(slots=True)
class AnthropicMessagesModelClient:
    """Anthropic Messages API adapter that speaks the internal orchestrator protocol."""

    client: Any
    model: str
    reasoning_effort: str | None = None

    @staticmethod
    def _text_from_content(content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts = [
                block.get("text", "")
                for block in content
                if isinstance(block, dict) and block.get("type") == "text"
            ]
            return "\n".join(part for part in parts if part)
        return str(content or "")

    @staticmethod
    def _serialize_messages(messages: list[dict[str, Any]]) -> tuple[str | None, list[dict[str, Any]]]:
        system_parts: list[str] = []
        serialized: list[dict[str, Any]] = []
        for message in messages:
            role = message.get("role")
            content = message.get("content")
            if role == "system":
                text = AnthropicMessagesModelClient._text_from_content(content)
                if text:
                    system_parts.append(text)
                continue
            if role not in {"user", "assistant"}:
                continue
            serialized.append({"role": role, "content": content if isinstance(content, list) else AnthropicMessagesModelClient._text_from_content(content)})
        system = "\n\n".join(system_parts) if system_parts else None
        return system, serialized

    @staticmethod
    def _serialize_tool_definitions(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
        serialized_tools: list[dict[str, Any]] = []
        for tool in tools:
            if not isinstance(tool, dict):
                continue
            name = tool.get("name")
            if not isinstance(name, str) or not name:
                continue
            input_schema = tool.get("input_schema")
            if not isinstance(input_schema, dict):
                input_schema = {"type": "object", "properties": {}}
            serialized_tools.append(
                {
                    "name": name,
                    "description": tool.get("description", "") if isinstance(tool.get("description"), str) else "",
                    "input_schema": input_schema,
                }
            )
        return serialized_tools

    @staticmethod
    def _parse_response(response: Any) -> dict[str, Any]:
        content_blocks: list[dict[str, Any]] = []
        for block in getattr(response, "content", []) or []:
            block_type = getattr(block, "type", None)
            if block_type == "text":
                content_blocks.append({"type": "text", "text": getattr(block, "text", "")})
            elif block_type in {"thinking", "redacted_thinking"}:
                text = getattr(block, "thinking", None) or getattr(block, "text", None)
                if isinstance(text, str) and text.strip():
                    content_blocks.append({"type": "thinking", "text": text})
            elif block_type == "tool_use":
                content_blocks.append(
                    {
                        "type": "tool_use",
                        "id": getattr(block, "id", ""),
                        "name": getattr(block, "name", ""),
                        "input": _coerce_tool_input(getattr(block, "input", {}) or {}),
                    }
                )
        return {
            "stop_reason": "tool_use" if getattr(response, "stop_reason", None) == "tool_use" else "end_turn",
            "content": content_blocks,
        }

    def create(self, *, messages, tools, max_tokens: int = 4096, system: str | None = None):
        extracted_system, serialized_messages = self._serialize_messages(list(messages))
        system_text = system or extracted_system
        kwargs: dict[str, Any] = {
            "model": self.model,
            "messages": serialized_messages,
            "max_tokens": max_tokens,
        }
        kwargs.update(_anthropic_thinking_kwargs(self.reasoning_effort, max_tokens))
        if system_text:
            kwargs["system"] = system_text
        serialized_tools = self._serialize_tool_definitions(list(tools))
        if serialized_tools:
            kwargs["tools"] = serialized_tools
        return self._parse_response(self.client.messages.create(**kwargs))



@dataclass(slots=True)
class CodexResponsesModelClient:
    """Model client for ChatGPT Codex OAuth.

    Calls chatgpt.com/backend-api/codex/responses using a Codex OAuth JWT.
    Uses ChatGPT Plus/Pro quota instead of OpenAI API billing credits.
    """

    api_key: str          # Codex OAuth JWT
    account_id: str       # chatgpt_account_id from the JWT
    model: str            # Required — caller must pass an explicit model name.
    #                       (Removed the implicit "gpt-5.5" default so the
    #                       runtime never silently picks a vendor model.)

    _ENDPOINT: str = "https://chatgpt.com/backend-api/codex/responses"
    reasoning_effort: str | None = None

    @staticmethod
    def _to_input(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Convert orchestrator messages → Responses API input.

        The ChatGPT/Codex Responses endpoint accepts prior tool calls and tool
        outputs as top-level input items, not as nested assistant/user message
        content blocks. See the OpenAI Responses tool-calling flow where callers
        append `response.output` items and then `function_call_output` items back
        into the next `input` list.
        """
        tool_result_ids: set[str] = set()
        for msg in messages:
            content = msg.get("content")
            if msg.get("role") != "user" or not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, dict) or block.get("type") != "tool_result":
                    continue
                tool_call_id = block.get("tool_use_id")
                if isinstance(tool_call_id, str) and tool_call_id:
                    tool_result_ids.add(tool_call_id)

        out: list[dict[str, Any]] = []
        for msg in messages:
            role = msg.get("role")
            content = msg.get("content")
            if role == "user":
                if isinstance(content, list):
                    text_parts: list[str] = []
                    def flush_user_text() -> None:
                        text = "".join(text_parts)
                        if text:
                            out.append({"role": "user", "content": text})
                        text_parts.clear()

                    for block in content:
                        if not isinstance(block, dict):
                            continue
                        if block.get("type") == "text":
                            text_parts.append(str(block.get("text", "") or ""))
                        elif block.get("type") == "image":
                            flush_user_text()
                            data_url = _image_block_data_url(block)
                            if data_url:
                                out.append(
                                    {
                                        "role": "user",
                                        "content": [{"type": "input_image", "image_url": data_url}],
                                    }
                                )
                        elif block.get("type") == "tool_result":
                            flush_user_text()
                            raw = block.get("content", "")
                            text_val = raw if isinstance(raw, str) else (
                                " ".join(b.get("text", "") for b in raw if isinstance(b, dict)) if isinstance(raw, list) else str(raw)
                            )
                            call_id = block.get("tool_use_id", "")
                            if call_id:
                                out.append({
                                    "type": "function_call_output",
                                    "call_id": call_id,
                                    "output": text_val,
                                })
                    flush_user_text()
                else:
                    out.append({"role": "user", "content": str(content or "")})
            elif role == "assistant":
                if isinstance(content, list):
                    text_parts: list[str] = []
                    has_tool_use = any(isinstance(block, dict) and block.get("type") == "tool_use" for block in content)

                    def flush_assistant_text(*, before_tool_use: bool = False) -> None:
                        text = "".join(text_parts)
                        if text and (not has_tool_use or before_tool_use):
                            out.append({"role": "assistant", "content": text})
                        text_parts.clear()

                    for block in content:
                        if not isinstance(block, dict):
                            continue
                        if block.get("type") == "text":
                            text_parts.append(str(block.get("text", "") or ""))
                        elif block.get("type") == "tool_use":
                            flush_assistant_text(before_tool_use=True)
                            call_id = block.get("id", "")
                            # Approval resumes can carry an assistant tool call
                            # that was intentionally paused before a tool
                            # output existed. The Responses API rejects that
                            # history shape, so only replay tool calls that
                            # have a matching function_call_output.
                            if call_id and call_id in tool_result_ids:
                                out.append({
                                    "type": "function_call",
                                    "call_id": call_id,
                                    "name": block.get("name", ""),
                                    "arguments": json.dumps(block.get("input") or {}),
                                })
                    flush_assistant_text()
                else:
                    out.append({"role": "assistant", "content": str(content or "")})
        return out

    @staticmethod
    def _to_tools(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out = []
        for t in tools:
            if not isinstance(t, dict):
                continue
            name = t.get("name")
            if not isinstance(name, str) or not name:
                continue
            schema = t.get("input_schema") or {"type": "object", "properties": {}}
            out.append({
                "type": "function",
                "name": name,
                "description": t.get("description", ""),
                "strict": False,
                "parameters": schema,
            })
        return out

    @staticmethod
    def _parse(data: dict[str, Any]) -> dict[str, Any]:
        """Parse Responses API output → orchestrator format."""
        content_blocks: list[dict[str, Any]] = []
        has_tool = False
        for item in data.get("output", []):
            itype = item.get("type")
            if itype == "message":
                for block in item.get("content", []):
                    if block.get("type") == "output_text":
                        content_blocks.append({"type": "text", "text": block.get("text", "")})
            elif itype in {"reasoning", "thinking"}:
                summary_parts: list[str] = []
                for block in item.get("summary", []) or item.get("content", []):
                    if not isinstance(block, dict):
                        continue
                    text = block.get("text") or block.get("summary")
                    if isinstance(text, str) and text.strip():
                        summary_parts.append(text.strip())
                if summary_parts:
                    content_blocks.append({"type": "thinking", "text": "\n".join(summary_parts)})
            elif itype == "function_call":
                has_tool = True
                call_id = item.get("call_id") or item.get("id", "")
                args = item.get("arguments", "{}")
                parsed = _parse_tool_arguments(args)
                content_blocks.append({
                    "type": "tool_use",
                    "id": call_id,
                    "name": item.get("name", ""),
                    "input": parsed,
                })
        stop_reason = "tool_use" if has_tool else "end_turn"
        return {"stop_reason": stop_reason, "content": content_blocks}

    def create(
        self,
        *,
        messages: Any,
        tools: Any,
        max_tokens: int | None = None,
        system: str | None = None,
    ) -> dict[str, Any]:
        del max_tokens
        if _httpx is None:  # pragma: no cover
            raise ModelClientConfigurationError("httpx package is not installed")

        # Extract system message → instructions; pass the rest as input
        messages_list = list(messages)
        instructions_parts: list[str] = []
        if system:
            instructions_parts.append(system)
        non_system: list[dict[str, Any]] = []
        for msg in messages_list:
            if msg.get("role") == "system":
                c = msg.get("content", "")
                if isinstance(c, str):
                    instructions_parts.append(c)
                elif isinstance(c, list):
                    instructions_parts.append(
                        " ".join(b.get("text", "") for b in c if isinstance(b, dict) and b.get("type") == "text")
                    )
                else:
                    instructions_parts.append(str(c or ""))
            else:
                non_system.append(msg)
        instructions = " ".join(instructions_parts).strip() or "You are a helpful assistant."

        body: dict[str, Any] = {
            "model": self.model,
            "instructions": instructions,
            "store": False,
            "stream": True,
            "input": self._to_input(non_system),
            "tool_choice": "auto",
            "parallel_tool_calls": True,
        }
        effort = _normalize_reasoning_effort(self.reasoning_effort)
        if effort:
            body["reasoning"] = {"effort": effort}
        converted_tools = self._to_tools(list(tools))
        if converted_tools:
            body["tools"] = converted_tools

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "ChatGPT-Account-Id": self.account_id,
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
        }

        text_parts: list[str] = []
        thinking_parts: list[str] = []
        tool_blocks: list[dict[str, Any]] = []
        # track in-progress function call by item index
        fn_calls: dict[int, dict[str, Any]] = {}

        with _httpx.Client(timeout=120.0) as client:
            with client.stream("POST", self._ENDPOINT, json=body, headers=headers) as resp:
                if resp.status_code >= 400:
                    detail = resp.read().decode("utf-8", errors="replace")
                    detail = " ".join(detail.split())
                    if len(detail) > 500:
                        detail = detail[:497].rstrip() + "..."
                    raise ModelClientConfigurationError(
                        f"Codex Responses request failed with HTTP {resp.status_code}: {detail or resp.reason_phrase}"
                    )
                resp.raise_for_status()
                for raw_line in resp.iter_lines():
                    line = raw_line.strip() if isinstance(raw_line, str) else raw_line.decode().strip()
                    if not line.startswith("data: "):
                        continue
                    data_str = line[6:]
                    if data_str == "[DONE]":
                        break
                    try:
                        ev = json.loads(data_str)
                    except Exception:
                        continue

                    etype = ev.get("type", "")

                    if etype == "response.output_text.delta":
                        text_parts.append(ev.get("delta", ""))
                    elif etype in {
                        "response.reasoning_summary_text.delta",
                        "response.reasoning_text.delta",
                    }:
                        thinking_parts.append(ev.get("delta", ""))

                    elif etype == "response.output_item.added":
                        item = ev.get("item", {})
                        if item.get("type") == "function_call":
                            idx = ev.get("output_index", len(fn_calls))
                            fn_calls[idx] = {
                                "id": item.get("call_id") or item.get("id", ""),
                                "name": item.get("name", ""),
                                "arguments": item.get("arguments", ""),
                            }
                        elif item.get("type") in {"reasoning", "thinking"}:
                            for block in item.get("summary", []) or item.get("content", []):
                                if isinstance(block, dict) and isinstance(block.get("text"), str):
                                    thinking_parts.append(block["text"])

                    elif etype == "response.function_call_arguments.delta":
                        idx = ev.get("output_index", -1)
                        if idx in fn_calls:
                            fn_calls[idx]["arguments"] += ev.get("delta", "")

                    elif etype == "response.output_item.done":
                        item = ev.get("item", {})
                        if item.get("type") == "function_call":
                            call_id = item.get("call_id") or item.get("id", "")
                            args_str = item.get("arguments", "{}")
                            parsed_input = _parse_tool_arguments(args_str)
                            tool_blocks.append({
                                "type": "tool_use",
                                "id": call_id,
                                "name": item.get("name", ""),
                                "input": parsed_input,
                            })

        # Also flush any fn_calls that didn't get an output_item.done
        seen_ids = {b["id"] for b in tool_blocks}
        for fc in fn_calls.values():
            if fc["id"] not in seen_ids:
                args_str = fc.get("arguments", "{}")
                parsed_input = _parse_tool_arguments(args_str)
                tool_blocks.append({
                    "type": "tool_use",
                    "id": fc["id"],
                    "name": fc["name"],
                    "input": parsed_input,
                })

        content_blocks: list[dict[str, Any]] = []
        full_thinking = "".join(thinking_parts).strip()
        if full_thinking:
            content_blocks.append({"type": "thinking", "text": full_thinking})
        full_text = "".join(text_parts)
        if full_text:
            content_blocks.append({"type": "text", "text": full_text})
        content_blocks.extend(tool_blocks)

        stop_reason = "tool_use" if tool_blocks else "end_turn"
        return {"stop_reason": stop_reason, "content": content_blocks}


def build_model_client_from_settings(
    settings: object,
) -> "OpenAIChatCompletionsModelClient | AnthropicMessagesModelClient | CodexResponsesModelClient":
    """Build a model client from NullionSettings.

    Resolution order:
      1. provider=anthropic → Anthropic Messages API client
      2. provider=codex → ChatGPT Codex Responses client
      3. everything else with an API key → OpenAI-compatible chat client
    """
    model_cfg = getattr(settings, "model", None)
    api_key = getattr(model_cfg, "openai_api_key", None) if model_cfg is not None else None
    provider = getattr(model_cfg, "provider", None) if model_cfg is not None else None
    provider = provider.strip().lower() if isinstance(provider, str) else None

    if provider == "codex":
        codex_token = api_key.strip() if isinstance(api_key, str) and api_key.strip() else ""
        refresh_token = getattr(model_cfg, "codex_refresh_token", None) if model_cfg is not None else None
        refresh_token = refresh_token.strip() if isinstance(refresh_token, str) else ""

        # Attempt a token refresh up front when the saved access token is
        # absent, expired, or missing the chatgpt_account_id claim. The old
        # behaviour only refreshed when the access token was *missing*, so a
        # stale-but-parseable token would land at the misleading
        # "missing chatgpt_account_id" error even though the real fix was a
        # refresh.
        def _needs_refresh(tok: str) -> bool:
            if not tok:
                return True
            if _jwt_is_expired(tok):
                return True
            if not _extract_chatgpt_account_id(tok):
                return True
            return False

        refresh_error: str | None = None
        if _needs_refresh(codex_token) and refresh_token:
            try:
                refreshed = _refresh_codex_access_token(refresh_token)
                if refreshed:
                    codex_token = refreshed
                    # Persist the fresh access token so subsequent runs (and
                    # other processes) don't keep using the stale one.
                    try:
                        from nullion.web_app import (
                            _read_credentials_json,
                            _write_credentials_json,
                        )
                        _creds = _read_credentials_json() or {}
                        if _creds.get("provider") == "codex":
                            _creds["api_key"] = codex_token
                            _write_credentials_json(_creds)
                    except Exception:
                        pass  # best-effort persistence
            except ModelClientConfigurationError as exc:
                refresh_error = str(exc)

        if not codex_token:
            hint = f" Refresh attempt failed: {refresh_error}" if refresh_error else ""
            raise ModelClientConfigurationError(
                "Codex OAuth is selected, but no Codex OAuth access or refresh token is configured. "
                f"Run `nullion-auth` and choose ChatGPT / OpenAI Codex.{hint}"
            )
        account_id = _extract_chatgpt_account_id(codex_token)
        if not account_id:
            # We refreshed (if possible) and the token STILL lacks the
            # ChatGPT account claim — surface a more accurate diagnosis.
            if refresh_error:
                raise ModelClientConfigurationError(
                    f"Could not refresh Codex OAuth token: {refresh_error} "
                    "Re-run `nullion-auth` to sign in again."
                )
            if not refresh_token:
                raise ModelClientConfigurationError(
                    "Codex OAuth access token has no chatgpt_account_id claim "
                    "and no refresh token is saved. Re-run `nullion-auth` to "
                    "sign in with a ChatGPT account."
                )
            raise ModelClientConfigurationError(
                "Codex OAuth token is missing the ChatGPT account claim even "
                "after refresh. This usually means the linked OpenAI account "
                "doesn't have a ChatGPT subscription. Re-run `nullion-auth` "
                "to sign in with a ChatGPT account."
            )
        model_name = (getattr(model_cfg, "openai_model", None) or "").strip()
        if not model_name:
            raise ModelClientConfigurationError(
                "Codex provider has no model configured. Set NULLION_MODEL "
                "(or fill in the Model field under Settings → Model) before "
                "starting Nullion."
            )
        return CodexResponsesModelClient(
            api_key=codex_token,
            account_id=account_id,
            model=model_name,
            reasoning_effort=getattr(model_cfg, "reasoning_effort", None),
        )

    if provider == "anthropic":
        anthropic_key = getattr(model_cfg, "anthropic_api_key", None) if model_cfg is not None else None
        if not (isinstance(anthropic_key, str) and anthropic_key.strip()):
            anthropic_key = api_key
        if isinstance(anthropic_key, str) and anthropic_key.strip():
            if _anthropic is None:  # pragma: no cover
                raise ModelClientConfigurationError("anthropic package is not installed")
            model_name = (
                getattr(model_cfg, "anthropic_model", None)
                or getattr(model_cfg, "openai_model", None)
                or ""
            ).strip()
            if not model_name:
                raise ModelClientConfigurationError(
                    "Anthropic provider has no model configured. Set "
                    "NULLION_MODEL (or fill in the Model field under "
                    "Settings → Model) before starting Nullion."
                )
            return AnthropicMessagesModelClient(
                client=_anthropic.Anthropic(api_key=anthropic_key.strip()),
                model=model_name,
                reasoning_effort=getattr(model_cfg, "reasoning_effort", None),
            )

    if isinstance(api_key, str) and api_key.strip():
        api_key = api_key.strip()

        # Standard OpenAI-compatible provider
        if OpenAI is None:  # pragma: no cover
            raise ModelClientConfigurationError("openai package is not installed")
        base_url = getattr(model_cfg, "openai_base_url", None)
        model_name = (getattr(model_cfg, "openai_model", None) or "").strip()
        if not model_name:
            raise ModelClientConfigurationError(
                "OpenAI-compatible provider has no model configured. Set "
                "NULLION_MODEL (or fill in the Model field under Settings → "
                "Model) before starting Nullion. The runtime is "
                "vendor-agnostic — it won't pick a default for you."
            )
        client = OpenAI(
            api_key=api_key,
            base_url=base_url.strip() if isinstance(base_url, str) and base_url.strip() else None,
        )
        return OpenAIChatCompletionsModelClient(
            client=client,
            model=model_name,
            provider=provider,
            reasoning_effort=getattr(model_cfg, "reasoning_effort", None),
        )

    raise ModelClientConfigurationError(
        "No LLM provider configured. Run `nullion-auth` to set one up."
    )


def clone_model_client_with_model(client: object, model_name: str) -> object:
    """Return a copy of *client* with the ``model`` field replaced by *model_name*.

    Works for any dataclass-based model client that has a ``model`` field
    (both ``OpenAIChatCompletionsModelClient`` and ``CodexResponsesModelClient``
    qualify).  The underlying API connection (``client`` field, credentials,
    base_url) is reused — only the model name changes.

    Raises:
        ValueError: if the client is not a dataclass or has no ``model`` field.
    """
    from dataclasses import fields, replace as _dc_replace

    try:
        field_names = {f.name for f in fields(client)}  # type: ignore[arg-type]
    except TypeError:
        raise ValueError(
            f"Cannot swap model on {type(client).__name__}: not a dataclass"
        )
    if "model" not in field_names:
        raise ValueError(
            f"Cannot swap model on {type(client).__name__}: no 'model' field"
        )
    return _dc_replace(client, model=model_name)  # type: ignore[arg-type]


__all__ = [
    "ModelClientConfigurationError",
    "AnthropicMessagesModelClient",
    "OpenAIChatCompletionsModelClient",
    "CodexResponsesModelClient",
    "build_model_client_from_settings",
    "clone_model_client_with_model",
]
