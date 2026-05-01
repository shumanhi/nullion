"""LangChain adapters for Nullion models and tools.

LangChain and Deep Agents provide the supported mini-agent harness, while
Nullion remains responsible for policy, tool scopes, and delivery contracts.
"""

from __future__ import annotations

import json
import asyncio
import inspect
import os
from importlib.util import find_spec
from typing import Any
from uuid import uuid4

from nullion.tools import ToolInvocation, ToolResult


def langchain_available() -> bool:
    return find_spec("langchain") is not None or find_spec("langchain_core") is not None


def deepagents_available() -> bool:
    return find_spec("deepagents") is not None


def optional_agent_harness_status() -> dict[str, bool]:
    return {
        "langchain": langchain_available(),
        "deepagents": deepagents_available(),
    }


def nullion_tools_as_langchain_tools(
    tool_registry: Any,
    *,
    allowed_tools: list[str],
    principal_id: str,
    cleanup_scope: str,
    policy_store: Any = None,
    tool_result_callback: Any = None,
) -> list[Any]:
    """Expose scoped Nullion tools as LangChain StructuredTool objects.

    Tool execution still flows through Nullion's registry/runtime boundary, so
    Sentinel policy and per-principal scopes remain the source of truth.
    """
    try:
        from langchain_core.tools import StructuredTool
    except Exception as exc:  # pragma: no cover - depends on optional package
        raise RuntimeError("LangChain tool adapter requires langchain-core") from exc

    tool_definitions = _scoped_tool_definitions(tool_registry, allowed_tools=allowed_tools)
    return [
        StructuredTool.from_function(
            coroutine=_make_langchain_tool_coroutine(
                name=str(tool_def.get("name")),
                fallback_tools=[str(tool) for tool in (tool_def.get("fallback_tools") or []) if isinstance(tool, str)],
                principal_id=principal_id,
                cleanup_scope=cleanup_scope,
                tool_registry=tool_registry,
                policy_store=policy_store,
                tool_result_callback=tool_result_callback,
            ),
            name=str(tool_def.get("name")),
            description=_enhanced_tool_description(tool_def),
            args_schema=_normalized_input_schema(tool_def),
            metadata={"nullion_tool_group": _tool_group_for_name(str(tool_def.get("name") or ""))},
        )
        for tool_def in tool_definitions
        if isinstance(tool_def.get("name"), str)
    ]


def nullion_client_as_langchain_chat_model(client: Any, *, default_max_tokens: int = 2048) -> Any:
    """Wrap a Nullion model client as a LangChain chat model.

    The wrapped client must expose Nullion's normalized ``create`` interface:
    ``create(messages=[...], tools=[...], max_tokens=..., system=...)`` and
    return normalized content blocks with ``text`` and ``tool_use`` entries.
    """
    try:
        from langchain_core.language_models.chat_models import BaseChatModel
        from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage, ToolMessage
        from langchain_core.outputs import ChatGeneration, ChatResult
        from pydantic import ConfigDict, Field
    except Exception as exc:  # pragma: no cover - depends on optional package
        raise RuntimeError("LangChain model adapter requires langchain-core") from exc

    tokens_default = default_max_tokens

    class NullionChatModel(BaseChatModel):
        model_config = ConfigDict(arbitrary_types_allowed=True)

        nullion_client: Any = Field(exclude=True)
        default_max_tokens: int = tokens_default
        bound_tools: tuple[Any, ...] = ()

        @property
        def _llm_type(self) -> str:
            return "nullion-chat-model"

        def bind_tools(self, tools, *, tool_choice: str | None = None, **kwargs: Any):
            del tool_choice, kwargs
            return self.model_copy(update={"bound_tools": tuple(tools or ())})

        def _generate(
            self,
            messages: list[BaseMessage],
            stop: list[str] | None = None,
            run_manager: Any = None,
            **kwargs: Any,
        ) -> ChatResult:
            del stop, run_manager
            response = _run_sync(
                _call_nullion_model(
                    self.nullion_client,
                    messages=messages,
                    tools=tuple(kwargs.get("tools") or self.bound_tools),
                    default_max_tokens=self.default_max_tokens,
                    message_types=(AIMessage, HumanMessage, SystemMessage, ToolMessage),
                )
            )
            return _chat_result_from_nullion_response(response)

        async def _agenerate(
            self,
            messages: list[BaseMessage],
            stop: list[str] | None = None,
            run_manager: Any = None,
            **kwargs: Any,
        ) -> ChatResult:
            del stop, run_manager
            response = await _call_nullion_model(
                self.nullion_client,
                messages=messages,
                tools=tuple(kwargs.get("tools") or self.bound_tools),
                default_max_tokens=self.default_max_tokens,
                message_types=(AIMessage, HumanMessage, SystemMessage, ToolMessage),
            )
            return _chat_result_from_nullion_response(response)

    return NullionChatModel(nullion_client=client, default_max_tokens=default_max_tokens)


def _scoped_tool_definitions(tool_registry: Any, *, allowed_tools: list[str]) -> list[dict[str, Any]]:
    if not allowed_tools:
        return []
    try:
        return list(tool_registry.list_tool_definitions(allowed=allowed_tools))
    except TypeError:
        definitions = list(tool_registry.list_tool_definitions())
        allowed = set(allowed_tools)
        return [definition for definition in definitions if definition.get("name") in allowed]
    except Exception:
        return []


def _normalized_input_schema(tool_definition: dict[str, Any]) -> dict[str, Any]:
    schema = tool_definition.get("input_schema")
    if not isinstance(schema, dict):
        return {"type": "object", "properties": {}}
    normalized = dict(schema)
    normalized.setdefault("type", "object")
    normalized.setdefault("properties", {})
    return normalized


def _make_langchain_tool_coroutine(
    *,
    name: str,
    fallback_tools: list[str],
    principal_id: str,
    cleanup_scope: str,
    tool_registry: Any,
    policy_store: Any,
    tool_result_callback: Any,
):
    async def _run_nullion_tool(**kwargs: Any) -> str:
        invocation = ToolInvocation(
            invocation_id=f"lc-{uuid4().hex[:12]}",
            tool_name=name,
            principal_id=principal_id,
            arguments=dict(kwargs),
            capsule_id=cleanup_scope,
        )
        result = _invoke_nullion_tool(
            invocation,
            tool_registry=tool_registry,
            policy_store=policy_store,
        )
        emitted_results = [result]
        if _result_allows_fallback(result):
            for fallback_name in fallback_tools:
                fallback_invocation = ToolInvocation(
                    invocation_id=f"lc-{uuid4().hex[:12]}",
                    tool_name=fallback_name,
                    principal_id=principal_id,
                    arguments=dict(kwargs),
                    capsule_id=cleanup_scope,
                )
                fallback_result = _invoke_nullion_tool(
                    fallback_invocation,
                    tool_registry=tool_registry,
                    policy_store=policy_store,
                )
                emitted_results.append(fallback_result)
                if not _result_allows_fallback(fallback_result):
                    result = fallback_result
                    break
        if tool_result_callback is not None:
            for emitted_result in emitted_results:
                tool_result_callback(emitted_result)
        return _tool_result_text(result)

    _run_nullion_tool.__name__ = f"nullion_{name}"
    _run_nullion_tool.__doc__ = f"Run the Nullion {name} tool through Sentinel policy."
    return _run_nullion_tool


def _invoke_nullion_tool(
    invocation: ToolInvocation,
    *,
    tool_registry: Any,
    policy_store: Any,
) -> ToolResult:
    attempts = max(1, _tool_retry_attempts())
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            if policy_store is None:
                return tool_registry.invoke(invocation)
            from nullion.runtime import invoke_tool

            return invoke_tool(policy_store, invocation, registry=tool_registry)
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts:
                break
    error = str(last_exc) if last_exc is not None else "tool invocation failed"
    return ToolResult(
        invocation.invocation_id,
        invocation.tool_name,
        "failed",
        {"error": error or last_exc.__class__.__name__},
        error=error or last_exc.__class__.__name__,
    )


def _tool_retry_attempts() -> int:
    raw = os.environ.get("NULLION_LANGCHAIN_TOOL_RETRIES", "2")
    try:
        return max(1, int(raw))
    except ValueError:
        return 2


def _result_allows_fallback(result: ToolResult) -> bool:
    output = result.output if isinstance(result.output, dict) else {}
    if output.get("reason") == "approval_required" or output.get("requires_approval"):
        return False
    return str(result.status).lower() in {"failed", "failure", "error"} or bool(result.error)


def _tool_result_text(result: ToolResult) -> str:
    if result.error:
        return result.error
    output = result.output
    if isinstance(output, str):
        return output
    try:
        return json.dumps(output, ensure_ascii=True)
    except TypeError:
        return str(output)


async def _call_nullion_model(
    client: Any,
    *,
    messages: list[Any],
    tools: tuple[Any, ...],
    default_max_tokens: int,
    message_types: tuple[type, type, type, type],
) -> dict[str, Any]:
    lc_messages, system = _nullion_messages_from_langchain_messages(messages, message_types=message_types)
    create_kwargs: dict[str, Any] = {
        "messages": lc_messages,
        "tools": [_tool_definition_from_langchain_tool(tool) for tool in tools],
        "max_tokens": default_max_tokens,
        "system": system,
    }
    create = getattr(client, "create")
    attempts = max(1, _model_retry_attempts())
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await _call_create(create, create_kwargs)
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            await asyncio.sleep(min(0.25 * attempt, 1.0))
    assert last_exc is not None
    raise last_exc


async def _call_create(create: Any, create_kwargs: dict[str, Any]) -> dict[str, Any]:
    if inspect.iscoroutinefunction(create):
        return await create(**create_kwargs)
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: create(**create_kwargs))


def _model_retry_attempts() -> int:
    raw = os.environ.get("NULLION_LANGCHAIN_MODEL_RETRIES", "2")
    try:
        return max(1, int(raw))
    except ValueError:
        return 2


def _nullion_messages_from_langchain_messages(
    messages: list[Any],
    *,
    message_types: tuple[type, type, type, type],
) -> tuple[list[dict[str, Any]], str | None]:
    AIMessage, HumanMessage, SystemMessage, ToolMessage = message_types
    system_parts: list[str] = []
    converted: list[dict[str, Any]] = []
    for message in messages:
        if isinstance(message, SystemMessage):
            text = _message_text(message.content)
            if text:
                system_parts.append(text)
            continue
        if isinstance(message, HumanMessage):
            converted.append({"role": "user", "content": [{"type": "text", "text": _message_text(message.content)}]})
            continue
        if isinstance(message, ToolMessage):
            converted.append(
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": str(getattr(message, "tool_call_id", "") or ""),
                            "content": [{"type": "text", "text": _message_text(message.content)}],
                        }
                    ],
                }
            )
            continue
        if isinstance(message, AIMessage):
            content: list[dict[str, Any]] = []
            text = _message_text(message.content)
            if text:
                content.append({"type": "text", "text": text})
            for tool_call in getattr(message, "tool_calls", []) or []:
                content.append(
                    {
                        "type": "tool_use",
                        "id": str(tool_call.get("id") or f"tool-{uuid4().hex[:8]}"),
                        "name": str(tool_call.get("name") or ""),
                        "input": dict(tool_call.get("args") or {}),
                    }
                )
            converted.append({"role": "assistant", "content": content or [{"type": "text", "text": ""}]})
            continue
        converted.append({"role": "user", "content": [{"type": "text", "text": _message_text(getattr(message, "content", ""))}]})
    system = "\n\n".join(part for part in system_parts if part).strip() or None
    return converted, system


def _message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict):
                if isinstance(block.get("text"), str):
                    parts.append(block["text"])
                elif isinstance(block.get("content"), str):
                    parts.append(block["content"])
            elif hasattr(block, "text"):
                parts.append(str(block.text))
        return "".join(parts)
    return str(content or "")


def _tool_definition_from_langchain_tool(tool: Any) -> dict[str, Any]:
    if isinstance(tool, dict):
        if "input_schema" in tool:
            return dict(tool)
        function = tool.get("function")
        if isinstance(function, dict):
            return {
                "name": str(function.get("name") or tool.get("name") or ""),
                "description": str(function.get("description") or ""),
                "input_schema": function.get("parameters") or {"type": "object", "properties": {}},
            }
        return {
            "name": str(tool.get("name") or ""),
            "description": str(tool.get("description") or ""),
            "input_schema": tool.get("args_schema") or tool.get("input_schema") or {"type": "object", "properties": {}},
        }
    return {
        "name": str(getattr(tool, "name", "")),
        "description": str(getattr(tool, "description", "") or ""),
        "input_schema": _args_schema_from_langchain_tool(tool),
    }


def _args_schema_from_langchain_tool(tool: Any) -> dict[str, Any]:
    args_schema = getattr(tool, "args_schema", None)
    if isinstance(args_schema, dict):
        return args_schema
    if args_schema is not None and hasattr(args_schema, "model_json_schema"):
        return args_schema.model_json_schema()
    args = getattr(tool, "args", None)
    if isinstance(args, dict):
        return {"type": "object", "properties": args}
    return {"type": "object", "properties": {}}


def _enhanced_tool_description(tool_definition: dict[str, Any]) -> str:
    name = str(tool_definition.get("name") or "")
    base = str(tool_definition.get("description") or "").strip()
    group = _tool_group_for_name(name)
    suffix = f"Nullion scoped tool. Group: {group}. Sentinel policy and approval checks still apply."
    return f"{base}\n\n{suffix}" if base else suffix


def _tool_group_for_name(name: str) -> str:
    lowered = str(name or "").lower()
    if lowered.startswith("browser_"):
        return "browser"
    if lowered in {"web_search", "web_fetch"}:
        return "research"
    if lowered in {"file_write", "pdf_create", "pdf_edit", "render", "image_generate"} or "screenshot" in lowered:
        return "artifact"
    if lowered in {"file_read", "file_search", "workspace_summary"}:
        return "repo_analysis"
    if "approval" in lowered:
        return "approval"
    if "service" in lowered or "doctor" in lowered or "health" in lowered:
        return "doctor"
    return "general"


def _chat_result_from_nullion_response(response: dict[str, Any]):
    from langchain_core.messages import AIMessage
    from langchain_core.outputs import ChatGeneration, ChatResult

    content_blocks = response.get("content") or []
    text_parts: list[str] = []
    tool_calls: list[dict[str, Any]] = []
    if isinstance(content_blocks, str):
        text_parts.append(content_blocks)
    elif isinstance(content_blocks, list):
        for block in content_blocks:
            if isinstance(block, str):
                text_parts.append(block)
                continue
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text":
                text_parts.append(str(block.get("text") or ""))
            elif block.get("type") == "tool_use":
                tool_calls.append(
                    {
                        "name": str(block.get("name") or ""),
                        "args": dict(block.get("input") or {}),
                        "id": str(block.get("id") or f"tool-{uuid4().hex[:8]}"),
                    }
                )
    message = AIMessage(content="".join(text_parts), tool_calls=tool_calls)
    return ChatResult(generations=[ChatGeneration(message=message)])


def _run_sync(awaitable: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(awaitable)
    raise RuntimeError("Synchronous LangChain model calls are not supported inside an active event loop")


__all__ = [
    "deepagents_available",
    "langchain_available",
    "nullion_client_as_langchain_chat_model",
    "nullion_tools_as_langchain_tools",
    "optional_agent_harness_status",
]
