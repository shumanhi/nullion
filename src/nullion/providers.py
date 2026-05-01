"""Provider resolution for capability plugins."""

from __future__ import annotations

import base64
import email
from email import policy
import imaplib
import inspect
import json
import mimetypes
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
from urllib.parse import quote, urlencode
import urllib.error
import urllib.request

from nullion.chat_backend import fetch_url_snapshot, search_web
from nullion.messaging_adapters import messaging_media_scratch_root


_GOOGLE_WORKSPACE_SCRIPT = Path(
    os.environ.get(
        "NULLION_GOOGLE_WORKSPACE_SCRIPT",
        str(Path.home() / ".nullion" / "providers" / "google-workspace" / "google_api.py"),
    )
)


_MEDIA_COMMAND_TIMEOUT_SECONDS = int(os.environ.get("NULLION_MEDIA_COMMAND_TIMEOUT_SECONDS", "120"))


def _read_credentials_json() -> dict[str, object]:
    try:
        from nullion.credential_store import migrate_credentials_json_to_db

        path = Path.home() / ".nullion" / "credentials.json"
        payload = migrate_credentials_json_to_db(path, db_path=path.with_name("runtime.db"))
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    return {}


def _media_model_selection(provider_env: str, model_env: str, enabled_env: str) -> tuple[str, str] | None:
    enabled = os.environ.get(enabled_env)
    if enabled is not None and enabled.strip().lower() in {"0", "false", "no", "off"}:
        return None
    provider = os.environ.get(provider_env, "").strip()
    model = os.environ.get(model_env, "").strip()
    if provider and model:
        return provider, model
    return None


def _provider_key_for_media(provider: str) -> str:
    provider_l = provider.strip().lower()
    media_env_names = {
        "anthropic": ("NULLION_MEDIA_ANTHROPIC_API_KEY",),
        "openai": ("NULLION_MEDIA_OPENAI_API_KEY",),
        "openrouter": ("NULLION_MEDIA_OPENROUTER_API_KEY",),
        "gemini": ("NULLION_MEDIA_GEMINI_API_KEY",),
        "groq": ("NULLION_MEDIA_GROQ_API_KEY",),
        "mistral": ("NULLION_MEDIA_MISTRAL_API_KEY",),
        "deepseek": ("NULLION_MEDIA_DEEPSEEK_API_KEY",),
        "xai": ("NULLION_MEDIA_XAI_API_KEY",),
        "together": ("NULLION_MEDIA_TOGETHER_API_KEY",),
        "custom": ("NULLION_MEDIA_CUSTOM_API_KEY",),
    }
    for env_name in media_env_names.get(provider_l, ()):
        media_key = os.environ.get(env_name, "").strip()
        if media_key:
            return media_key
    if provider_l == "anthropic":
        env_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    else:
        provider_env_names = {
            "openrouter": ("NULLION_OPENROUTER_API_KEY", "OPENROUTER_API_KEY"),
            "gemini": ("NULLION_GEMINI_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY"),
            "groq": ("NULLION_GROQ_API_KEY", "GROQ_API_KEY"),
            "mistral": ("NULLION_MISTRAL_API_KEY", "MISTRAL_API_KEY"),
            "deepseek": ("NULLION_DEEPSEEK_API_KEY", "DEEPSEEK_API_KEY"),
            "xai": ("NULLION_XAI_API_KEY", "XAI_API_KEY"),
            "together": ("NULLION_TOGETHER_API_KEY", "TOGETHER_API_KEY"),
            "openai": ("NULLION_OPENAI_API_KEY", "OPENAI_API_KEY"),
        }
        env_key = ""
        for env_name in provider_env_names.get(provider_l, ("OPENAI_API_KEY", "NULLION_OPENAI_API_KEY")):
            env_key = os.environ.get(env_name, "").strip()
            if env_key:
                break
    if env_key:
        return env_key
    creds = _read_credentials_json()
    keys = creds.get("keys")
    if isinstance(keys, dict):
        key = keys.get(provider_l) or keys.get(provider)
        if isinstance(key, str) and key.strip():
            return key.strip()
    if creds.get("provider") == provider and isinstance(creds.get("api_key"), str):
        return str(creds.get("api_key") or "").strip()
    return ""


def _media_settings_for_model(provider: str, model: str):
    from nullion.config import load_settings

    env = dict(os.environ)
    env["NULLION_MODEL_PROVIDER"] = provider
    env["NULLION_MODEL"] = model
    key = _provider_key_for_media(provider)
    if key:
        if provider.strip().lower() == "anthropic":
            env["ANTHROPIC_API_KEY"] = key
        else:
            env["OPENAI_API_KEY"] = key
            env["NULLION_OPENAI_API_KEY"] = key
    if provider.strip().lower() in {"openrouter", "openrouter-key"}:
        env.setdefault("NULLION_OPENAI_BASE_URL", "https://openrouter.ai/api/v1")
    if provider.strip().lower() == "custom":
        custom_base_url = os.environ.get("NULLION_MEDIA_CUSTOM_BASE_URL", "").strip()
        if custom_base_url:
            env["NULLION_OPENAI_BASE_URL"] = custom_base_url
    settings = load_settings(env=env)
    settings.model.provider = provider
    settings.model.openai_model = model
    return settings


def _whisper_cpp_model_path() -> Path:
    return Path.home() / ".nullion" / "models" / "ggml-base.en.bin"


def _which_local_tool(name: str) -> str:
    if shutil.which(name):
        return name
    if getattr(shutil.which, "__module__", "shutil") != "shutil":
        return ""
    for prefix in ("/opt/homebrew/bin", "/usr/local/bin"):
        candidate = Path(prefix) / name
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    return ""


def _first_env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return ""


def _require_env(*names: str, provider_name: str) -> str:
    value = _first_env(*names)
    if value:
        return value
    labels = " or ".join(names)
    raise RuntimeError(f"{provider_name} requires {labels}")


def _request_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    payload: dict[str, object] | None = None,
    timeout_seconds: int = 20,
) -> dict[str, object]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request_headers = dict(headers or {})
    if payload is not None:
        request_headers.setdefault("Content-Type", "application/json")
    request = urllib.request.Request(url, data=data, headers=request_headers, method=method)
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        body = response.read(1_000_000).decode("utf-8", "ignore")
    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise RuntimeError("search provider returned non-object JSON payload")
    return parsed


def _extract_rows(payload: dict[str, object], *, provider_name: str) -> list[object]:
    for key in ("results", "messages", "items", "data"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return rows
    raise RuntimeError(f"{provider_name} returned no results list")


def _clamped_limit(limit: int, *, max_limit: int = 20) -> int:
    return max(1, min(int(limit), max_limit))


def _result(title: object, url: object, snippet: object = "") -> dict[str, object] | None:
    if not isinstance(title, str) or not title.strip():
        return None
    if not isinstance(url, str) or not url.strip():
        return None
    item: dict[str, object] = {
        "title": title.strip(),
        "url": url.strip(),
        "snippet": snippet.strip() if isinstance(snippet, str) else "",
    }
    return item


def _brave_web_search(query: str, limit: int) -> list[dict[str, object]]:
    api_key = _require_env(
        "NULLION_BRAVE_SEARCH_API_KEY",
        "BRAVE_SEARCH_API_KEY",
        provider_name="brave_search_provider",
    )
    count = _clamped_limit(limit)
    payload = _request_json(
        "https://api.search.brave.com/res/v1/web/search?"
        + urlencode({"q": query, "count": str(count)}),
        headers={
            "Accept": "application/json",
            "X-Subscription-Token": api_key,
        },
    )
    web = payload.get("web")
    rows = web.get("results") if isinstance(web, dict) else []
    if not isinstance(rows, list):
        return []
    results: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        item = _result(row.get("title"), row.get("url"), row.get("description"))
        if item is not None:
            results.append(item)
    return results[:count]


def _google_custom_search(query: str, limit: int) -> list[dict[str, object]]:
    api_key = _require_env(
        "NULLION_GOOGLE_SEARCH_API_KEY",
        "GOOGLE_SEARCH_API_KEY",
        provider_name="google_custom_search_provider",
    )
    cx = _require_env(
        "NULLION_GOOGLE_SEARCH_CX",
        "GOOGLE_SEARCH_CX",
        provider_name="google_custom_search_provider",
    )
    count = _clamped_limit(limit, max_limit=10)
    payload = _request_json(
        "https://www.googleapis.com/customsearch/v1?"
        + urlencode({"key": api_key, "cx": cx, "q": query, "num": str(count)}),
        headers={"Accept": "application/json"},
    )
    rows = payload.get("items")
    if not isinstance(rows, list):
        return []
    results: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        item = _result(row.get("title"), row.get("link"), row.get("snippet"))
        if item is not None:
            results.append(item)
    return results[:count]


def _perplexity_search(query: str, limit: int) -> list[dict[str, object]]:
    api_key = _require_env(
        "NULLION_PERPLEXITY_API_KEY",
        "PERPLEXITY_API_KEY",
        provider_name="perplexity_search_provider",
    )
    count = _clamped_limit(limit)
    payload = _request_json(
        "https://api.perplexity.ai/search",
        method="POST",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        payload={
            "query": query,
            "max_results": count,
            "max_tokens_per_page": 1024,
        },
    )
    rows = payload.get("results")
    if not isinstance(rows, list):
        return []
    results: list[dict[str, object]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        item = _result(row.get("title"), row.get("url"), row.get("snippet"))
        if item is None:
            continue
        if isinstance(row.get("date"), str) and row.get("date"):
            item["date"] = row["date"]
        results.append(item)
    return results[:count]


def _duckduckgo_instant_answer_search(query: str, limit: int) -> list[dict[str, object]]:
    count = _clamped_limit(limit)
    payload = _request_json(
        "https://api.duckduckgo.com/?"
        + urlencode({"q": query, "format": "json", "no_redirect": "1", "no_html": "1"}),
        headers={"Accept": "application/json"},
    )
    results: list[dict[str, object]] = []
    abstract = _result(
        payload.get("Heading") or query,
        payload.get("AbstractURL"),
        payload.get("AbstractText"),
    )
    if abstract is not None:
        results.append(abstract)

    def collect_topics(topics: object) -> None:
        if not isinstance(topics, list):
            return
        for topic in topics:
            if len(results) >= count:
                return
            if not isinstance(topic, dict):
                continue
            if isinstance(topic.get("Topics"), list):
                collect_topics(topic.get("Topics"))
                continue
            item = _result(topic.get("Text"), topic.get("FirstURL"), topic.get("Text"))
            if item is not None:
                results.append(item)

    collect_topics(payload.get("RelatedTopics"))
    return results[:count]


def _run_media_command_template(
    template: str,
    *,
    substitutions: dict[str, str],
    timeout_seconds: int = _MEDIA_COMMAND_TIMEOUT_SECONDS,
) -> subprocess.CompletedProcess[str]:
    try:
        command = template.format(
            **{key: shlex.quote(value) for key, value in substitutions.items()}
        )
    except KeyError as exc:
        missing = exc.args[0]
        raise RuntimeError(f"media provider command template is missing placeholder: {missing}") from exc
    argv = shlex.split(command)
    if not argv:
        raise RuntimeError("media provider command template is empty")
    executable = argv[0]
    if Path(executable).name == executable and shutil.which(executable) is None:
        raise RuntimeError(f"media provider executable not found: {executable}")
    completed = subprocess.run(
        argv,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_seconds,
    )
    if completed.returncode != 0:
        failure_output = (completed.stderr or completed.stdout).strip() or "media provider command failed"
        raise RuntimeError(failure_output)
    return completed


def _audio_transcriber_input_path(source_path: str, *, scratch_parent: Path) -> tuple[str, tempfile.TemporaryDirectory[str] | None]:
    suffix = Path(source_path).suffix.lower()
    if suffix == ".wav":
        return source_path, None
    ffmpeg = _which_local_tool("ffmpeg")
    if not ffmpeg:
        return source_path, None
    scratch_parent.mkdir(parents=True, exist_ok=True)
    temp_dir = tempfile.TemporaryDirectory(prefix="audio-", dir=scratch_parent)
    wav_path = Path(temp_dir.name) / "input.wav"
    convert = subprocess.run(
        [ffmpeg, "-y", "-i", source_path, "-ar", "16000", "-ac", "1", "-c:a", "pcm_s16le", str(wav_path)],
        capture_output=True,
        text=True,
        check=False,
        timeout=_MEDIA_COMMAND_TIMEOUT_SECONDS,
    )
    if convert.returncode != 0:
        temp_dir.cleanup()
        failure_output = (convert.stderr or convert.stdout).strip() or "ffmpeg audio conversion failed"
        raise RuntimeError(failure_output)
    return str(wav_path), temp_dir


def _local_audio_transcribe(path: str, language: str | None) -> dict[str, object]:
    template = os.environ.get("NULLION_AUDIO_TRANSCRIBE_COMMAND", "").strip()
    source_path = str(Path(path).expanduser())
    if template:
        prepared_path, temp_dir = _audio_transcriber_input_path(source_path, scratch_parent=messaging_media_scratch_root())
        try:
            completed = _run_media_command_template(
                template,
                substitutions={
                    "input": prepared_path,
                    "path": prepared_path,
                    "language": language or "",
                },
            )
            text = completed.stdout.strip()
            if not text:
                raise RuntimeError("audio transcription provider returned no text")
            return {
                "text": text,
                "language": language,
                "provider": "local_media_provider",
            }
        finally:
            if temp_dir is not None:
                temp_dir.cleanup()
    model_selection = _media_model_selection(
        "NULLION_AUDIO_TRANSCRIBE_PROVIDER",
        "NULLION_AUDIO_TRANSCRIBE_MODEL",
        "NULLION_AUDIO_TRANSCRIBE_ENABLED",
    )
    if model_selection is not None:
        provider, model = model_selection
        return _model_audio_transcribe(provider, model, path, language)
    return _default_local_audio_transcribe(source_path, language)


def _model_audio_transcribe(provider: str, model: str, path: str, language: str | None) -> dict[str, object]:
    provider_l = provider.strip().lower()
    if provider_l == "codex":
        raise RuntimeError(
            "Codex OAuth cannot be used for audio transcription. "
            "Configure a platform API key or an OpenAI-compatible transcription endpoint."
        )
    try:
        from openai import OpenAI
    except Exception as exc:  # pragma: no cover - optional dependency guard
        raise RuntimeError("openai package is required for provider-backed audio transcription") from exc
    api_key = _provider_key_for_media(provider)
    if not api_key:
        raise RuntimeError(f"{provider} audio transcription requires an API key")
    provider_base_urls = {
        "groq": "https://api.groq.com/openai/v1",
        "custom": os.environ.get("NULLION_MEDIA_CUSTOM_BASE_URL", "").strip(),
    }
    base_url = provider_base_urls.get(provider_l)
    if provider_l not in {"openai", "groq", "custom"}:
        raise RuntimeError(f"audio transcription model provider is not supported: {provider}")
    if provider_l == "custom" and not base_url:
        raise RuntimeError("custom audio transcription requires NULLION_MEDIA_CUSTOM_BASE_URL")
    client = OpenAI(api_key=api_key, base_url=base_url or None)
    kwargs: dict[str, object] = {"model": model}
    if language:
        kwargs["language"] = language
    with Path(path).expanduser().open("rb") as handle:
        result = client.audio.transcriptions.create(file=handle, **kwargs)
    text = getattr(result, "text", None)
    if not isinstance(text, str) or not text.strip():
        if isinstance(result, dict):
            text = str(result.get("text") or "")
    if not str(text or "").strip():
        raise RuntimeError("audio transcription provider returned no text")
    return {
        "text": str(text).strip(),
        "language": language,
        "provider": f"{provider}:{model}",
    }


def _default_local_audio_transcribe(path: str, language: str | None) -> dict[str, object]:
    whisper_cli = _which_local_tool("whisper-cli")
    if whisper_cli:
        scratch_root = messaging_media_scratch_root()
        whisper_input, temp_dir = _audio_transcriber_input_path(path, scratch_parent=scratch_root)
        try:
            argv = [whisper_cli]
            model_path = _whisper_cpp_model_path()
            if model_path.exists():
                argv.extend(["-m", str(model_path)])
            argv.extend(["-f", whisper_input, "-nt"])
            if language:
                argv.extend(["-l", language])
            completed = subprocess.run(
                argv,
                capture_output=True,
                text=True,
                check=False,
                timeout=_MEDIA_COMMAND_TIMEOUT_SECONDS,
            )
            if completed.returncode != 0:
                failure_output = (completed.stderr or completed.stdout).strip() or "whisper-cli failed"
                raise RuntimeError(failure_output)
            text = completed.stdout.strip()
            if not text:
                raise RuntimeError("whisper-cli returned no text")
            return {
                "text": text,
                "language": language,
                "provider": "local_media_provider:whisper-cli",
            }
        finally:
            if temp_dir is not None:
                temp_dir.cleanup()

    raise RuntimeError(
        "No local audio transcriber found. Set NULLION_AUDIO_TRANSCRIBE_COMMAND, "
        "or install whisper.cpp and configure whisper-cli with a small GGML model."
    )


def _local_image_extract_text(path: str) -> dict[str, object]:
    template = os.environ.get("NULLION_IMAGE_OCR_COMMAND", "").strip()
    if template:
        source_path = str(Path(path).expanduser())
        completed = _run_media_command_template(
            template,
            substitutions={
                "input": source_path,
                "path": source_path,
            },
        )
        text = completed.stdout.strip()
        if not text:
            raise RuntimeError("image OCR provider returned no text")
        return {
            "text": text,
            "provider": "local_media_provider",
        }
    model_selection = _media_model_selection(
        "NULLION_IMAGE_OCR_PROVIDER",
        "NULLION_IMAGE_OCR_MODEL",
        "NULLION_IMAGE_OCR_ENABLED",
    )
    if model_selection is not None:
        provider, model = model_selection
        return _model_image_extract_text(provider, model, path)
    if shutil.which("tesseract") is not None:
        template = "tesseract {input} stdout"
    else:
        raise RuntimeError(
            "local_media_provider requires NULLION_IMAGE_OCR_COMMAND or tesseract for image_extract_text"
        )
    source_path = str(Path(path).expanduser())
    completed = _run_media_command_template(
        template,
        substitutions={
            "input": source_path,
            "path": source_path,
        },
    )
    text = completed.stdout.strip()
    if not text:
        raise RuntimeError("image OCR provider returned no text")
    return {
        "text": text,
        "provider": "local_media_provider",
    }


def _model_image_extract_text(provider: str, model: str, path: str) -> dict[str, object]:
    from nullion.model_clients import build_model_client_from_settings

    source = Path(path).expanduser()
    media_type = mimetypes.guess_type(str(source))[0] or "image/png"
    encoded = base64.b64encode(source.read_bytes()).decode("ascii")
    settings = _media_settings_for_model(provider, model)
    client = build_model_client_from_settings(settings)
    create_kwargs = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Extract all visible text from this image. Return only the extracted text.",
                    },
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": encoded,
                        },
                    },
                ],
            }
        ],
        "tools": [],
    }
    if "max_tokens" in inspect.signature(client.create).parameters:
        create_kwargs["max_tokens"] = 2048
    result = client.create(**create_kwargs)
    content = result.get("content") if isinstance(result, dict) else None
    parts: list[str] = []
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text" and isinstance(block.get("text"), str):
                parts.append(str(block["text"]))
    text = "\n".join(part.strip() for part in parts if part.strip()).strip()
    if not text:
        raise RuntimeError("image OCR provider returned no text")
    return {
        "text": text,
        "provider": f"{provider}:{model}",
    }


def _local_image_generate(prompt: str, output_path: str, size: str | None, source_path: str | None = None) -> dict[str, object]:
    template = os.environ.get("NULLION_IMAGE_GENERATE_COMMAND", "").strip()
    if template:
        return _command_image_generate(template, prompt, output_path, size, source_path=source_path)
    model_selection = _media_model_selection(
        "NULLION_IMAGE_GENERATE_PROVIDER",
        "NULLION_IMAGE_GENERATE_MODEL",
        "NULLION_IMAGE_GENERATE_ENABLED",
    )
    if model_selection is not None:
        provider, model = model_selection
        return _model_image_generate(provider, model, prompt, output_path, size, source_path=source_path)
    raise RuntimeError(
        "local_media_provider requires NULLION_IMAGE_GENERATE_COMMAND for image_generate"
    )


def _command_image_generate(
    template: str,
    prompt: str,
    output_path: str,
    size: str | None,
    *,
    source_path: str | None = None,
) -> dict[str, object]:
    resolved_output = str(Path(output_path).expanduser())
    scratch_root = messaging_media_scratch_root()
    scratch_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="image-", dir=scratch_root) as temp_dir:
        prompt_file = Path(temp_dir) / "prompt.txt"
        prompt_file.write_text(prompt, encoding="utf-8")
        _run_media_command_template(
            template,
            substitutions={
                "prompt": prompt,
                "prompt_file": str(prompt_file),
                "output": resolved_output,
                "output_path": resolved_output,
                "size": size or "",
                "input": str(Path(source_path).expanduser()) if source_path else "",
                "source": str(Path(source_path).expanduser()) if source_path else "",
                "source_path": str(Path(source_path).expanduser()) if source_path else "",
            },
            timeout_seconds=max(_MEDIA_COMMAND_TIMEOUT_SECONDS, 180),
        )
    if not Path(resolved_output).expanduser().exists():
        raise RuntimeError("image generation provider did not create the requested output file")
    return {
        "path": resolved_output,
        "provider": "local_media_provider",
        "size": size,
    }


def _write_image_url_to_path(image_url: str, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if image_url.startswith("data:"):
        _, _, encoded = image_url.partition(",")
        if not encoded:
            raise RuntimeError("image generation provider returned an empty image data URL")
        output.write_bytes(base64.b64decode(encoded))
        return
    with urllib.request.urlopen(image_url, timeout=_MEDIA_COMMAND_TIMEOUT_SECONDS) as response_body:
        output.write_bytes(response_body.read())


def _openrouter_image_config(size: str | None) -> dict[str, str]:
    normalized = str(size or "").strip().lower()
    if normalized in {"1024x1024", "1k"}:
        return {"image_size": "1K", "aspect_ratio": "1:1"}
    if normalized in {"512x512", "0.5k"}:
        return {"image_size": "0.5K", "aspect_ratio": "1:1"}
    if normalized in {"2048x2048", "2k"}:
        return {"image_size": "2K", "aspect_ratio": "1:1"}
    if normalized in {"4096x4096", "4k"}:
        return {"image_size": "4K", "aspect_ratio": "1:1"}
    if "x" in normalized:
        width_raw, _, height_raw = normalized.partition("x")
        try:
            width = int(width_raw)
            height = int(height_raw)
        except ValueError:
            return {}
        if width > 0 and height > 0:
            from math import gcd

            divisor = gcd(width, height)
            return {"aspect_ratio": f"{width // divisor}:{height // divisor}"}
    return {}


def _openrouter_image_generate(
    api_key: str,
    model: str,
    prompt: str,
    output_path: str,
    size: str | None,
    *,
    source_path: str | None = None,
) -> dict[str, object]:
    content: str | list[dict[str, object]]
    if source_path:
        source = Path(source_path).expanduser()
        media_type = mimetypes.guess_type(str(source))[0] or "image/png"
        encoded = base64.b64encode(source.read_bytes()).decode("ascii")
        content = [
            {"type": "text", "text": prompt},
            {
                "type": "image_url",
                "image_url": {"url": f"data:{media_type};base64,{encoded}"},
            },
        ]
    else:
        content = prompt

    payload: dict[str, object] = {
        "model": model,
        "messages": [{"role": "user", "content": content}],
        "modalities": ["image", "text"],
        "stream": False,
    }
    image_config = _openrouter_image_config(size)
    if image_config:
        payload["image_config"] = image_config
    request = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=_MEDIA_COMMAND_TIMEOUT_SECONDS) as response_body:
            response_payload = json.loads(response_body.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.reason
        try:
            error_payload = json.loads(exc.read().decode("utf-8"))
            if isinstance(error_payload, dict):
                error = error_payload.get("error")
                if isinstance(error, dict) and error.get("message"):
                    detail = str(error["message"])
        except Exception:
            pass
        raise RuntimeError(f"OpenRouter image generation failed ({exc.code}): {detail}") from exc

    choices = response_payload.get("choices") if isinstance(response_payload, dict) else None
    message = choices[0].get("message") if isinstance(choices, list) and choices else None
    images = message.get("images") if isinstance(message, dict) else None
    first = images[0] if isinstance(images, list) and images else None
    image_url = None
    if isinstance(first, dict):
        image_url_payload = first.get("image_url") or first.get("imageUrl")
        if isinstance(image_url_payload, dict):
            image_url = image_url_payload.get("url")
        elif isinstance(image_url_payload, str):
            image_url = image_url_payload
    if not isinstance(image_url, str) or not image_url:
        raise RuntimeError(
            "OpenRouter image generation returned no image. Confirm the configured model supports image output."
        )
    output = Path(output_path).expanduser()
    _write_image_url_to_path(image_url, output)
    return {
        "path": str(output),
        "provider": f"openrouter:{model}",
        "size": size,
    }


def _request_gemini_json(url: str, *, api_key: str, payload: dict[str, object]) -> dict[str, object]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "x-goog-api-key": api_key,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=_MEDIA_COMMAND_TIMEOUT_SECONDS) as response_body:
            decoded = json.loads(response_body.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.reason
        try:
            error_payload = json.loads(exc.read().decode("utf-8"))
            if isinstance(error_payload, dict):
                error = error_payload.get("error")
                if isinstance(error, dict) and error.get("message"):
                    detail = str(error["message"])
        except Exception:
            pass
        raise RuntimeError(f"Gemini image generation failed ({exc.code}): {detail}") from exc
    if not isinstance(decoded, dict):
        raise RuntimeError("Gemini image generation returned a non-object response")
    return decoded


def _gemini_model_name(model: str) -> str:
    return model.strip().removeprefix("models/")


def _gemini_imagen_generate(
    api_key: str,
    model: str,
    prompt: str,
    output_path: str,
    size: str | None,
    *,
    source_path: str | None = None,
) -> dict[str, object]:
    if source_path:
        raise RuntimeError(
            "The configured Gemini Imagen model supports text-to-image only. "
            "Use a Gemini image model such as gemini-3.1-flash-image-preview or gemini-2.5-flash-image for image edits."
        )
    image_config = _openrouter_image_config(size)
    parameters: dict[str, object] = {"sampleCount": 1}
    if image_config.get("image_size") in {"1K", "2K"}:
        parameters["imageSize"] = image_config["image_size"]
    if image_config.get("aspect_ratio") in {"1:1", "3:4", "4:3", "9:16", "16:9"}:
        parameters["aspectRatio"] = image_config["aspect_ratio"]
    response_payload = _request_gemini_json(
        f"https://generativelanguage.googleapis.com/v1beta/models/{_gemini_model_name(model)}:predict",
        api_key=api_key,
        payload={
            "instances": [{"prompt": prompt}],
            "parameters": parameters,
        },
    )
    predictions = response_payload.get("predictions")
    first = predictions[0] if isinstance(predictions, list) and predictions else None
    encoded = None
    if isinstance(first, dict):
        encoded = first.get("bytesBase64Encoded") or first.get("bytes_base64_encoded")
    if not isinstance(encoded, str) or not encoded:
        raise RuntimeError("Gemini Imagen returned no image.")
    output = Path(output_path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(base64.b64decode(encoded))
    return {
        "path": str(output),
        "provider": f"gemini:{model}",
        "size": size,
    }


def _gemini_native_image_generate(
    api_key: str,
    model: str,
    prompt: str,
    output_path: str,
    size: str | None,
    *,
    source_path: str | None = None,
) -> dict[str, object]:
    parts: list[dict[str, object]] = [{"text": prompt}]
    if source_path:
        source = Path(source_path).expanduser()
        media_type = mimetypes.guess_type(str(source))[0] or "image/png"
        parts.append(
            {
                "inline_data": {
                    "mime_type": media_type,
                    "data": base64.b64encode(source.read_bytes()).decode("ascii"),
                }
            }
        )
    image_config = _openrouter_image_config(size)
    generation_config: dict[str, object] = {"responseModalities": ["TEXT", "IMAGE"]}
    if image_config:
        generation_config["imageConfig"] = {
            **({"imageSize": image_config["image_size"]} if "image_size" in image_config else {}),
            **({"aspectRatio": image_config["aspect_ratio"]} if "aspect_ratio" in image_config else {}),
        }
    response_payload = _request_gemini_json(
        f"https://generativelanguage.googleapis.com/v1beta/models/{_gemini_model_name(model)}:generateContent",
        api_key=api_key,
        payload={
            "contents": [{"role": "user", "parts": parts}],
            "generationConfig": generation_config,
        },
    )
    candidates = response_payload.get("candidates")
    first_candidate = candidates[0] if isinstance(candidates, list) and candidates else None
    content = first_candidate.get("content") if isinstance(first_candidate, dict) else None
    response_parts = content.get("parts") if isinstance(content, dict) else None
    encoded = None
    if isinstance(response_parts, list):
        for part in response_parts:
            if not isinstance(part, dict):
                continue
            inline_data = part.get("inlineData") or part.get("inline_data")
            if isinstance(inline_data, dict) and isinstance(inline_data.get("data"), str):
                encoded = inline_data["data"]
                break
    if not encoded:
        raise RuntimeError(
            "Gemini image generation returned no image. Confirm the configured model supports image output."
        )
    output = Path(output_path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(base64.b64decode(encoded))
    return {
        "path": str(output),
        "provider": f"gemini:{model}",
        "size": size,
    }


def _gemini_image_generate(
    api_key: str,
    model: str,
    prompt: str,
    output_path: str,
    size: str | None,
    *,
    source_path: str | None = None,
) -> dict[str, object]:
    model_l = _gemini_model_name(model).lower()
    if model_l.startswith("imagen-"):
        return _gemini_imagen_generate(
            api_key,
            model,
            prompt,
            output_path,
            size,
            source_path=source_path,
        )
    return _gemini_native_image_generate(
        api_key,
        model,
        prompt,
        output_path,
        size,
        source_path=source_path,
    )


def _model_image_generate(
    provider: str,
    model: str,
    prompt: str,
    output_path: str,
    size: str | None,
    *,
    source_path: str | None = None,
) -> dict[str, object]:
    provider_l = provider.strip().lower()
    api_key = _provider_key_for_media(provider)
    if not api_key:
        raise RuntimeError(f"{provider} image generation requires an API key")
    if provider_l in {"openrouter", "openrouter-key"}:
        return _openrouter_image_generate(
            api_key,
            model,
            prompt,
            output_path,
            size,
            source_path=source_path,
        )
    if provider_l == "gemini":
        return _gemini_image_generate(
            api_key,
            model,
            prompt,
            output_path,
            size,
            source_path=source_path,
        )
    try:
        from openai import OpenAI
    except Exception as exc:  # pragma: no cover - optional dependency guard
        raise RuntimeError("openai package is required for provider-backed image generation") from exc
    provider_base_urls = {
        "gemini": "https://generativelanguage.googleapis.com/v1beta/openai/",
        "groq": "https://api.groq.com/openai/v1",
        "mistral": "https://api.mistral.ai/v1",
        "deepseek": "https://api.deepseek.com/v1",
        "xai": "https://api.x.ai/v1",
        "together": "https://api.together.xyz/v1",
    }
    base_url = provider_base_urls.get(provider_l)
    if provider_l == "custom":
        base_url = os.environ.get("NULLION_MEDIA_CUSTOM_BASE_URL", "").strip()
        if not base_url:
            raise RuntimeError("custom image generation requires NULLION_MEDIA_CUSTOM_BASE_URL")
    client = OpenAI(api_key=api_key, base_url=base_url or None)
    source_image = Path(source_path).expanduser() if source_path else None
    if source_image is not None:
        with source_image.open("rb") as image_file:
            response = client.images.edit(
                model=model,
                image=image_file,
                prompt=prompt,
                size=size or "1024x1024",
            )
    else:
        response = client.images.generate(
            model=model,
            prompt=prompt,
            size=size or "1024x1024",
        )
    data = getattr(response, "data", None)
    first = data[0] if isinstance(data, list) and data else None
    b64_json = getattr(first, "b64_json", None) if first is not None else None
    url = getattr(first, "url", None) if first is not None else None
    output = Path(output_path).expanduser()
    if isinstance(b64_json, str) and b64_json:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(base64.b64decode(b64_json))
    elif isinstance(url, str) and url:
        _write_image_url_to_path(url, output)
    else:
        raise RuntimeError("image generation provider returned no image")
    return {
        "path": str(output),
        "provider": f"{provider}:{model}",
        "size": size,
    }



def _google_workspace_himalaya_account_args(principal_id: str | None) -> list[str]:
    from nullion.connections import require_workspace_connection_for_principal

    connection = require_workspace_connection_for_principal(principal_id, "google_workspace_provider")
    if connection is None or not connection.provider_profile:
        return []
    return ["--account", connection.provider_profile]


def _google_workspace_email_search(
    query: str,
    limit: int,
    *,
    principal_id: str | None = None,
) -> list[dict[str, object]]:
    if shutil.which("himalaya") is None:
        raise RuntimeError("google_workspace_provider requires himalaya to be installed")
    completed = subprocess.run(
        [
            "himalaya",
            "envelope",
            "list",
            "--output",
            "json",
            *_google_workspace_himalaya_account_args(principal_id),
            "--page-size",
            str(limit),
            "subject",
            query,
            "or",
            "from",
            query,
            "or",
            "body",
            query,
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    if completed.returncode != 0:
        failure_output = (completed.stderr or completed.stdout).strip() or "email search failed"
        raise RuntimeError(failure_output)
    stdout = completed.stdout.strip()
    if not stdout:
        return []
    payload = json.loads(stdout)
    if not isinstance(payload, list):
        raise RuntimeError("google_workspace_provider returned non-list envelope payload")
    return payload



def _google_workspace_email_read(
    message_id: str,
    *,
    principal_id: str | None = None,
) -> dict[str, object]:
    if shutil.which("himalaya") is None:
        raise RuntimeError("google_workspace_provider requires himalaya to be installed")
    completed = subprocess.run(
        [
            "himalaya",
            "message",
            "read",
            "--output",
            "json",
            *_google_workspace_himalaya_account_args(principal_id),
            message_id,
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    if completed.returncode != 0:
        failure_output = (completed.stderr or completed.stdout).strip() or "email read failed"
        raise RuntimeError(failure_output)
    stdout = completed.stdout.strip()
    if not stdout:
        raise RuntimeError("google_workspace_provider returned empty email payload")
    payload = json.loads(stdout)
    if not isinstance(payload, dict):
        raise RuntimeError("google_workspace_provider returned non-object email payload")
    return payload



def _google_workspace_calendar_list(start: str, end: str, max_results: int) -> list[dict[str, object]]:
    if not _GOOGLE_WORKSPACE_SCRIPT.exists():
        raise RuntimeError("google_workspace_provider requires local google_api.py wrapper")
    completed = subprocess.run(
        [
            sys.executable,
            str(_GOOGLE_WORKSPACE_SCRIPT),
            "calendar",
            "list",
            "--start",
            start,
            "--end",
            end,
            "--max",
            str(max_results),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    if completed.returncode != 0:
        failure_output = (completed.stderr or completed.stdout).strip() or "calendar list failed"
        raise RuntimeError(failure_output)
    stdout = completed.stdout.strip()
    if not stdout:
        return []
    payload = json.loads(stdout)
    if not isinstance(payload, list):
        raise RuntimeError("google_workspace_provider returned non-list calendar payload")
    return payload


def _custom_api_connection(principal_id: str | None):
    from nullion.connections import require_workspace_connection_for_principal

    return require_workspace_connection_for_principal(principal_id, "custom_api_provider")


def _custom_api_base_url(connection: object | None) -> str:
    profile = getattr(connection, "provider_profile", None)
    if isinstance(profile, str) and profile.strip().startswith(("http://", "https://")):
        return profile.strip().rstrip("/")
    return _require_env("NULLION_CUSTOM_API_BASE_URL", provider_name="custom_api_provider").rstrip("/")


def _custom_api_token(connection: object | None) -> str:
    credential_ref = getattr(connection, "credential_ref", None) or getattr(connection, "provider_profile", None)
    candidates: list[str] = []
    if isinstance(credential_ref, str) and credential_ref.strip():
        ref = credential_ref.strip()
        candidates.append(ref.removeprefix("env:"))
    candidates.append("NULLION_CUSTOM_API_TOKEN")
    for name in candidates:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    labels = " or ".join(dict.fromkeys(candidates))
    raise RuntimeError(f"custom_api_provider requires {labels}")


def _custom_api_headers(connection: object | None) -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {_custom_api_token(connection)}",
    }


def _custom_api_email_search(
    query: str,
    limit: int,
    *,
    principal_id: str | None = None,
) -> list[dict[str, object]]:
    connection = _custom_api_connection(principal_id)
    base_url = _custom_api_base_url(connection)
    payload = _request_json(
        f"{base_url}/email/search?{urlencode({'q': query, 'limit': str(_clamped_limit(limit))})}",
        headers=_custom_api_headers(connection),
    )
    rows = _extract_rows(payload, provider_name="custom_api_provider")
    return [row for row in rows if isinstance(row, dict)]


def _custom_api_email_read(
    message_id: str,
    *,
    principal_id: str | None = None,
) -> dict[str, object]:
    connection = _custom_api_connection(principal_id)
    base_url = _custom_api_base_url(connection)
    payload = _request_json(
        f"{base_url}/email/read/{quote(message_id, safe='')}",
        headers=_custom_api_headers(connection),
    )
    message = payload.get("message", payload)
    if not isinstance(message, dict):
        raise RuntimeError("custom_api_provider returned non-object email payload")
    return message


def _env_key_from_reference(value: object, *, default: str = "ACCOUNT") -> str:
    text = re.sub(r"[^A-Z0-9_]+", "_", str(value or "").strip().upper()).strip("_")
    return text or default


def _imap_smtp_connection(principal_id: str | None):
    from nullion.connections import require_workspace_connection_for_principal

    return require_workspace_connection_for_principal(principal_id, "imap_smtp_provider")


def _imap_env_prefix(connection: object | None) -> str:
    ref = getattr(connection, "credential_ref", None) or getattr(connection, "provider_profile", None)
    return f"NULLION_IMAP_{_env_key_from_reference(ref)}"


def _imap_required_env(prefix: str, name: str) -> str:
    env_name = f"{prefix}_{name}"
    value = os.environ.get(env_name, "").strip()
    if not value:
        raise RuntimeError(f"imap_smtp_provider requires {env_name}")
    return value


def _imap_connect(connection: object | None) -> imaplib.IMAP4:
    prefix = _imap_env_prefix(connection)
    host = _imap_required_env(prefix, "HOST")
    username = _imap_required_env(prefix, "USERNAME")
    password = _imap_required_env(prefix, "PASSWORD")
    raw_port = os.environ.get(f"{prefix}_PORT", "").strip()
    port = int(raw_port) if raw_port else 993
    use_ssl = os.environ.get(f"{prefix}_SSL", "true").strip().lower() not in {"0", "false", "no", "off"}
    client: imaplib.IMAP4
    if use_ssl:
        client = imaplib.IMAP4_SSL(host, port)
    else:
        client = imaplib.IMAP4(host, port)
    client.login(username, password)
    return client


def _imap_select_mailbox(client: imaplib.IMAP4, connection: object | None, *, readonly: bool = True) -> None:
    prefix = _imap_env_prefix(connection)
    mailbox = os.environ.get(f"{prefix}_MAILBOX", "INBOX").strip() or "INBOX"
    status, _ = client.select(mailbox, readonly=readonly)
    if status != "OK":
        raise RuntimeError(f"imap_smtp_provider could not select mailbox {mailbox}")


def _message_addresses(message: email.message.EmailMessage, header_name: str) -> str:
    value = message.get(header_name, "")
    return str(value or "")


def _message_body(message: email.message.EmailMessage) -> str:
    if message.is_multipart():
        html_fallback = ""
        for part in message.walk():
            content_disposition = str(part.get("Content-Disposition", "")).lower()
            if "attachment" in content_disposition:
                continue
            content_type = part.get_content_type()
            if content_type == "text/plain":
                try:
                    return str(part.get_content()).strip()
                except Exception:
                    payload = part.get_payload(decode=True) or b""
                    return payload.decode(part.get_content_charset() or "utf-8", errors="replace").strip()
            if content_type == "text/html" and not html_fallback:
                try:
                    html_fallback = str(part.get_content()).strip()
                except Exception:
                    payload = part.get_payload(decode=True) or b""
                    html_fallback = payload.decode(part.get_content_charset() or "utf-8", errors="replace").strip()
        return html_fallback
    try:
        return str(message.get_content()).strip()
    except Exception:
        payload = message.get_payload(decode=True) or b""
        return payload.decode(message.get_content_charset() or "utf-8", errors="replace").strip()


def _message_to_summary(message_id: str, message: email.message.EmailMessage) -> dict[str, object]:
    return {
        "id": message_id,
        "subject": str(message.get("Subject", "")),
        "from": _message_addresses(message, "From"),
        "to": _message_addresses(message, "To"),
        "date": str(message.get("Date", "")),
    }


def _message_to_detail(message_id: str, message: email.message.EmailMessage) -> dict[str, object]:
    attachments: list[dict[str, object]] = []
    if message.is_multipart():
        for part in message.walk():
            filename = part.get_filename()
            if filename:
                attachments.append(
                    {
                        "filename": filename,
                        "content_type": part.get_content_type(),
                    }
                )
    detail = _message_to_summary(message_id, message)
    detail.update(
        {
            "cc": _message_addresses(message, "Cc"),
            "body": _message_body(message),
            "attachments": attachments,
        }
    )
    return detail


def _imap_fetch_message(client: imaplib.IMAP4, message_id: str, fetch_spec: str) -> email.message.EmailMessage:
    status, data = client.uid("FETCH", message_id, fetch_spec)
    if status != "OK":
        raise RuntimeError(f"imap_smtp_provider could not fetch message {message_id}")
    for item in data:
        if isinstance(item, tuple) and isinstance(item[1], (bytes, bytearray)):
            return email.message_from_bytes(bytes(item[1]), policy=policy.default)
    raise RuntimeError(f"imap_smtp_provider returned empty message {message_id}")


def _imap_search_ids(client: imaplib.IMAP4, query: str) -> list[str]:
    attempts = (
        ("SEARCH", "CHARSET", "UTF-8", "TEXT", query),
        ("SEARCH", "TEXT", query),
        ("SEARCH", "ALL"),
    )
    for args in attempts:
        status, data = client.uid(*args)
        if status == "OK":
            raw_ids = data[0] if data else b""
            if isinstance(raw_ids, bytes):
                return [item.decode("ascii", errors="ignore") for item in raw_ids.split() if item]
            if isinstance(raw_ids, str):
                return [item for item in raw_ids.split() if item]
    raise RuntimeError("imap_smtp_provider email search failed")


def _imap_smtp_email_search(
    query: str,
    limit: int,
    *,
    principal_id: str | None = None,
) -> list[dict[str, object]]:
    connection = _imap_smtp_connection(principal_id)
    client = _imap_connect(connection)
    try:
        _imap_select_mailbox(client, connection, readonly=True)
        message_ids = list(reversed(_imap_search_ids(client, query)))[: _clamped_limit(limit)]
        results: list[dict[str, object]] = []
        for message_id in message_ids:
            message = _imap_fetch_message(client, message_id, "(BODY.PEEK[HEADER])")
            results.append(_message_to_summary(message_id, message))
        return results
    finally:
        try:
            client.logout()
        except Exception:
            pass


def _imap_smtp_email_read(
    message_id: str,
    *,
    principal_id: str | None = None,
) -> dict[str, object]:
    connection = _imap_smtp_connection(principal_id)
    client = _imap_connect(connection)
    try:
        _imap_select_mailbox(client, connection, readonly=True)
        message = _imap_fetch_message(client, message_id, "(RFC822)")
        return _message_to_detail(message_id, message)
    finally:
        try:
            client.logout()
        except Exception:
            pass



def resolve_plugin_provider_kwargs(*, plugin_name: str, provider_name: str) -> dict[str, object]:
    if plugin_name == "search_plugin":
        if provider_name == "builtin_search_provider":
            return {
                "web_fetcher": fetch_url_snapshot,
                "web_searcher": search_web,
            }
        if provider_name == "brave_search_provider":
            return {
                "web_fetcher": fetch_url_snapshot,
                "web_searcher": _brave_web_search,
            }
        if provider_name in {"google_custom_search_provider", "google_search_provider"}:
            return {
                "web_fetcher": fetch_url_snapshot,
                "web_searcher": _google_custom_search,
            }
        if provider_name == "perplexity_search_provider":
            return {
                "web_fetcher": fetch_url_snapshot,
                "web_searcher": _perplexity_search,
            }
        if provider_name in {"duckduckgo_instant_answer_provider", "duckduckgo_search_provider"}:
            return {
                "web_fetcher": fetch_url_snapshot,
                "web_searcher": _duckduckgo_instant_answer_search,
            }
        raise ValueError(f"unknown provider binding for search_plugin: {provider_name}")
    if plugin_name == "email_plugin":
        if provider_name == "google_workspace_provider":
            return {
                "email_searcher": _google_workspace_email_search,
                "email_reader": _google_workspace_email_read,
            }
        if provider_name == "custom_api_provider":
            return {
                "email_searcher": _custom_api_email_search,
                "email_reader": _custom_api_email_read,
            }
        if provider_name == "imap_smtp_provider":
            return {
                "email_searcher": _imap_smtp_email_search,
                "email_reader": _imap_smtp_email_read,
            }
        raise ValueError(f"unknown provider binding for email_plugin: {provider_name}")
    if plugin_name == "calendar_plugin":
        if provider_name == "google_workspace_provider":
            return {
                "calendar_lister": _google_workspace_calendar_list,
            }
        raise ValueError(f"unknown provider binding for calendar_plugin: {provider_name}")
    if plugin_name == "media_plugin":
        if provider_name == "local_media_provider":
            return {
                "audio_transcriber": _local_audio_transcribe,
                "image_text_extractor": _local_image_extract_text,
                "image_generator": _local_image_generate,
            }
        raise ValueError(f"unknown provider binding for media_plugin: {provider_name}")
    raise ValueError(f"unsupported plugin provider resolution: {plugin_name}")


__all__ = ["resolve_plugin_provider_kwargs"]
