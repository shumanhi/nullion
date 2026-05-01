"""nullion auth — interactive credential setup.

Stores credentials encrypted in ~/.nullion/runtime.db.
Run: uv run nullion-auth   (from the project directory)
"""
from __future__ import annotations

import getpass
import http.server
import argparse
import contextlib
import json
import socket
import sys
import threading
import time
import urllib.parse
import urllib.request
import webbrowser
from functools import lru_cache
from pathlib import Path
from typing import Any, TypedDict

from nullion.entrypoint_guard import run_user_facing_entrypoint
from langgraph.graph import END, START, StateGraph

import httpx
from nullion.credential_store import (
    load_encrypted_credentials,
    migrate_credentials_json_to_db,
    save_encrypted_credentials,
)

CREDENTIALS_PATH = Path.home() / ".nullion" / "credentials.json"

# ── Provider menu ─────────────────────────────────────────────────────────────

_OAUTH_PROVIDERS = [
    {
        "id": "openrouter",
        "label": "OpenRouter  (GPT, Gemini, Llama, Claude & 100+ models)",
        "kind": "oauth",
    },
    {
        "id": "codex",
        "label": "ChatGPT / OpenAI Codex  (your chatgpt.com account)",
        "kind": "oauth",
    },
]

_APIKEY_PROVIDERS = [
    {
        "id": "openai",
        "label": "OpenAI  [recommended]",
        "base_url": None,
        "default_model": "gpt-5.5",
        "key_hint": "sk-...",
    },
    {
        "id": "anthropic",
        "label": "Anthropic",
        "base_url": "https://api.anthropic.com/v1",
        "default_model": "claude-opus-4-6",
        "key_hint": "sk-ant-...",
    },
    {
        "id": "gemini",
        "label": "Google Gemini",
        "base_url": "https://generativelanguage.googleapis.com/v1beta/openai/",
        "default_model": "models/gemini-2.5-flash",
        "key_hint": "AIza...",
    },
    {
        "id": "openrouter",
        "label": "OpenRouter  (API key)",
        "base_url": "https://openrouter.ai/api/v1",
        "default_model": "openai/gpt-4o",
        "key_hint": "sk-or-v1-...",
    },
    {
        "id": "ollama",
        "label": "Ollama local  (OpenAI-compatible endpoint)",
        "base_url": "http://127.0.0.1:11434/v1",
        "default_model": "llama3.3",
        "key_hint": "local endpoint; any non-empty key works",
        "optional_key": True,
    },
    {
        "id": "groq",
        "label": "Groq",
        "base_url": "https://api.groq.com/openai/v1",
        "default_model": "llama-3.3-70b-versatile",
        "key_hint": "gsk_...",
    },
    {
        "id": "mistral",
        "label": "Mistral",
        "base_url": "https://api.mistral.ai/v1",
        "default_model": "mistral-large-latest",
        "key_hint": "...",
    },
    {
        "id": "deepseek",
        "label": "DeepSeek",
        "base_url": "https://api.deepseek.com/v1",
        "default_model": "deepseek-chat",
        "key_hint": "sk-...",
    },
    {
        "id": "xai",
        "label": "xAI",
        "base_url": "https://api.x.ai/v1",
        "default_model": "grok-4",
        "key_hint": "xai-...",
    },
    {
        "id": "together",
        "label": "Together AI",
        "base_url": "https://api.together.xyz/v1",
        "default_model": "meta-llama/Llama-3.3-70B-Instruct-Turbo",
        "key_hint": "...",
    },
    {
        "id": "custom",
        "label": "Custom / self-hosted  (vLLM, LM Studio, LiteLLM, …)",
        "base_url": None,
        "default_model": None,
        "key_hint": None,
    },
]


# ── UI helpers ─────────────────────────────────────────────────────────────────

def _banner() -> None:
    print()
    print("  Nullion — LLM provider setup")
    print("  ─────────────────────────────")
    print()


def _prompt(label: str, default: str | None = None, secret: bool = False) -> str:
    suffix = f" [{default}]" if default else ""
    prompt_str = f"  {label}{suffix}: "
    while True:
        value = (getpass.getpass(prompt_str) if secret else input(prompt_str)).strip()
        if value:
            return value
        if default is not None:
            return default
        print("  (required)")


def _pick_provider() -> dict[str, Any]:
    print("  ── Browser login (no API key needed)  ────────────────")
    for i, p in enumerate(_OAUTH_PROVIDERS, 1):
        print(f"    {i}. {p['label']}")
    print()
    offset = len(_OAUTH_PROVIDERS)
    print("  ── API key  ──────────────────────────────────────────")
    for i, p in enumerate(_APIKEY_PROVIDERS, offset + 1):
        print(f"    {i}. {p['label']}")
    print()
    total = offset + len(_APIKEY_PROVIDERS)
    while True:
        raw = input(f"  Your choice [1-{total}]: ").strip()
        try:
            idx = int(raw)
            if 1 <= idx <= offset:
                return {"kind": "oauth", **_OAUTH_PROVIDERS[idx - 1]}
            if offset < idx <= total:
                return {"kind": "api_key", **_APIKEY_PROVIDERS[idx - offset - 1]}
        except ValueError:
            pass
        print(f"  Please enter a number between 1 and {total}.")


# ── Codex device-code OAuth ───────────────────────────────────────────────────

_CODEX_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
_CODEX_ISSUER = "https://auth.openai.com"
_CODEX_VERIFY_URL = "https://auth.openai.com/codex/device"
_CODEX_BASE_URL = "https://api.openai.com/v1"


def _codex_device_code_oauth() -> tuple[str, str]:
    """Device-code OAuth for ChatGPT / OpenAI Codex. Returns (access_token, refresh_token)."""
    with httpx.Client(timeout=httpx.Timeout(15.0)) as client:
        try:
            resp = client.post(
                f"{_CODEX_ISSUER}/api/accounts/deviceauth/usercode",
                json={"client_id": _CODEX_CLIENT_ID},
                headers={"Content-Type": "application/json"},
            )
        except Exception as exc:
            print(f"  Failed to contact OpenAI: {exc}")
            sys.exit(1)

        if resp.status_code != 200:
            print(f"  OpenAI device auth returned HTTP {resp.status_code}.")
            print("  To use OpenAI directly, choose the OpenAI API key option instead.")
            sys.exit(1)

        device_data = resp.json()
        user_code = device_data.get("user_code", "")
        device_auth_id = device_data.get("device_auth_id", "")
        poll_interval = max(3, int(device_data.get("interval", 5)))

        if not user_code or not device_auth_id:
            print("  Unexpected response from OpenAI (missing user_code or device_auth_id).")
            sys.exit(1)

        print()
        print(f"  1. Open: {_CODEX_VERIFY_URL}")
        print(f"  2. Enter code: \033[1m{user_code}\033[0m")
        print()

        if not webbrowser.open(_CODEX_VERIFY_URL):
            print("  (Browser did not open automatically — visit the URL above)")

        print("  Waiting for you to approve… (Ctrl+C to cancel)")

        deadline = time.monotonic() + 15 * 60
        code_resp = None
        while time.monotonic() < deadline:
            time.sleep(poll_interval)
            try:
                poll = client.post(
                    f"{_CODEX_ISSUER}/api/accounts/deviceauth/token",
                    json={"device_auth_id": device_auth_id, "user_code": user_code},
                    headers={"Content-Type": "application/json"},
                )
            except Exception as exc:
                print(f"  Poll error: {exc}")
                sys.exit(1)

            if poll.status_code == 200:
                code_resp = poll.json()
                break
            if poll.status_code in (403, 404):
                continue
            print(f"  Unexpected poll status {poll.status_code}.")
            sys.exit(1)

        if code_resp is None:
            print("  Timed out after 15 minutes.")
            sys.exit(1)

        authorization_code = code_resp.get("authorization_code", "")
        code_verifier = code_resp.get("code_verifier", "")
        redirect_uri = f"{_CODEX_ISSUER}/deviceauth/callback"

        if not authorization_code or not code_verifier:
            print("  Device auth response missing authorization_code or code_verifier.")
            sys.exit(1)

        try:
            token_resp = client.post(
                f"{_CODEX_ISSUER}/oauth/token",
                data={
                    "grant_type": "authorization_code",
                    "code": authorization_code,
                    "redirect_uri": redirect_uri,
                    "client_id": _CODEX_CLIENT_ID,
                    "code_verifier": code_verifier,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
        except Exception as exc:
            print(f"  Token exchange failed: {exc}")
            sys.exit(1)

        if token_resp.status_code != 200:
            print(f"  Token exchange returned HTTP {token_resp.status_code}.")
            sys.exit(1)

        tokens = token_resp.json()
        access_token = tokens.get("access_token", "")
        refresh_token = tokens.get("refresh_token", "")

        if not access_token:
            print("  OpenAI did not return an access_token.")
            sys.exit(1)

        return access_token, refresh_token


# ── OpenRouter OAuth ───────────────────────────────────────────────────────────

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def _openrouter_oauth() -> str:
    """Browser OAuth for OpenRouter. Returns an API key."""
    port = _free_port()
    callback_url = f"http://localhost:{port}/callback"
    auth_url = "https://openrouter.ai/auth?" + urllib.parse.urlencode({"callback_url": callback_url})

    result: dict[str, Any] = {}
    event = threading.Event()

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query)
            code = (params.get("code") or [""])[0]
            if code:
                result["code"] = code
                body = b"<html><body style='font-family:sans-serif;padding:2em'><h2>Done! Return to your terminal.</h2></body></html>"
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(400)
                self.end_headers()
            event.set()

        def log_message(self, *args: Any) -> None:
            pass

    server = http.server.HTTPServer(("localhost", port), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    print()
    print("  Opening your browser…")
    if not webbrowser.open(auth_url):
        print(f"  Could not open browser automatically. Visit:")
        print(f"  {auth_url}")
    print("  Waiting for login to complete…")

    if not event.wait(timeout=300):
        server.shutdown()
        print("  Timed out after 5 minutes.")
        sys.exit(1)

    server.shutdown()
    code = result.get("code", "")
    if not code:
        print("  No auth code received.")
        sys.exit(1)

    # Exchange code for API key
    print("  Exchanging code for API key…")
    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/auth/keys",
        data=json.dumps({"code": code}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = json.loads(resp.read())
    except Exception as exc:
        print(f"  Failed to exchange code: {exc}")
        sys.exit(1)

    api_key = payload.get("key", "")
    if not api_key:
        print("  OpenRouter did not return an API key.")
        sys.exit(1)

    return api_key


# ── API key flow ───────────────────────────────────────────────────────────────

def _collect_api_key(provider: dict[str, Any]) -> dict[str, Any]:
    print()
    creds: dict[str, Any] = {"provider": provider["id"]}

    if provider["id"] == "custom":
        creds["base_url"] = _prompt("Base URL (e.g. http://localhost:11434/v1)")
        creds["model"] = _prompt("Model name")
        use_key = input("  API key required? [y/N]: ").strip().lower()
        creds["api_key"] = _prompt("API key", secret=True) if use_key in {"y", "yes"} else "none"
    else:
        if provider.get("key_hint"):
            print(f"  hint: {provider['key_hint']}")
        if provider.get("optional_key"):
            use_key = input("  API key required? [y/N]: ").strip().lower()
            creds["api_key"] = _prompt("API key", secret=True) if use_key in {"y", "yes"} else "ollama-local"
        else:
            creds["api_key"] = _prompt("API key", secret=True)
        if provider.get("base_url"):
            creds["base_url"] = provider["base_url"]
        creds["model"] = _prompt("Model", default=provider["default_model"])

    return creds


# ── Persistence ────────────────────────────────────────────────────────────────

def _save(creds: dict[str, Any]) -> None:
    save_encrypted_credentials(creds, db_path=CREDENTIALS_PATH.with_name("runtime.db"))


def codex_oauth_credentials() -> dict[str, Any]:
    access_token, refresh_token = _codex_device_code_oauth()
    return {
        "provider": "codex",
        "api_key": access_token,
        "refresh_token": refresh_token,
        "base_url": _CODEX_BASE_URL,
        "model": "gpt-5.5",
    }


class _AuthCredentialWorkflowState(TypedDict, total=False):
    mode: str
    provider: dict[str, Any]
    credentials: dict[str, Any]
    existing: dict[str, Any]
    saved_path: Path


def _auth_collect_credentials_node(state: _AuthCredentialWorkflowState) -> dict[str, object]:
    mode = str(state.get("mode") or "setup")
    provider = state.get("provider") or {}
    if mode == "reauth_codex":
        return {"credentials": codex_oauth_credentials()}
    if provider.get("kind") == "oauth" and provider.get("id") == "openrouter":
        return {
            "credentials": {
                "provider": "openrouter",
                "api_key": _openrouter_oauth(),
                "base_url": "https://openrouter.ai/api/v1",
                "model": "openai/gpt-4o",
            }
        }
    if provider.get("kind") == "oauth" and provider.get("id") == "codex":
        return {"credentials": codex_oauth_credentials()}
    return {"credentials": _collect_api_key(provider)}


def _auth_merge_credentials_node(state: _AuthCredentialWorkflowState) -> dict[str, object]:
    mode = str(state.get("mode") or "setup")
    new_creds = dict(state.get("credentials") or {})
    if mode != "reauth_codex":
        return {"credentials": new_creds}
    existing = load_stored_credentials() or {}
    creds = {**existing, **new_creds}
    keys = creds.get("keys")
    if not isinstance(keys, dict):
        keys = {}
    keys["codex"] = new_creds["api_key"]
    creds["keys"] = keys
    models = creds.get("models")
    if not isinstance(models, dict):
        models = {}
    models.setdefault("codex", str(existing.get("model") or new_creds["model"]))
    creds["models"] = models
    return {"credentials": creds, "existing": existing}


def _auth_save_credentials_node(state: _AuthCredentialWorkflowState) -> dict[str, object]:
    creds = dict(state.get("credentials") or {})
    _save(creds)
    return {"saved_path": CREDENTIALS_PATH.with_name("runtime.db")}


@lru_cache(maxsize=1)
def _compiled_auth_credential_workflow_graph():
    graph = StateGraph(_AuthCredentialWorkflowState)
    graph.add_node("collect", _auth_collect_credentials_node)
    graph.add_node("merge", _auth_merge_credentials_node)
    graph.add_node("save", _auth_save_credentials_node)
    graph.add_edge(START, "collect")
    graph.add_edge("collect", "merge")
    graph.add_edge("merge", "save")
    graph.add_edge("save", END)
    return graph.compile()


def save_credentials_for_provider(provider: dict[str, Any]) -> Path:
    final_state = _compiled_auth_credential_workflow_graph().invoke(
        {"mode": "setup", "provider": provider},
        config={"configurable": {"thread_id": f"auth-setup:{provider.get('id', 'unknown')}"}},
    )
    return final_state.get("saved_path") or CREDENTIALS_PATH.with_name("runtime.db")


def reauthenticate_codex_oauth() -> Path:
    final_state = _compiled_auth_credential_workflow_graph().invoke(
        {"mode": "reauth_codex", "provider": {"id": "codex", "kind": "oauth"}},
        config={"configurable": {"thread_id": "auth-reauth:codex"}},
    )
    saved_path = final_state.get("saved_path") or CREDENTIALS_PATH.with_name("runtime.db")
    print()
    print(f"  Saved Codex OAuth credentials to {saved_path}")
    print()
    return saved_path


def load_stored_credentials() -> dict[str, Any] | None:
    stored = load_encrypted_credentials(db_path=CREDENTIALS_PATH.with_name("runtime.db"))
    if stored:
        return stored
    return migrate_credentials_json_to_db(CREDENTIALS_PATH, db_path=CREDENTIALS_PATH.with_name("runtime.db"))


# ── CLI entry point ────────────────────────────────────────────────────────────

def cli(argv: list[str] | None = None) -> None:
    return run_user_facing_entrypoint(lambda: _cli_impl(argv))


def _cli_impl(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="nullion-auth")
    parser.add_argument("--reauth", choices=("codex",), help="Re-authenticate an existing OAuth provider and exit")
    parser.add_argument(
        "--print-codex-access-token",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--write-codex-access-token",
        metavar="PATH",
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(argv)
    if args.print_codex_access_token:
        with contextlib.redirect_stdout(sys.stderr):
            creds = codex_oauth_credentials()
        print(creds["api_key"])
        return
    if args.write_codex_access_token:
        creds = codex_oauth_credentials()
        token_path = Path(args.write_codex_access_token)
        token_path.write_text(str(creds["api_key"]), encoding="utf-8")
        token_path.chmod(0o600)
        return

    if args.reauth == "codex":
        try:
            _banner()
            reauthenticate_codex_oauth()
            return
        except KeyboardInterrupt:
            print()
            print("  Cancelled.")
            sys.exit(0)

    try:
        _banner()
        chosen = _pick_provider()
        saved_path = save_credentials_for_provider(chosen)
        print()
        print(f"  Saved to {saved_path}")
        print()
        print("  Restart the bot:")
        print("    pkill -f nullion-telegram")
        print("    nullion-telegram --checkpoint runtime.db --env-file .env >> /private/tmp/nullion.log 2>&1 &")
        print()
    except KeyboardInterrupt:
        print()
        print("  Cancelled.")
        sys.exit(0)


if __name__ == "__main__":
    cli()
