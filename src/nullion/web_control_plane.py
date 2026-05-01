"""Minimal read-only web control-plane scaffold for Project Nullion."""

from __future__ import annotations

import json
import secrets
from collections.abc import Callable
from typing import Any

from nullion.runtime import build_runtime_status_snapshot, build_skill_snapshot, list_skills
from nullion.runtime_store import RuntimeStore


StatusHeadersBody = tuple[str, list[tuple[str, str]], bytes]
_WEB_CONTROL_PLANE_WWW_AUTHENTICATE = 'Bearer realm="nullion-web-control-plane"'


def _json_response(status: str, payload: dict[str, Any], *, extra_headers: list[tuple[str, str]] | None = None) -> StatusHeadersBody:
    body = json.dumps(payload).encode("utf-8")
    headers = [
        ("Content-Type", "application/json"),
        ("Content-Length", str(len(body))),
    ]
    if extra_headers:
        headers.extend(extra_headers)
    return status, headers, body


def _unauthorized_response() -> StatusHeadersBody:
    return _json_response(
        "401 Unauthorized",
        {"error": "unauthorized"},
        extra_headers=[("WWW-Authenticate", _WEB_CONTROL_PLANE_WWW_AUTHENTICATE)],
    )


def _is_authorized_request(environ: dict[str, Any], bearer_token: str) -> bool:
    authorization = str(environ.get("HTTP_AUTHORIZATION", "")).strip()
    if not authorization:
        return False
    scheme, separator, token = authorization.partition(" ")
    if not separator or scheme.lower() != "bearer":
        return False
    return secrets.compare_digest(token.strip(), bearer_token)


def create_web_control_plane_app(
    store: RuntimeStore,
    *,
    bearer_token: str,
) -> Callable[[dict[str, Any], Callable[..., Any]], list[bytes]]:
    """Build a tiny WSGI-compatible read-only control plane.

    Endpoints:
    - GET /api/status
    - GET /api/approvals
    - GET /api/grants
    - GET /api/skills
    """

    normalized_bearer_token = bearer_token.strip()
    if not normalized_bearer_token:
        raise ValueError("bearer_token is required")

    def app(environ: dict[str, Any], start_response: Callable[..., Any]) -> list[bytes]:
        if not _is_authorized_request(environ, normalized_bearer_token):
            status, headers, body = _unauthorized_response()
            start_response(status, headers)
            return [body]

        method = str(environ.get("REQUEST_METHOD", "GET")).upper()
        path = str(environ.get("PATH_INFO", "/"))

        if path == "/api/status":
            if method != "GET":
                status, headers, body = _json_response(
                    "405 Method Not Allowed",
                    {"error": "method_not_allowed"},
                    extra_headers=[("Allow", "GET")],
                )
            else:
                status, headers, body = _json_response("200 OK", build_runtime_status_snapshot(store))
        elif path == "/api/approvals":
            if method != "GET":
                status, headers, body = _json_response(
                    "405 Method Not Allowed",
                    {"error": "method_not_allowed"},
                    extra_headers=[("Allow", "GET")],
                )
            else:
                snapshot = build_runtime_status_snapshot(store)
                status, headers, body = _json_response("200 OK", {"approvals": snapshot["approval_requests"]})
        elif path == "/api/grants":
            if method != "GET":
                status, headers, body = _json_response(
                    "405 Method Not Allowed",
                    {"error": "method_not_allowed"},
                    extra_headers=[("Allow", "GET")],
                )
            else:
                snapshot = build_runtime_status_snapshot(store)
                status, headers, body = _json_response("200 OK", {"grants": snapshot["permission_grants"]})
        elif path == "/api/skills":
            if method != "GET":
                status, headers, body = _json_response(
                    "405 Method Not Allowed",
                    {"error": "method_not_allowed"},
                    extra_headers=[("Allow", "GET")],
                )
            else:
                status, headers, body = _json_response(
                    "200 OK",
                    {"skills": [build_skill_snapshot(skill) for skill in list_skills(store)]},
                )
        else:
            status, headers, body = _json_response("404 Not Found", {"error": "not_found"})

        start_response(status, headers)
        return [body]

    return app


__all__ = ["create_web_control_plane_app"]
