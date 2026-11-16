"""Own transient tool resources across all recovery passes of one request."""
from contextvars import ContextVar
from functools import wraps
import logging
from uuid import uuid4

logger = logging.getLogger(__name__)
_scope = ContextVar("request_resource_scope", default=None)


def current_request_resource_scope() -> str | None:
    return _scope.get()


def request_resource_scope(function):
    """Release request-owned resources on every exit, independently of Code Mode."""
    @wraps(function)
    def wrapped(runtime, *args, **kwargs):
        if _scope.get() is not None:
            return function(runtime, *args, **kwargs)
        owner = "request:" + uuid4().hex
        token = _scope.set(owner)
        try:
            return function(runtime, *args, **kwargs)
        finally:
            try:
                registry = getattr(runtime, "active_tool_registry", None)
                cleanup = getattr(registry, "run_cleanup_hooks", None)
                if callable(cleanup):
                    cleanup(scope_id=owner)
            except Exception:
                logger.warning("Request resource cleanup failed for %s", owner, exc_info=True)
            finally:
                _scope.reset(token)
    return wrapped
