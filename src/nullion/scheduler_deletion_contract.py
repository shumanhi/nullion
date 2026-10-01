"""Check user authorization before a chat agent proposes scheduler deletion."""

from __future__ import annotations

import json
from typing import Mapping

from nullion.tools import ToolInvocation, ToolResult


def scheduler_deletion_guard_result(
    invocation: ToolInvocation, *, user_message: str, model_client: object | None,
) -> ToolResult | None:
    # Gate the extra model decision on an actual destructive tool candidate.
    # Scope expansion is a capability request, not evidence of user consent.
    if invocation.tool_name != "delete_cron":
        return None
    target_id = str(invocation.arguments.get("id") or "").strip()
    if not target_id:
        return _denied(invocation, "scheduler_delete_target_missing")
    from nullion.connections import workspace_id_for_principal

    from nullion.crons import get_cron
    target = get_cron(target_id)
    target_name = str(getattr(target, "name", "") or "")
    if target is None or getattr(target, "workspace_id", "workspace_admin") != workspace_id_for_principal(invocation.principal_id):
        return _denied(invocation, "scheduler_delete_target_unverified")
    if model_client is None or not callable(getattr(model_client, "create", None)):
        return _denied(invocation, "scheduler_delete_authorization_unavailable")
    try:
        response = model_client.create(
            messages=[{"role": "user", "content": [{"type": "text", "text": json.dumps({
                "user_request": user_message,
                "candidate_action": invocation.tool_name,
                "target": {"id": target_id, "name": target_name},
            }, ensure_ascii=False)}]}],
            tools=[], max_tokens=400,
            system=(
                'Return only JSON matching {"authorized":false,"target_id":"", "user_evidence":""}. '
                'Determine whether the user request explicitly authorizes deleting this verified scheduled object. '
                'A complaint about output, an edit, inspection, rerun, or repair does not authorize deletion, '
                'including deletion as an intermediate step. Do not infer authorization from the candidate action. '
                'Set authorized true only for an unambiguous user deletion request for this target; '
                'echo its exact target_id and an exact supporting quote from user_request. '
                'Ambiguous target references must return false. Interpret any user language.'
            ),
        )
        content = response.get("content", []) if isinstance(response, Mapping) else []
        text = content if isinstance(content, str) else "".join(
            str(block.get("text") or "") for block in content
            if isinstance(block, Mapping) and block.get("type") == "text"
        )
        decision = json.loads(text)
        evidence = decision.get("user_evidence") if isinstance(decision, dict) else None
        if not (
            isinstance(decision, dict) and decision.get("authorized") is True
            and decision.get("target_id") == target_id
            and isinstance(evidence, str) and evidence.strip() and evidence in user_message
        ):
            return _denied(invocation, "scheduler_delete_not_user_authorized")
    except Exception:
        return _denied(invocation, "scheduler_delete_authorization_unavailable")
    return None


def _denied(invocation: ToolInvocation, reason: str) -> ToolResult:
    return ToolResult(
        invocation.invocation_id, invocation.tool_name, "failed",
        {"reason": reason, "target_id": invocation.arguments.get("id")},
        "Deletion was blocked because the current user request does not verify deletion of this scheduled object. "
        "Continue inspecting or repairing it; do not request deletion approval.",
    )
