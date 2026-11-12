"""Outcome review assists composition; execution and delivery retain ownership.

This is a content handoff to the existing shared renderer, not another style
policy. A critique cannot replace a usable answer with a grading status.
"""
from __future__ import annotations


def has_supported_clarification(review):
    return (review is not None and not getattr(review,'review_error',None)
        and review.disposition=='needs_user_input' and getattr(review,'execution_satisfied',None) is False
        and any(not row.satisfied and (row.evidence_invocation_ids or row.requirement_kind == 'user_input')
                for row in getattr(review,'material_requirements',()))
        and bool(review.clarification_question and review.clarification_options))


def assisted_reply(draft, review, *, artifact_paths=None, prepared_answer=None):
    """Use a grounded improvement, otherwise preserve the producer's answer."""
    if review is None or getattr(review, 'review_error', None):
        return draft
    from nullion.agent_orchestrator import _publishable_review_correction
    original = str(draft or '')
    from nullion.execution_outcome import PreparedAnswer
    owned_answer = (isinstance(prepared_answer, PreparedAnswer)
        and prepared_answer.outcome.user_visible_message == original)
    def retain_delivery(value):
        # Composition edits do not revoke the producer's selected attachments.
        # An explicit current selection supersedes old MEDIA directives; the
        # delivery boundary still validates every path and exact file.
        if artifact_paths is None:
            media = [line.strip() for line in original.splitlines() if line.strip().startswith('MEDIA:')]
        else:
            media = ['MEDIA:' + str(path) for path in artifact_paths]
            value = '\n'.join(line for line in str(value or '').splitlines()
                if not line.strip().startswith('MEDIA:')).strip()
        missing = [line for line in dict.fromkeys(media) if line not in str(value or '').splitlines()]
        return '\n\n'.join(filter(None, (value, '\n'.join(missing)))) if missing else value
    correction = _publishable_review_correction(review)
    if correction:
        return retain_delivery(correction)
    findings = tuple(getattr(review, 'supported_findings', ()) or ())
    # These rows have passed the review owner's current-receipt validation.
    # A replacement needs actual findings, rather than a warning or checklist.
    rows = tuple(getattr(review, 'material_requirements', ()) or ())
    fully_covered = bool(rows) and all(row.satisfied and row.draft_covered for row in rows)
    replace_draft = (not original.strip() or getattr(review, 'source_presentation_valid', None) is False
        or getattr(review, 'runtime_failure_code', None) in {'scope_selection_stalled', 'preflight_revision_stalled'}
        or bool(rows) and all(not row.draft_covered for row in rows))
    if findings and (replace_draft or not fully_covered):
        from nullion.review_publication import render_supported_findings
        if replace_draft:
            detail = render_supported_findings(tuple(row for row in findings
                if getattr(row, 'draft_relation', 'replacement') != 'already_covered'))
            draft = detail or draft
        else:
            # A second rendering of the report is an alternate composition,
            # not additional facts. Only explicitly typed narrow additions or
            # material limitations may supplement an existing usable answer.
            from nullion.chat_text import REPORT_MESSAGE_FORMAT
            additions = tuple(row for row in findings
                if getattr(row, 'draft_relation', 'replacement') != 'already_covered'
                and not getattr(row, 'is_report', False)
                and getattr(row, 'message_format', None) != REPORT_MESSAGE_FORMAT
                and (row.kind == 'limitation' or getattr(row, 'draft_relation', None) == 'additional'))
            detail = render_supported_findings(additions)
        if not replace_draft and detail and detail not in str(draft or ''):
            # Partial extra findings cannot replace the producer's larger
            # answer. Retain its facts while supplying the grounded addition.
            from nullion.chat_text import UserMessage, render_user_message
            draft = render_user_message(UserMessage(paragraphs=tuple(filter(None, (draft, detail)))))
    from nullion.review_publication import comparison_material_gap, operational_failure_notice
    operation_notice = operational_failure_notice(review)
    missing = tuple(row for row in rows if not row.satisfied)
    failure_already_described = bool(missing) and all(row.draft_covered for row in missing)
    if operation_notice and not failure_already_described:
        from nullion.chat_text import UserMessage, render_user_message
        if replace_draft and not findings and not owned_answer:
            draft = operation_notice
        elif operation_notice not in str(draft or ''):
            draft = render_user_message(UserMessage(paragraphs=tuple(filter(None, (draft, operation_notice)))))
    material_gap = comparison_material_gap(review)
    if material_gap and material_gap not in str(draft or '') and not any(row.kind == 'limitation' for row in findings):
        # Optional unvisited candidates are not a warning. An unresolved
        # material shortlist challenger can change the requested recommendation.
        from nullion.chat_text import UserMessage, render_user_message
        draft = render_user_message(UserMessage(paragraphs=tuple(filter(None, (draft, material_gap)))))
    if has_supported_clarification(review):
        # A grounded ambiguity can be explained alongside the result. It does
        # not erase that result or grant an action/source permission.
        from nullion.chat_text import UserMessage,render_user_message
        if not any(row.evidence_invocation_ids for row in rows):
            # A missing user-controlled choice needs no external source read.
            # With no observed findings, publish the validated question alone.
            draft = ''
        return retain_delivery(render_user_message(UserMessage(paragraphs=tuple(filter(None,(draft,review.clarification_question))),
            choices=tuple(review.clarification_options))))
    return retain_delivery(draft)


def record_assistance(presentation, review):
    """Keep critique/availability diagnostic state separate from work status."""
    presentation.update(outcome_review_role='helper',
        outcome_review_status=('unavailable' if review is None or getattr(review, 'review_error', None)
            else 'complete' if review.disposition == 'complete' else 'advice'),
        outcome_review_error=getattr(review, 'review_error', None),
        outcome_review_timeout=bool(getattr(review, 'review_timed_out', False)))
    if has_supported_clarification(review):
        presentation['needs_input']=True


def assisted_fulfillment(producer_fulfilled, review):
    """An editor can confirm a grounded result, but cannot revoke execution."""
    if producer_fulfilled is False:
        return False
    if (review is not None and not getattr(review, 'review_error', None)
            and review.disposition == 'complete' and getattr(review, 'execution_satisfied', None) is True):
        return True
    return producer_fulfilled


def validated_execution_improvement(plan, tasks):
    """A critique is not an execution plan or an expansion of task scope."""
    if not isinstance(plan,dict) or set(plan)!={'kind','task_ids','tool_names'} or plan.get('kind')!='execution_improvement':
        return None
    identities=plan.get('task_ids');names=plan.get('tool_names')
    if any(not isinstance(values,list) or not values or len(values)>64
            or any(not isinstance(value,str) or not value for value in values)
            or len(set(values))!=len(values) for values in (identities,names)):
        return None
    owned={task.task_id:task for task in tasks}
    if not set(identities)<=set(owned):return None
    allowed={name for identity in identities for name in owned[identity].allowed_tools}
    return dict(plan) if set(names)<=allowed else None
