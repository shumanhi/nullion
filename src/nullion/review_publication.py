"""Publish reviewed evidence, never the reviewer's editing instructions.

This boundary deliberately does not read free-form review diagnostics. Public
limitations come from typed outcome state; certified findings retain their
source-backed detail. No language detector or additional model call is needed.
"""


def finding_layout(row):
    """Malformed display hints cannot invalidate otherwise grounded findings."""
    presentation = row.get('presentation')
    section = row.get('section')
    return (presentation if presentation in ('paragraph', 'item') else None,
        section.strip() if isinstance(section, str) and section.strip()
        and len(section) <= 80 and not any(char in section for char in '\r\n') else None)


def render_supported_findings(findings, *, message_kind=None):
    """Adapt certified rows to the common content contract, without new review."""
    from nullion.chat_text import ReplyBlock, UserMessage, render_user_message
    blocks=[]; previous_section=None
    for row in findings:
        body=row.text.strip()
        if not body: continue
        section=getattr(row,'section',None)
        if section!=previous_section and section:
            blocks.append(ReplyBlock(section,'heading'))
        previous_section=section
        blocks.append(ReplyBlock(body,'item' if getattr(row,'presentation',None)=='item' else 'paragraph'))
    return render_user_message(UserMessage(blocks=tuple(blocks), kind=message_kind))


def operational_failure_notice(review):
    """Safe copy for a runtime-attributed failure, never a critic's prose."""
    if (review is None or getattr(review, 'review_error', None)
            or getattr(review, 'runtime_failure_code', None) not in {
                'network_error', 'browser_timeout', 'browser_action_failed', 'browser_operation_failed',
                'action_denied', 'source_connection_required', 'scope_selection_stalled',
                'preflight_revision_stalled'}):
        return None
    from dataclasses import replace
    return render_incomplete_review(replace(review, disposition='blocked', supported_findings=()))


def comparison_material_gap(review):
    """Only a runtime-validated material shortlist gap, never a broad warning."""
    if getattr(review, 'runtime_failure_code', None) != 'search_coverage_incomplete':
        return None
    errors = set(getattr(review, 'search_coverage_errors', ()) or ())
    if not errors & {'comparison_frontier_unchecked', 'comparison_ranking_unresolved'}:
        return None
    coverage = getattr(review, 'search_coverage', None) or {}
    frontier = set((coverage.get('comparison_scope') or {}).get('frontier_candidate_ids', ()))
    remaining = sum(row.get('candidate_id') in frontier and row.get('disposition') not in {'checked','irrelevant'}
        for row in coverage.get('candidates', ()))
    return (f"The comparison is unfinished: {remaining} shortlisted "
        f"{'alternative still needs' if remaining == 1 else 'alternatives still need'} verification."
        if remaining else "The shortlisted alternatives could not all be verified, so the comparison is unfinished."
        if frontier else "The ranking of the checked options is still unverified.")


def render_incomplete_review(review):
    from nullion.chat_text import UserMessage, render_user_message
    rows_to_publish = review.supported_findings if review.review_error is None else ()
    formatted = render_supported_findings(rows_to_publish,
        message_kind='report' if getattr(review, 'report_layout_required', False) else None)
    findings = [formatted] if formatted else []
    def publish(notice):
        return render_user_message(UserMessage(paragraphs=tuple([*findings, notice])))
    disposition = review.disposition
    if disposition == "needs_user_input":
        if review.clarification_question and review.clarification_options:
            return render_user_message(UserMessage(paragraphs=tuple([*findings,review.clarification_question]),
                choices=tuple(review.clarification_options)))
        return render_user_message(UserMessage(paragraphs=tuple([*findings,'I need one detail before I can finish. Reply with one option:']),
            choices=('Provide the missing detail.','Ask me to continue with the best verifiable alternative.')))
    if disposition == "needs_approval":
        return render_user_message(UserMessage(paragraphs=tuple([*findings,'I need your approval before I can continue. Reply with one option:']),
            choices=('Approve the pending action.','Cancel it.')))

    rows = review.material_requirements
    missing = [row for row in rows if not row.satisfied]
    # A rejected draft can contain unsupported extras while every requested
    # fact/action is already satisfied. Publish the independently certified
    # findings without appending a fictional missing-work notice. This does
    # not promote the execution/review verdict or resurrect the rejected draft.
    code = getattr(review, "runtime_failure_code", None)
    described_requirements = {identity for row in rows_to_publish if row.kind == 'limitation'
        for identity in getattr(row, 'requirement_ids', ())}
    if findings and any(row.kind == 'limitation' for row in rows_to_publish) and code in {
            None, 'unverified_requirements', 'search_coverage_incomplete'} and {
                row.requirement_id for row in missing}.issubset(described_requirements) and not (
                    code == 'search_coverage_incomplete' and getattr(review, 'search_coverage_errors', ())):
        # Certified, concrete limitations already explain the incomplete work.
        # Independent coverage gaps are not bound by material requirement ids.
        # This changes publication only; the outcome remains incomplete.
        return formatted
    if (findings and rows and not missing and review.all_material_requirements_enumerated is True
            and code is None):
        return "\n\n".join(findings)

    # Only runtime-owned failure codes can select specific operational copy.
    # The model's user_facing_failure and unresolved_requirements are diagnostics,
    # irrespective of their names. They have no publication authority.
    if code == "deadline":
        notice = "I couldn't complete this request before the response time limit."
    elif code == "model_response_timeout":
        notice = "The model timed out while preparing the response."
    elif code == "preflight_revision_stalled":
        notice = "The proposed action still needed correction, so it was not executed. The remaining work is unfinished."
    elif code == "scope_selection_stalled":
        notice = "Tool selection kept returning the same capabilities without completing the remaining work. The requested result is unfinished."
    elif code == "operation_pending":
        notice = "The operation was dispatched; a confirmed final result is unavailable."
    elif code == "network_error":
        notice = "The source request failed because of a network error. The requested output is incomplete. Try again when the connection is available."
    elif code == "browser_timeout":
        notice = "A browser step timed out. The requested result wasn't confirmed."
    elif code == "browser_action_failed":
        notice = "The page interaction failed. The requested result wasn't confirmed."
    elif code == "browser_operation_failed":
        notice = "A required browser step failed. The affected result wasn't confirmed."
    elif code == "action_denied":
        notice = "The required action wasn't authorized."
    elif code == "source_connection_required":
        notice = "A source check requires an account connection. Reconnect it before retrying."
    elif code == "required_tool_missing":
        notice = "A required step has not been completed."
    elif code == "search_coverage_incomplete":
        from nullion.search_coverage import COVERAGE_PROTOCOL_ERRORS
        errors = set(getattr(review, "search_coverage_errors", ()) or ())
        if errors and errors <= COVERAGE_PROTOCOL_ERRORS:
            notice = "I couldn't finish validating the search. The findings above are available, but the final check is unconfirmed."
        elif 'search_scope_mismatch' in errors:
            notice = "The completed search did not cover the full requested scope."
        elif 'unverified_search_constraints' in errors:
            notice = "Some requested search filters remain unverified."
        elif 'unobserved_positive_match' in errors or 'unverified_positive_match' in errors:
            notice = "The proposed match could not be confirmed from a completed source check."
        elif errors == {'broader_source_discovery_not_observed'}:
            notice = "I couldn't confirm that the search covered the requested sources."
        elif 'comparison_scope_undisclosed' in errors:
            notice = "The listed options were checked, but an overall best across all available options has not been established."
        elif 'unobserved_comparison_selection' in errors:
            notice = "The selected options could not all be confirmed from completed source checks."
        elif errors & {'comparison_frontier_unchecked', 'comparison_ranking_unresolved'}:
            notice = comparison_material_gap(review)
        elif 'unobserved_candidate' in errors:
            notice = "I couldn't verify every candidate used in this comparison."
        elif 'ungrounded_candidate_exclusion' in errors:
            notice = "Some alternatives were excluded without enough evidence, so the comparison is unfinished."
        else:
            coverage = getattr(review, 'search_coverage', None) or {}
            remaining = sum(row.get('disposition') == 'pending' for row in coverage.get('candidates', ()))
            notice = (f"{remaining} discovered {'candidate remains' if remaining == 1 else 'candidates remain'} unchecked; these findings cover the completed checks."
                if remaining else "Some requested source checks remain unfinished.")
    elif code == "unverified_requirements":
        notice = "Some requested work remains incomplete."
    elif missing:
        kinds = {row.requirement_kind for row in missing}
        if kinds == {"action"}:
            notice = "The remaining requested actions have not been confirmed."
        elif kinds == {"deliverable"}:
            notice = "The requested output is not yet complete."
        elif kinds.issubset({"fact", "comparison_side"}):
            notice = "Some requested information remains unconfirmed."
        else:
            notice = "Some requested work remains incomplete."
    elif findings:
        notice = "Some requested work remains incomplete."
    else:
        notice = "I couldn't complete this request. The final result is incomplete."
    return publish(notice)
