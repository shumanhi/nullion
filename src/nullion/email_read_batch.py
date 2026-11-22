"""Execute selected account reads under one native, deadline-bound READ grant."""

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextvars import copy_context
from dataclasses import replace
from time import monotonic


def read_independent_messages(invocation, read_message, *, prepare_read=None):
    from nullion.retained_work_context import inspection_request
    from nullion.source_context_projection import email_body_field
    from nullion.tools import (
        ToolResult, _default_email_connector_provider_id, _invalid_concrete_resource_id_reason,
        tool_execution_cancelled, tool_execution_remaining_seconds,
    )

    ids = invocation.arguments['ids']
    if (invocation.arguments.get('independent_messages') is not True or 'id' in invocation.arguments
            or not 1 <= len(ids) <= 10 or not all(isinstance(item, str) and len(item) <= 512 and item == item.strip()
                and item and not _invalid_concrete_resource_id_reason(item) for item in ids)
            or len(set(ids)) != len(ids)):
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed',
            {'reason': 'invalid_tool_arguments', 'executed': False}, 'Select up to ten distinct, exact message ids.')
    try:
        provider_id = str(invocation.arguments.get('provider_id') or '').strip()
        if not provider_id:
            provider_id = _default_email_connector_provider_id(invocation.principal_id)
        if prepare_read is not None:
            read_message = prepare_read(invocation, provider_id)
    except Exception as error:
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed',
            {'reason': 'email_source_unavailable', 'executed': False}, str(error))

    children = [replace(invocation, invocation_id=f'{invocation.invocation_id}:message:{index}',
                        arguments={'id': identifier, 'provider_id': provider_id}, progress_callback=None)
                for index, identifier in enumerate(ids)]
    remaining = tool_execution_remaining_seconds()
    # Publish completed items just before the outer native timer closes.
    deadline = monotonic() + (max(0.0, remaining - 0.25) if remaining is not None else 20.0)
    pending, results = {}, {}
    next_index = 0
    pool = ThreadPoolExecutor(max_workers=min(3, len(children)), thread_name_prefix='nullion-email-read')
    try:
        while next_index < len(children) or pending:
            if tool_execution_cancelled() or monotonic() >= deadline:
                break
            while next_index < len(children) and len(pending) < 3 and monotonic() < deadline:
                pending[pool.submit(copy_context().run, read_message, children[next_index])] = next_index
                next_index += 1
            done, _ = wait(pending, timeout=min(0.1, max(0.0, deadline - monotonic())), return_when=FIRST_COMPLETED)
            for future in done:
                index = pending.pop(future)
                try:
                    results[index] = future.result()
                except Exception as error:
                    results[index] = ToolResult(children[index].invocation_id, invocation.tool_name, 'failed',
                        {'reason': 'email_read_failed'}, str(error))
    finally:
        pool.shutdown(wait=False, cancel_futures=True)

    cancelled = tool_execution_cancelled()
    rows = []
    for index, child in enumerate(children):
        result = results.get(index)
        if result is None:
            reason = 'request_cancelled' if cancelled else 'source_timeout'
            result = ToolResult(child.invocation_id, child.tool_name, 'failed',
                {'reason': reason, 'executed': index < next_index}, 'The selected message read did not finish.')
        message = result.output.get('message')
        if result.status == 'completed' and (not isinstance(message, dict) or message.get('id') != ids[index]):
            result = ToolResult(child.invocation_id, child.tool_name, 'failed',
                {'reason': 'email_message_identity_mismatch'}, 'The provider did not return the selected message.')
        row = {'id': ids[index], 'invocation_id': child.invocation_id, 'status': result.status,
               'output': result.output, **({'error': result.error} if result.error else {})}
        if result.status == 'completed' and (field := email_body_field(result.output.get('message'))):
            row['inspection_request'] = inspection_request(invocation.invocation_id, f'/messages/{index}/output/message/{field}')
        rows.append(row)
    completed = sum(row['status'] == 'completed' for row in rows)
    output = {'result_kind': 'email_message_batch', 'provider_id': provider_id, 'messages': rows,
              'requested_messages': len(rows), 'completed_messages': completed, 'complete': completed == len(rows),
              'parallel_limit': min(3, len(children))}
    # Preserve partial evidence without caching it as a successful read batch.
    if output['complete']:
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed', output)
    output['reason'] = 'email_batch_incomplete'
    return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed', output,
                      'Some selected messages could not be read.' if completed else 'None of the selected messages could be read.')
