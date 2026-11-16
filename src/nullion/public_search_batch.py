"""Bounded independent searches behind the existing registered read boundary."""
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextvars import copy_context
from dataclasses import replace
import time


def search_batch(invocation, single_search):
    from nullion.tools import ToolResult, tool_execution_cancelled, tool_execution_remaining_seconds
    from nullion.request_progress import publish_source_progress
    queries = invocation.arguments.get('queries')
    if (invocation.arguments.get('independent_queries') is not True or 'query' in invocation.arguments
            or not isinstance(queries, list) or not 1 <= len(queries) <= 6
            or not all(isinstance(query, str) and query.strip() for query in queries)):
        return ToolResult(invocation.invocation_id, invocation.tool_name, 'failed',
            {'reason':'invalid_tool_arguments'}, 'Use one to six independent queries with independent_queries=true.')
    remaining = tool_execution_remaining_seconds()
    deadline = time.monotonic() + (max(0, remaining - 1) if remaining is not None else 19)
    executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix='nullion-public-search')
    pending, results, next_index = {}, {}, 0
    def submit(index):
        child = replace(invocation, invocation_id=f'{invocation.invocation_id}:query:{index}',
            arguments={'query':queries[index], 'limit':invocation.arguments.get('limit', 5)}, progress_callback=None)
        pending[executor.submit(copy_context().run, single_search, child)] = index
    cancelled = False
    try:
        while next_index < len(queries) or pending:
            if tool_execution_cancelled():
                cancelled = True
                break
            if time.monotonic() >= deadline:
                break
            while next_index < len(queries) and len(pending) < 2:
                submit(next_index)
                next_index += 1
            done, _ = wait(pending, timeout=min(.1, max(0, deadline-time.monotonic())), return_when=FIRST_COMPLETED)
            for future in done:
                index = pending.pop(future)
                try:
                    results[index] = future.result()
                except Exception as error:
                    results[index] = ToolResult(f'{invocation.invocation_id}:query:{index}', invocation.tool_name,
                        'failed', {'reason':'web_search_failed','failure_scope':'invocation'}, str(error))
            if done and invocation.progress_callback is not None and not tool_execution_cancelled():
                publish_source_progress(invocation, unit='searches', total=len(queries),
                    completed=sum(r.status == 'completed' for r in results.values()),
                    failed=sum(r.status != 'completed' for r in results.values()),
                    records=sum(len(r.output.get('results', ())) for r in results.values() if r.status == 'completed'))
    finally:
        for future in pending:
            future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
    records, outcomes = [], []
    for index, query in enumerate(queries):
        result = results.get(index)
        if result is None:
            outcomes.append({'query_index':index,'status':'failed',
                'reason':'request_cancelled' if cancelled else 'source_timeout','failure_scope':'invocation'})
            continue
        output = result.output
        selected = output.get('results', []) if result.status == 'completed' else []
        records.extend({**row,'query_index':index} for row in selected if isinstance(row, dict))
        outcomes.append({'query_index':index,'status':result.status,'result_count':len(selected),
            **{key:output[key] for key in ('reason','failure_scope','status_code','candidates') if key in output},
            **({'error':result.error} if result.error else {})})
    completed = sum(row['status'] == 'completed' for row in outcomes)
    publish_source_progress(invocation, unit='searches', completed=completed,
        failed=len(queries)-completed, total=len(queries), records=len(records))
    output = {'result_kind':'public_search_results','queries':queries,'query_outcomes':outcomes,
        'results':records,'completed_queries':completed,'requested_queries':len(queries),'parallel_limit':2,
        'source_record_collections':[{'path':['results'],'identity_fields':['query_index','url'],'cardinality':'many'}]}
    if not completed:
        output.update(reason='request_cancelled' if cancelled else 'web_search_failed',
            failure_scope='tool' if any(row.get('failure_scope') == 'tool' for row in outcomes) else 'invocation')
    return ToolResult(invocation.invocation_id, invocation.tool_name, 'completed' if completed else 'failed', output,
        None if completed else 'The requested searches could not be completed.')
