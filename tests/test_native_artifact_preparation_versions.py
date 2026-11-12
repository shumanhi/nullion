"""Inspecting accepted producer bytes must not invent a replacement write."""
from types import SimpleNamespace

import pytest

from nullion.artifact_lifecycle import (
    artifact_candidate_versions, record_delivery_manifest, select_delivery_paths,
)
from nullion.completion_pipeline import prepare_completion
from nullion.cron_delivery import _ScheduledCompletionPreparer
from nullion.tools import ToolRegistry, ToolResult


@pytest.mark.parametrize('review_state', ['timeout', 'limit'])
def test_existing_native_report_keeps_current_selection_through_common_preparation(tmp_path, review_state):
    path = tmp_path / 'report.html'
    path.write_text('<!doctype html><html><head><title>Selected report</title></head>'
        '<body><h1>Selected report</h1><p>Current value: 7.</p></body></html>')
    producer = ToolResult('actual-native-write', 'file_write', 'completed',
        {'path': str(path), 'artifact_path': str(path), 'artifact_paths': [str(path)]})
    results = [producer]
    record_delivery_manifest(results, [str(path)], candidate_paths=[str(path)])
    versions = artifact_candidate_versions(results)
    job = SimpleNamespace(task='選択した値を報告。', artifact_delivery_options={
        'requires_attachment_delivery': True, 'required_artifact_extensions': ['.html']})
    preparer = _ScheduledCompletionPreparer(job, 'telegram:123', model_client=None, record_preflight=None)
    state = {'completion_preparer': preparer, 'tool_results': results,
        'artifacts': [str(path)], 'tool_registry': ToolRegistry(),
        'completion_interruption': {'model_timed_out': review_state == 'timeout',
            'reached_iteration_limit': review_state == 'limit'}}
    next_state, reply, changed = prepare_completion(state, 'Report ready.')
    assert changed
    assert artifact_candidate_versions(next_state['tool_results']) == versions
    assert [row.invocation_id for row in next_state['tool_results']] == ['actual-native-write']
    assert select_delivery_paths(next_state['artifacts'], next_state['tool_results'], reply,
        required_extensions=['.html']) == ([str(path)], False)
    assert path.read_text().endswith('</html>')


@pytest.mark.parametrize("missing_media", [False, True])
@pytest.mark.parametrize("missing_companion", [False, True])
@pytest.mark.parametrize("change", ["bytes", "request", "evidence", "selection", "receipt"])
def test_bound_partial_report_keeps_delivery_separate_from_newer_source_work(tmp_path, missing_media, missing_companion, change):
    from nullion.agent_orchestrator import _completed_required_artifact_paths_for_turn
    from nullion.execution_outcome import prepare_answer
    from nullion.report_completion import bind_report_outcome
    path = tmp_path / "partial.html"
    path.write_text("<!doctype html><html><body><h1>Observed result</h1><p>7</p></body></html>")
    request = "観測した結果を報告し未完了の確認は明示。"
    rows = [ToolResult("write", "file_write", "completed", {"artifact_paths": [str(path)]}),
            ToolResult("later", "browser_extract_text", "completed", {"text": "New source observation: 8"})]
    state = {"user_message": request, "tool_results": rows, "artifacts": [str(path)],
        "tool_flow_context": {"required_artifact_extensions": [".html", ".pdf"] if missing_companion else [".html"],
            "required_embedded_media_extensions": [".html"] if missing_media else []},
        "completion_content_receipt": bind_report_outcome(request,
            {"status": "partial", "remaining_work": ["Review new observation"]}, [str(path)]),
        "completion_prepared_answer": prepare_answer(request, "Partial report attached.", rows, [str(path)])}
    assert _completed_required_artifact_paths_for_turn(state, [str(path)]) == ()
    assert _completed_required_artifact_paths_for_turn(state, [str(path)], allow_partial_report=True) == (str(path),)
    if change == "bytes":
        path.write_text("<!doctype html><html><body>Different report</body></html>")
    elif change == "request":
        state["user_message"] = "別の依頼。"
    elif change == "evidence":
        rows[-1].output["text"] = "Different source observation"
    elif change == "selection":
        state["completion_content_receipt"]["files"] = {}
    else:
        state["completion_content_receipt"] = None
    assert _completed_required_artifact_paths_for_turn(state, [str(path)], allow_partial_report=True) == ()
