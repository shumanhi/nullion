from __future__ import annotations

import os
import threading
from pathlib import Path

import pytest


@pytest.fixture(scope='session')
def live_stage_provider_guard():
    from scripts.live_stage_provider_guard import process_provider_guard
    return process_provider_guard()


@pytest.fixture(autouse=True)
def stop_paid_live_tests_after_terminal_provider_failure(request, live_stage_provider_guard):
    if os.environ.get('NULLION_LIVE_STAGE_E2E') != '1' or not request.node.path.name.startswith('test_live_stage'):
        yield
        return
    blocked = live_stage_provider_guard.check_boundary()
    if blocked:
        pytest.exit(blocked, returncode=1)
    yield
    blocked = live_stage_provider_guard.check_boundary()
    if blocked:
        pytest.exit(blocked, returncode=1)


@pytest.fixture(autouse=True)
def isolate_test_preferences(tmp_path, monkeypatch):
    if os.environ.get("NULLION_RUN_E2E") == "1":
        from nullion import preferences
        # Settings tests intentionally persist consent and sentinel modes.
        # Each product workflow needs its own file even when the module was
        # imported before a later fixture changed NULLION_HOME.
        monkeypatch.setattr(preferences, "_PREFS_PATH", tmp_path / "preferences.json")
    yield


@pytest.fixture(autouse=True)
def drain_test_builder_tasks(monkeypatch, _close_test_timers):
    if os.environ.get("NULLION_RUN_E2E") != "1":
        yield
        return
    from nullion import builder_background

    completed = []
    original_schedule = builder_background.schedule_builder_background_task

    def schedule(label, worker):
        done = threading.Event()

        def run():
            try:
                worker()
            finally:
                done.set()

        accepted = original_schedule(label, run)
        if accepted:
            completed.append((label, done))
        return accepted

    monkeypatch.setattr(builder_background, "schedule_builder_background_task", schedule)
    yield
    # Real queued work must finish before its provider/DB patches are restored.
    # Otherwise the next case's HTTP fake can receive an earlier account's call.
    for label, done in completed:
        assert done.wait(timeout=10), f"Test background task did not finish: {label}"


@pytest.fixture(autouse=True)
def close_test_app_schedulers(monkeypatch):
    if os.environ.get("NULLION_RUN_E2E") != "1":
        yield
        return
    from nullion.crons import CronScheduler

    started = []
    original_start = CronScheduler.start

    def start(scheduler):
        result = original_start(scheduler)
        thread = getattr(scheduler, "_thread", None)
        if isinstance(thread, threading.Thread) and thread.ident is not None:
            started.append(scheduler)
        return result

    monkeypatch.setattr(CronScheduler, "start", start)
    yield
    # Keep each app's real timer scoped to its test, before transport and
    # database patches are restored and the next test supplies a new store.
    for scheduler in started:
        scheduler.stop()
        scheduler._thread.join(timeout=2)
        assert not scheduler._thread.is_alive(), "Test app scheduler did not stop"
        status_thread = getattr(scheduler, '_status_thread', None)
        if status_thread is not None and status_thread.ident is not None:
            status_thread.join(timeout=2)
            assert not status_thread.is_alive(), "Test tracker notifier did not stop"


def pytest_configure(config: pytest.Config) -> None:  # noqa: ARG001
    if os.environ.get("NULLION_RUN_E2E") != "1":
        return
    if os.environ.get("NULLION_E2E_CONTAINER") == "1" and Path("/.dockerenv").exists():
        return
    pytest.exit(
        "Refusing to run e2e tests on the host. Use scripts/e2e-docker.sh so GUI/e2e tests run inside Docker.",
        returncode=4,
    )


@pytest.fixture
def sourced_report_model_reply():
    """Script only the external model review; assert real tool/artifact evidence."""
    import json

    def reply(kwargs, *, source_tools, producer_tool, expected_tokens):
        from tests.workflow_provider_protocol import workflow_provider_reply, fixture_source_plan, fixture_source_assessment
        action_reply = workflow_provider_reply(kwargs, completion=False)
        if action_reply is not None: return action_reply
        system = str(kwargs.get('system') or '')
        if system.startswith('You are a material-requirements planner'):
            packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
            payload = {**fixture_source_plan(packet, scope='evidence_summary'), 'all_material_requirements_enumerated': True, 'material_requirements': [
                {'requirement': 'Read the requested current source records', 'requirement_kind': 'fact',
                 'evidence_subject_id': 'source', 'eligible_evidence_tool_names': list(source_tools)},
                {'requirement': 'Deliver the requested report with the source records', 'requirement_kind': 'deliverable',
                 'evidence_subject_id': 'report', 'eligible_evidence_tool_names': [producer_tool]},
            ]}
        elif system.startswith('You are a semantic completion reviewer'):
            evidence = json.loads(kwargs['messages'][0]['content'][0]['text'])
            artifact_text = json.dumps(evidence['artifact_evidence'], ensure_ascii=False)
            assert all(token in artifact_text for token in expected_tokens), artifact_text
            rows = []
            for requirement in evidence['material_requirement_contract']:
                matching = [record for record in evidence['tool_evidence']
                    if record.get('tool_name') in requirement['eligible_evidence_tool_names']
                    and record.get('status') == 'completed']
                assert matching, requirement
                rows.append({**requirement, 'satisfied': True, 'draft_covered': True,
                             'evidence_invocation_ids': [record['invocation_id'] for record in matching]})
            payload = {**fixture_source_assessment(evidence, conclusion='limited'), 'disposition': 'complete', 'all_material_requirements_enumerated': True,
                       'material_requirements': rows, 'unresolved_requirements': [], 'retry_tool_names': []}
        else:
            return None
        return {'stop_reason': 'end_turn', 'content': [{'type': 'text', 'text': json.dumps(payload)}]}

    return reply


@pytest.fixture
def source_summary_model_reply():
    """External semantic review for captured answers with optional unknowns."""
    import json
    def reply(kwargs):
        from tests.workflow_provider_protocol import workflow_provider_reply, fixture_source_plan, fixture_source_assessment
        action_reply = workflow_provider_reply(kwargs, completion=False)
        if action_reply is not None: return action_reply
        system = str(kwargs.get('system') or '')
        if system.startswith('You are a material-requirements planner'):
            packet = json.loads(kwargs['messages'][0]['content'][0]['text'])
            decision = {**fixture_source_plan(packet, scope='evidence_summary'), 'all_material_requirements_enumerated':True, 'material_requirements':[
                {'requirement':'Answer the requested question using recorded observations and disclose uncertainty',
                 'requirement_kind':'fact', 'evidence_subject_id':'answer',
                 'eligible_evidence_tool_names':['browser_run_task']}]}
        elif system.startswith('You are a semantic completion reviewer'):
            payload=json.loads(kwargs['messages'][0]['content'][0]['text'])
            assert payload['draft_final_response'].strip()
            assert payload['material_requirement_contract']
            ids=[r['invocation_id'] for r in payload['tool_evidence'] if r['tool_name']=='browser_run_task' and r['status']=='completed']
            assert ids
            decision={**fixture_source_assessment(payload, conclusion='limited'), 'disposition':'complete', 'all_material_requirements_enumerated':True,
                'unresolved_requirements':[], 'retry_tool_names':[], 'material_requirements':[
                    {**r, 'satisfied':True,'draft_covered':True, 'evidence_invocation_ids':ids[-1:]}
                    for r in payload['material_requirement_contract']]}
        else: return None
        return {'stop_reason':'end_turn','content':[{'type':'text','text':json.dumps(decision)}]}
    return reply
