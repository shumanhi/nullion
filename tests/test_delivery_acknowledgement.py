"""A transport retry must not create a second externally accepted send."""
import asyncio
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from nullion.delivery_acknowledgement import UnconfirmedDelivery, send_error_replay_safety, deliver_attachment_batch
from nullion.messaging_adapters import retry_messaging_delivery_operation


@pytest.mark.parametrize('kind', [httpx.ReadTimeout, httpx.WriteTimeout, httpx.ReadError,
    httpx.WriteError, httpx.RemoteProtocolError, TimeoutError, ConnectionError])
def test_uncertain_send_is_not_replayed(kind):
    calls = []
    async def send():
        calls.append('accepted before losing acknowledgement')
        raise kind('opaque transport diagnostic')
    with pytest.raises(UnconfirmedDelivery):
        asyncio.run(retry_messaging_delivery_operation(send, replay_safe=False, retry_delay_seconds=0))
    assert len(calls) == 1


@pytest.mark.parametrize('kind', [httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout])
def test_pre_send_failure_can_retry(kind):
    calls = []
    async def send():
        calls.append(1)
        if len(calls) == 1: raise kind('not connected')
        return 7
    assert asyncio.run(retry_messaging_delivery_operation(send, replay_safe=False, retry_delay_seconds=0)) == 7
    assert len(calls) == 2


def test_wrapped_read_timeout_outranks_nested_connection_error():
    nested = httpx.ConnectError('prior connection attempt')
    read = httpx.ReadTimeout('response lost'); read.__cause__ = nested
    outer = RuntimeError('opaque provider wrapper'); outer.__cause__ = read
    assert send_error_replay_safety(outer) == 'unknown'


@pytest.mark.parametrize('unknown', [True, False])
def test_batch_preserves_acknowledged_files_and_stops_before_third(unknown):
    paths = tuple(Path(f'/synthetic/{n}.bin') for n in range(3))
    seen = []
    async def send(index, path):
        seen.append(path)
        if index == 1:
            if unknown: raise UnconfirmedDelivery()
            raise ValueError('explicit rejection')
        return SimpleNamespace(message_id=7)
    with pytest.raises(UnconfirmedDelivery) as caught:
        asyncio.run(deliver_attachment_batch(paths, send))
    exc = caught.value
    assert seen == list(paths[:2]) and exc.confirmed == (str(paths[0]),)
    assert exc.message_ids == ('7',)
    assert exc.status == ('unknown' if unknown else 'partial')
    assert exc.uncertain == ((str(paths[1]),) if unknown else ())
    assert exc.unsent == tuple(str(p) for p in paths[2 if unknown else 1:])


@pytest.mark.parametrize('persistent', [False, True])
def test_unknown_outbox_does_not_retry_even_after_reopen(tmp_path, persistent):
    from nullion.delivery_outbox import DeliveryOutbox
    path = tmp_path/'runtime.db' if persistent else None
    box = DeliveryOutbox(path)
    for status in ('pending', 'failed', 'delivering', 'delivered', 'unknown', 'partial'):
        box.put(status, {'conversation_id':'telegram:77','status':status,'attempts':1,'retry_at':0})
    if persistent: box = DeliveryOutbox(path)
    assert set(box.due()) == {'pending','failed','delivering'}


def test_runtime_report_receipt_is_typed_and_identity_follows_exact_bytes(tmp_path):
    from nullion.cron_delivery import _complete_typed_html_artifact_recovery
    from nullion.tools import ToolResult
    report=tmp_path/'report.html'
    report.write_text('<html><body><h1>Selected report</h1><p>Verified record: 7.</p></body></html>')
    result={'tool_results':[]}
    def accept():
        _complete_typed_html_artifact_recovery(result,artifact_path=report,required_extensions=('.html',),
            request='照合結果',content_outcome={'status':'partial','remaining_work':['Selected comparison not finished']})
        return result['tool_results'][-1]
    first=accept();assert isinstance(first,ToolResult) and first.invocation_id
    assert accept().invocation_id==first.invocation_id
    report.write_text(report.read_text().replace('7.','8.'))
    assert accept().invocation_id!=first.invocation_id


@pytest.mark.parametrize('status',['unknown','partial'])
def test_manual_status_does_not_claim_failure_or_completion_for_unconfirmed_delivery(status):
    from nullion.cron_delivery import manual_cron_terminal_status_text
    job=SimpleNamespace(name='確認')
    reply=manual_cron_terminal_status_text(job,result={'cron_delivery_status':status,'response_fulfilled':False})
    assert 'Delivery unconfirmed' in reply and 'automatic upload replay has stopped' in reply
    assert 'Failed:' not in reply and 'Completed:' not in reply


def test_invalid_adapter_retry_policy_cannot_authorize_replay():
    async def send():raise TimeoutError('Lost acknowledgement')
    with pytest.raises(UnconfirmedDelivery):
        asyncio.run(retry_messaging_delivery_operation(send,replay_safe=False,retry_policy=lambda exc:'invalid'))
