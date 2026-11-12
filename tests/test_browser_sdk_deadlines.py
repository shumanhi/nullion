import asyncio
import time
import pytest
from nullion.plugins.browser_plugin.browser_use_adapter import _bounded_sdk_await


@pytest.mark.parametrize('owner_cancel',[False,True])
def test_sdk_cancellation_cannot_extend_observation_deadline(owner_cancel):
    async def scenario():
        release=asyncio.Event();started=asyncio.Event();inner=[]
        async def stubborn():
            inner.append(asyncio.current_task());started.set()
            try:await asyncio.Future()
            except asyncio.CancelledError:await release.wait()
        outer=asyncio.create_task(_bounded_sdk_await(stubborn(),.03 if not owner_cancel else 20))
        await started.wait();before=time.monotonic()
        try:
            if owner_cancel:outer.cancel()
            with pytest.raises(asyncio.CancelledError if owner_cancel else TimeoutError):
                await asyncio.wait_for(outer,.3)
            assert time.monotonic()-before<.3
        finally:
            release.set();await asyncio.gather(*inner,return_exceptions=True)
    asyncio.run(scenario())


def test_sdk_deadline_preserves_completed_results_and_exceptions():
    async def scenario():
        value={'captured':37}
        async def complete():return value
        async def fail():raise ValueError('Native SDK failure')
        assert await _bounded_sdk_await(complete(),1) is value
        with pytest.raises(ValueError):await _bounded_sdk_await(fail(),1)
    asyncio.run(scenario())


def test_sdk_timeout_wrappers_use_exception_facts_not_error_wording():
    from nullion.plugins.browser_plugin.browser_use_adapter import _sdk_timeout_error
    try:
        try:raise TimeoutError('接続')
        except TimeoutError:raise RuntimeError('Conexión sin respuesta')
    except RuntimeError as wrapped:
        assert _sdk_timeout_error(wrapped)
        assert _sdk_timeout_error(ExceptionGroup('group',[wrapped]))
    assert not _sdk_timeout_error(ValueError('TimeoutError: connection timed out'))
