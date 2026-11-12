"""Real sockets prove that connection recovery cannot repeat a request."""
import asyncio
import importlib.util
from pathlib import Path
import socket
import time


def test_stage_transport_waits_before_forwarding_and_never_resends_bytes():
    spec = importlib.util.spec_from_file_location('stage_transport', Path(__file__).resolve().parents[2]/'scripts/live_stage_tcp_forwarder.py')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    async def scenario():
        reserve = socket.socket(); reserve.bind(('127.0.0.1', 0)); port = reserve.getsockname()[1]; reserve.close()
        received = []
        async def upstream(reader, writer):
            received.append(await reader.readexactly(12)); writer.write(b'confirmed'); await writer.drain(); writer.close()
        proxy = await asyncio.start_server(lambda r, w: module.forward_connection(r, w, host='127.0.0.1', port=port), '127.0.0.1', 0)
        reader, writer = await asyncio.open_connection('127.0.0.1', proxy.sockets[0].getsockname()[1])
        writer.write(b'one request!'); await writer.drain()
        await asyncio.sleep(.15)
        actual = await asyncio.start_server(upstream, '127.0.0.1', port)
        assert await asyncio.wait_for(reader.read(), timeout=3) == b'confirmed'
        assert received == [b'one request!']
        writer.close(); await writer.wait_closed()
        proxy.close(); actual.close(); await proxy.wait_closed(); await actual.wait_closed()
    asyncio.run(scenario())


def test_stage_transport_closes_when_connection_deadline_expires():
    spec = importlib.util.spec_from_file_location('stage_transport', Path(__file__).resolve().parents[2]/'scripts/live_stage_tcp_forwarder.py')
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    async def scenario():
        reserve = socket.socket(); reserve.bind(('127.0.0.1', 0)); port = reserve.getsockname()[1]; reserve.close()
        server = await asyncio.start_server(lambda r, w: module.forward_connection(r, w, host='127.0.0.1', port=port, connect_seconds=.15), '127.0.0.1', 0)
        started = time.monotonic()
        reader, writer = await asyncio.open_connection('127.0.0.1', server.sockets[0].getsockname()[1])
        assert await asyncio.wait_for(reader.read(), timeout=1) == b''
        assert time.monotonic()-started < 1
        writer.close(); await writer.wait_closed(); server.close(); await server.wait_closed()
    asyncio.run(scenario())
