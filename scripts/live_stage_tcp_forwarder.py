"""Stage test transport; retries only before any request bytes are forwarded."""
import asyncio
import logging
import time
import json

logger = logging.getLogger(__name__)


def live_forward_ports(fixture_ports='[]'):
    """Expose only explicitly owned fixture ports alongside the stage lane."""
    ports=json.loads(fixture_ports or '[]')
    if not isinstance(ports,list) or any(type(port) is not int or not 1024<=port<=65535
            or port in {8742,8752,8753} for port in ports):
        raise ValueError('Fixture ports must be integer ports outside the runtime lanes')
    return [8753,*dict.fromkeys(ports)]


async def forward_connection(reader, writer, *, host, port, connect_seconds=2.0):
    upstream_writer = None
    deadline = time.monotonic() + connect_seconds
    try:
        while True:
            try:
                upstream_reader, upstream_writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port), timeout=max(.001, deadline - time.monotonic()))
                break
            except (OSError, TimeoutError) as exc:
                if time.monotonic() >= deadline:
                    raise
                # No reader.read() has happened yet: this cannot resend a chat,
                # action, or partially forwarded HTTP request.
                await asyncio.sleep(min(.1, max(0, deadline - time.monotonic())))
        async def copy(source, destination):
            try:
                while chunk := await source.read(65536):
                    destination.write(chunk)
                    await destination.drain()
            finally:
                destination.close()
        await asyncio.gather(copy(reader, upstream_writer), copy(upstream_reader, writer))
    except (OSError, TimeoutError) as exc:
        logger.warning('Stage test transport closed: %s', type(exc).__name__)
    finally:
        for stream in (writer, upstream_writer):
            if stream is not None:
                stream.close()
                try:
                    await stream.wait_closed()
                except OSError:
                    pass
