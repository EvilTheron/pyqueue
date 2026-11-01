import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import pytest

from pyqueue.client import Client
from pyqueue.protocol import RequestType
from pyqueue.server import Server

Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


@asynccontextmanager
async def fake_server(handler: Handler) -> AsyncIterator[int]:
    """Run a misbehaving server and yield its port."""
    srv = await asyncio.start_server(handler, "127.0.0.1", 0)
    port: int = srv.sockets[0].getsockname()[1]
    try:
        yield port
    finally:
        srv.close()
        await srv.wait_closed()


async def test_request_returns_response(server: Server) -> None:
    async with Client(port=server.port) as client:
        response = await client.request(RequestType.PRODUCE, b"hi")
    assert response.request_type is RequestType.PRODUCE
    assert response.body == b"hi"


async def test_correlation_ids_increase(server: Server) -> None:
    async with Client(port=server.port) as client:
        first = await client.request(RequestType.METADATA)
        second = await client.request(RequestType.METADATA)
    assert (first.correlation_id, second.correlation_id) == (0, 1)


async def test_concurrent_requests_get_their_own_responses(server: Server) -> None:
    async with Client(port=server.port) as client:
        bodies = [f"msg-{i}".encode() for i in range(50)]
        responses = await asyncio.gather(
            *(client.request(RequestType.PRODUCE, body) for body in bodies)
        )
    assert [response.body for response in responses] == bodies


async def test_request_before_connect_raises() -> None:
    client = Client()
    with pytest.raises(RuntimeError):
        await client.request(RequestType.METADATA)


async def test_request_after_close_raises(server: Server) -> None:
    async with Client(port=server.port) as client:
        await client.request(RequestType.METADATA)
    with pytest.raises(RuntimeError):
        await client.request(RequestType.METADATA)


async def test_server_disconnect_fails_pending_request() -> None:
    async def close_after_first_byte(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        await reader.read(1)
        writer.close()
        await writer.wait_closed()

    async with fake_server(close_after_first_byte) as port, Client(port=port) as client:
        with pytest.raises(ConnectionError):
            await client.request(RequestType.PRODUCE, b"lost")


async def test_request_times_out_when_server_is_silent() -> None:
    async def never_respond(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await reader.read()
        writer.close()

    async with fake_server(never_respond) as port, Client(port=port, timeout=0.2) as client:
        with pytest.raises(TimeoutError):
            await client.request(RequestType.PRODUCE, b"ping")
