import asyncio
import contextlib

import pytest

from pyqueue.protocol import LENGTH, MAX_FRAME_SIZE, Frame, RequestType, encode_frame
from pyqueue.server import Server
from pyqueue.transport import read_frame, write_frame

TIMEOUT = 2.0


async def connect(server: Server) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    return await asyncio.open_connection("127.0.0.1", server.port)


async def close(writer: asyncio.StreamWriter) -> None:
    writer.close()
    await writer.wait_closed()


async def assert_closed_by_server(reader: asyncio.StreamReader) -> None:
    assert await asyncio.wait_for(reader.read(), TIMEOUT) == b""


async def wait_until_listening(server: Server) -> int:
    while True:
        try:
            return server.port
        except RuntimeError:
            await asyncio.sleep(0.01)


class BrokenServer(Server):
    """Server whose request handler always fails, imitating a bug."""

    async def _handle_frame(self, frame: Frame) -> Frame:
        raise RuntimeError("bug in handler")


async def test_echoes_frame(server: Server) -> None:
    reader, writer = await connect(server)
    frame = Frame(RequestType.PRODUCE, 1, b"hello")
    await write_frame(writer, frame)
    assert await asyncio.wait_for(read_frame(reader), TIMEOUT) == frame
    await close(writer)


async def test_handles_many_frames_on_one_connection(server: Server) -> None:
    reader, writer = await connect(server)
    for correlation_id in range(5):
        frame = Frame(RequestType.FETCH, correlation_id, b"x")
        await write_frame(writer, frame)
        assert await asyncio.wait_for(read_frame(reader), TIMEOUT) == frame
    await close(writer)


async def test_splits_two_frames_sent_in_one_write(server: Server) -> None:
    reader, writer = await connect(server)
    first = Frame(RequestType.PRODUCE, 1, b"a")
    second = Frame(RequestType.PRODUCE, 2, b"b")
    writer.write(encode_frame(first) + encode_frame(second))
    await writer.drain()
    assert await asyncio.wait_for(read_frame(reader), TIMEOUT) == first
    assert await asyncio.wait_for(read_frame(reader), TIMEOUT) == second
    await close(writer)


async def test_assembles_frame_sent_byte_by_byte(server: Server) -> None:
    reader, writer = await connect(server)
    frame = Frame(RequestType.METADATA, 9, b"slow")
    for byte in encode_frame(frame):
        writer.write(bytes([byte]))
        await writer.drain()
    assert await asyncio.wait_for(read_frame(reader), TIMEOUT) == frame
    await close(writer)


async def test_closes_connection_on_oversized_length(server: Server) -> None:
    reader, writer = await connect(server)
    writer.write(LENGTH.pack(MAX_FRAME_SIZE + 1))
    await writer.drain()
    await assert_closed_by_server(reader)
    await close(writer)


async def test_closes_connection_on_unknown_type(server: Server) -> None:
    reader, writer = await connect(server)
    writer.write(LENGTH.pack(6) + b"\x03\xe7" + b"\x00\x00\x00\x01")
    await writer.drain()
    await assert_closed_by_server(reader)
    await close(writer)


async def test_closes_connection_on_truncated_length_prefix(server: Server) -> None:
    reader, writer = await connect(server)
    writer.write(b"\x00\x00")
    writer.write_eof()
    await assert_closed_by_server(reader)
    await close(writer)


async def test_survives_truncated_frame_and_serves_next_client(server: Server) -> None:
    reader, writer = await connect(server)
    writer.write(encode_frame(Frame(RequestType.PRODUCE, 1, b"cut"))[:-2])
    writer.write_eof()
    await assert_closed_by_server(reader)
    await close(writer)

    reader, writer = await connect(server)
    frame = Frame(RequestType.PRODUCE, 2, b"ok")
    await write_frame(writer, frame)
    assert await asyncio.wait_for(read_frame(reader), TIMEOUT) == frame
    await close(writer)


def test_port_before_start_raises() -> None:
    with pytest.raises(RuntimeError):
        _ = Server(port=0).port


async def test_serve_forever_accepts_clients() -> None:
    srv = Server(port=0)
    task = asyncio.create_task(srv.serve_forever())
    try:
        port = await asyncio.wait_for(wait_until_listening(srv), TIMEOUT)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        frame = Frame(RequestType.METADATA, 1, b"forever")
        await write_frame(writer, frame)
        assert await asyncio.wait_for(read_frame(reader), TIMEOUT) == frame
        await close(writer)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        await srv.stop()


async def test_handler_bug_closes_only_that_connection() -> None:
    srv = BrokenServer(port=0)
    await srv.start()
    try:
        reader, writer = await connect(srv)
        await write_frame(writer, Frame(RequestType.PRODUCE, 1, b"boom"))
        await assert_closed_by_server(reader)
        await close(writer)

        # The listener is still alive and accepts new connections.
        _, writer = await connect(srv)
        await close(writer)
    finally:
        await srv.stop()
