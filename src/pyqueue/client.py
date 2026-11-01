"""Asyncio client for the PyQueue frame protocol."""

import asyncio
import contextlib
import logging
from types import TracebackType
from typing import Self

from pyqueue.protocol import Frame, ProtocolError, RequestType
from pyqueue.transport import read_frame, write_frame

logger = logging.getLogger(__name__)

MAX_CORRELATION_ID = 2**32


class Client:
    def __init__(self, host: str = "127.0.0.1", port: int = 9092, timeout: float = 5.0) -> None:
        self._host = host
        self._port = port
        self._timeout = timeout
        self._writer: asyncio.StreamWriter | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._pending: dict[int, asyncio.Future[Frame]] = {}
        self._next_correlation_id = 0
        self._write_lock = asyncio.Lock()
        self._connection_error: Exception | None = None

    async def connect(self) -> None:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(self._host, self._port), self._timeout
        )
        self._writer = writer
        self._connection_error = None
        self._reader_task = asyncio.create_task(self._read_responses(reader))

    async def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            with contextlib.suppress(ConnectionError):
                await self._writer.wait_closed()
            self._writer = None
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def request(self, request_type: RequestType, body: bytes = b"") -> Frame:
        """Send a request and wait for the response with the same correlation id."""
        if self._writer is None:
            raise RuntimeError("client is not connected")
        if self._connection_error is not None:
            raise ConnectionError("connection is broken") from self._connection_error

        correlation_id = self._next_correlation_id
        self._next_correlation_id = (correlation_id + 1) % MAX_CORRELATION_ID
        future: asyncio.Future[Frame] = asyncio.get_running_loop().create_future()
        self._pending[correlation_id] = future
        try:
            async with self._write_lock:
                await write_frame(self._writer, Frame(request_type, correlation_id, body))
            return await asyncio.wait_for(future, self._timeout)
        finally:
            self._pending.pop(correlation_id, None)

    async def _read_responses(self, reader: asyncio.StreamReader) -> None:
        error: Exception = ConnectionError("connection closed")
        try:
            while (frame := await read_frame(reader)) is not None:
                future = self._pending.get(frame.correlation_id)
                if future is None or future.done():
                    logger.warning(
                        "dropping response with unknown correlation id %d", frame.correlation_id
                    )
                    continue
                future.set_result(frame)
        except (ProtocolError, ConnectionError) as exc:
            error = exc
        finally:
            self._connection_error = error
            for pending in self._pending.values():
                if not pending.done():
                    pending.set_exception(error)
