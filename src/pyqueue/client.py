"""Asyncio client for the PyQueue frame protocol.

`request()` sends raw frames. The typed methods (`create_topic`, `produce`, `fetch`,
`commit_offset`, `fetch_offset`, `metadata`) encode the request body, decode the response
and raise `RequestFailedError` if the broker answered with an error code.
"""

import asyncio
import contextlib
import logging
from collections.abc import Sequence
from types import TracebackType
from typing import Self

from pyqueue.messages import (
    MAX_FETCH_RECORDS_SIZE,
    CommitOffsetRequest,
    CommitOffsetResponse,
    CreateTopicRequest,
    CreateTopicResponse,
    ErrorCode,
    FetchOffsetRequest,
    FetchOffsetResponse,
    FetchRequest,
    FetchResponse,
    MetadataRequest,
    MetadataResponse,
    ProduceRecord,
    ProduceRequest,
    ProduceResponse,
)
from pyqueue.protocol import Frame, ProtocolError, RequestType
from pyqueue.record import Record
from pyqueue.transport import read_frame, write_frame

logger = logging.getLogger(__name__)

MAX_CORRELATION_ID = 2**32


class RequestFailedError(Exception):
    """The broker answered with an error code."""

    def __init__(self, request_type: RequestType, error: ErrorCode) -> None:
        super().__init__(f"{request_type.name} failed: {error.name}")
        self.request_type = request_type
        self.error = error


def _check(request_type: RequestType, error: ErrorCode) -> None:
    if error is not ErrorCode.NONE:
        raise RequestFailedError(request_type, error)


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

    async def create_topic(self, topic: str, partitions: int) -> None:
        frame = await self.request(
            RequestType.CREATE_TOPIC, CreateTopicRequest(topic, partitions).encode()
        )
        _check(RequestType.CREATE_TOPIC, CreateTopicResponse.decode(frame.body).error)

    async def produce(self, topic: str, partition: int, records: Sequence[ProduceRecord]) -> int:
        """Send records to a partition and return the offset of the first one."""
        body = ProduceRequest(topic, partition, tuple(records)).encode()
        response = ProduceResponse.decode((await self.request(RequestType.PRODUCE, body)).body)
        _check(RequestType.PRODUCE, response.error)
        return response.base_offset

    async def fetch(
        self, topic: str, partition: int, offset: int, max_bytes: int = MAX_FETCH_RECORDS_SIZE
    ) -> tuple[list[Record], int]:
        """Read records from `offset`. Return them and the partition's next offset."""
        body = FetchRequest(topic, partition, offset, max_bytes).encode()
        response = FetchResponse.decode((await self.request(RequestType.FETCH, body)).body)
        _check(RequestType.FETCH, response.error)
        return list(response.records), response.high_watermark

    async def commit_offset(self, group: str, topic: str, partition: int, offset: int) -> None:
        """Store the offset `group` will read next from a partition."""
        body = CommitOffsetRequest(group, topic, partition, offset).encode()
        frame = await self.request(RequestType.COMMIT_OFFSET, body)
        _check(RequestType.COMMIT_OFFSET, CommitOffsetResponse.decode(frame.body).error)

    async def fetch_offset(self, group: str, topic: str, partition: int) -> int | None:
        """Return the committed offset of `group`, or None if it has not committed one."""
        body = FetchOffsetRequest(group, topic, partition).encode()
        frame = await self.request(RequestType.FETCH_OFFSET, body)
        response = FetchOffsetResponse.decode(frame.body)
        _check(RequestType.FETCH_OFFSET, response.error)
        return None if response.offset == -1 else response.offset

    async def metadata(self) -> dict[str, int]:
        """Return every topic with its number of partitions."""
        frame = await self.request(RequestType.METADATA, MetadataRequest().encode())
        response = MetadataResponse.decode(frame.body)
        _check(RequestType.METADATA, response.error)
        return {topic.name: topic.partitions for topic in response.topics}

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
