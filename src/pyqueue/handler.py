"""Turns request frames into broker calls and broker results into response frames.

Broker calls block on disk (writes, fsync), so they run in worker threads with
`asyncio.to_thread`: the event loop keeps serving other connections while a partition
syncs. The broker is thread-safe for exactly this reason.

Expected failures become error codes in the response, and the connection stays open:

* a body that does not match its layout: `INVALID_REQUEST`;
* a broker refusal (unknown topic, offset out of range, ...): its own code;
* a disk or log failure: `STORAGE_ERROR`, logged on the server with the details.

Anything else is a bug: it propagates, the server logs it and closes that connection.
"""

import asyncio
import logging
from collections.abc import Callable

from pyqueue.broker import (
    Broker,
    BrokerError,
    InvalidRequestError,
    RecordTooLargeError,
    TopicExistsError,
    UnknownTopicOrPartitionError,
)
from pyqueue.log import LogError, OffsetOutOfRangeError
from pyqueue.messages import (
    BodyError,
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
    ProduceRequest,
    ProduceResponse,
    TopicMetadata,
)
from pyqueue.protocol import Frame, RequestType

logger = logging.getLogger(__name__)

# Checked in order, so a subclass must come before its base class.
_ERROR_CODES: list[tuple[type[Exception], ErrorCode]] = [
    (BodyError, ErrorCode.INVALID_REQUEST),
    (InvalidRequestError, ErrorCode.INVALID_REQUEST),
    (UnknownTopicOrPartitionError, ErrorCode.UNKNOWN_TOPIC_OR_PARTITION),
    (TopicExistsError, ErrorCode.TOPIC_ALREADY_EXISTS),
    (RecordTooLargeError, ErrorCode.RECORD_TOO_LARGE),
    (OffsetOutOfRangeError, ErrorCode.OFFSET_OUT_OF_RANGE),
    (LogError, ErrorCode.STORAGE_ERROR),
    (BrokerError, ErrorCode.STORAGE_ERROR),
    (OSError, ErrorCode.STORAGE_ERROR),
]

# Builds an error response body of the right type for each request type.
_ERROR_BODIES: dict[RequestType, Callable[[ErrorCode], bytes]] = {
    RequestType.CREATE_TOPIC: lambda code: CreateTopicResponse(code).encode(),
    RequestType.PRODUCE: lambda code: ProduceResponse(code).encode(),
    RequestType.FETCH: lambda code: FetchResponse(code).encode(),
    RequestType.COMMIT_OFFSET: lambda code: CommitOffsetResponse(code).encode(),
    RequestType.FETCH_OFFSET: lambda code: FetchOffsetResponse(code).encode(),
    RequestType.METADATA: lambda code: MetadataResponse(code).encode(),
}


def error_code_for(exc: Exception) -> ErrorCode | None:
    """Return the error code for an expected failure, or None for a bug."""
    for exc_type, code in _ERROR_CODES:
        if isinstance(exc, exc_type):
            return code
    return None


class RequestHandler:
    def __init__(self, broker: Broker) -> None:
        self._broker = broker

    async def __call__(self, frame: Frame) -> Frame:
        try:
            body = await self._dispatch(frame)
        except Exception as exc:
            code = error_code_for(exc)
            if code is None:
                raise
            if code is ErrorCode.STORAGE_ERROR:
                logger.error("storage error on %s: %r", frame.request_type.name, exc)
            else:
                logger.debug("rejected %s: %s", frame.request_type.name, exc)
            body = _ERROR_BODIES[frame.request_type](code)
        return Frame(frame.request_type, frame.correlation_id, body)

    async def _dispatch(self, frame: Frame) -> bytes:
        match frame.request_type:
            case RequestType.CREATE_TOPIC:
                return await self._create_topic(CreateTopicRequest.decode(frame.body))
            case RequestType.PRODUCE:
                return await self._produce(ProduceRequest.decode(frame.body))
            case RequestType.FETCH:
                return await self._fetch(FetchRequest.decode(frame.body))
            case RequestType.METADATA:
                return await self._metadata(MetadataRequest.decode(frame.body))
            case RequestType.COMMIT_OFFSET:
                return await self._commit_offset(CommitOffsetRequest.decode(frame.body))
            case RequestType.FETCH_OFFSET:
                return await self._fetch_offset(FetchOffsetRequest.decode(frame.body))

    async def _create_topic(self, request: CreateTopicRequest) -> bytes:
        await asyncio.to_thread(self._broker.create_topic, request.topic, request.partitions)
        return CreateTopicResponse(ErrorCode.NONE).encode()

    async def _produce(self, request: ProduceRequest) -> bytes:
        base_offset = await asyncio.to_thread(
            self._broker.produce, request.topic, request.partition, request.records
        )
        return ProduceResponse(ErrorCode.NONE, base_offset).encode()

    async def _fetch(self, request: FetchRequest) -> bytes:
        records, high_watermark = await asyncio.to_thread(
            self._broker.fetch, request.topic, request.partition, request.offset, request.max_bytes
        )
        return FetchResponse(ErrorCode.NONE, high_watermark, tuple(records)).encode()

    async def _commit_offset(self, request: CommitOffsetRequest) -> bytes:
        await asyncio.to_thread(
            self._broker.commit_offset,
            request.group,
            request.topic,
            request.partition,
            request.offset,
        )
        return CommitOffsetResponse(ErrorCode.NONE).encode()

    async def _fetch_offset(self, request: FetchOffsetRequest) -> bytes:
        offset = await asyncio.to_thread(
            self._broker.fetch_offset, request.group, request.topic, request.partition
        )
        return FetchOffsetResponse(ErrorCode.NONE, -1 if offset is None else offset).encode()

    async def _metadata(self, request: MetadataRequest) -> bytes:
        topics = await asyncio.to_thread(self._broker.topics)
        metadata = tuple(TopicMetadata(name, count) for name, count in topics.items())
        return MetadataResponse(ErrorCode.NONE, metadata).encode()
