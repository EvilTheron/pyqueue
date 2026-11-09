"""Bodies of PyQueue requests and responses.

A response frame repeats the request type and correlation id of its request. Every
response body starts with an error code; the other fields are meaningful only when it is
`ErrorCode.NONE`.

Primitive types (all integers big-endian):

* `uint16`, `int32`, `int64`
* string: `uint16` length followed by that many bytes of UTF-8
* bytes: `int32` length followed by that many bytes; length `-1` means null
* array: `int32` count followed by that many items

Bodies:

    CreateTopic   request:  topic: string, partitions: int32
                  response: error: uint16
    Produce       request:  topic: string, partition: int32,
                            records: array of (key: bytes, value: bytes)
                  response: error: uint16, base_offset: int64
    Fetch         request:  topic: string, partition: int32, offset: int64, max_bytes: int32
                  response: error: uint16, high_watermark: int64,
                            records: array of (offset: int64, timestamp_ms: int64,
                                               key: bytes, value: bytes)
    CommitOffset  request:  group: string, topic: string, partition: int32, offset: int64
                  response: error: uint16
    FetchOffset   request:  group: string, topic: string, partition: int32
                  response: error: uint16, offset: int64 (-1 if nothing committed)
    Metadata      request:  empty
                  response: error: uint16, topics: array of (name: string, partitions: int32)

Decoding checks only the layout. Whether a topic exists or an offset makes sense is
decided by the broker.
"""

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Self

from pyqueue.protocol import HEADER, MAX_FRAME_SIZE
from pyqueue.record import Record

_U16 = struct.Struct(">H")
_I32 = struct.Struct(">i")
_I64 = struct.Struct(">q")
NULL_LENGTH = -1
MAX_STRING_SIZE = 0xFFFF

# Fixed part of a Fetch response body: error, high watermark, record count.
FETCH_RESPONSE_FIXED_SIZE = _U16.size + _I64.size + _I32.size
# Fixed part of one record in a Fetch response: offset, timestamp, key and value lengths.
FETCH_RECORD_FIXED_SIZE = 2 * _I64.size + 2 * _I32.size
# Room for records in one Fetch response frame.
MAX_FETCH_RECORDS_SIZE = MAX_FRAME_SIZE - HEADER.size - FETCH_RESPONSE_FIXED_SIZE
# Largest key + value that can always be returned by Fetch, alone in a response.
MAX_RECORD_DATA_SIZE = MAX_FETCH_RECORDS_SIZE - FETCH_RECORD_FIXED_SIZE


class ErrorCode(IntEnum):
    NONE = 0
    INVALID_REQUEST = 1
    UNKNOWN_TOPIC_OR_PARTITION = 2
    TOPIC_ALREADY_EXISTS = 3
    OFFSET_OUT_OF_RANGE = 4
    RECORD_TOO_LARGE = 5
    STORAGE_ERROR = 6


class BodyError(Exception):
    """Raised when a body does not match the layout of its message.

    Unlike `ProtocolError`, the frame boundaries are intact, so the connection can stay
    open and the server answers with `ErrorCode.INVALID_REQUEST`.
    """


class _Writer:
    def __init__(self) -> None:
        self._parts: list[bytes] = []

    def _pack(self, fmt: struct.Struct, value: int) -> None:
        try:
            self._parts.append(fmt.pack(value))
        except struct.error as exc:
            raise BodyError(f"value {value} does not fit: {exc}") from exc

    def u16(self, value: int) -> None:
        self._pack(_U16, value)

    def i32(self, value: int) -> None:
        self._pack(_I32, value)

    def i64(self, value: int) -> None:
        self._pack(_I64, value)

    def string(self, value: str) -> None:
        data = value.encode()
        if len(data) > MAX_STRING_SIZE:
            raise BodyError(f"string of {len(data)} bytes exceeds {MAX_STRING_SIZE}")
        self.u16(len(data))
        self._parts.append(data)

    def nullable_bytes(self, value: bytes | None) -> None:
        if value is None:
            self.i32(NULL_LENGTH)
            return
        self.i32(len(value))
        self._parts.append(value)

    def getvalue(self) -> bytes:
        return b"".join(self._parts)


class _Reader:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._position = 0

    def _take(self, size: int) -> bytes:
        end = self._position + size
        if end > len(self._data):
            raise BodyError(
                f"body ends at byte {len(self._data)}, needed {size} bytes at {self._position}"
            )
        chunk = self._data[self._position : end]
        self._position = end
        return chunk

    def _unpack(self, fmt: struct.Struct) -> int:
        value: int = fmt.unpack(self._take(fmt.size))[0]
        return value

    def u16(self) -> int:
        return self._unpack(_U16)

    def i32(self) -> int:
        return self._unpack(_I32)

    def i64(self) -> int:
        return self._unpack(_I64)

    def string(self) -> str:
        raw = self._take(self.u16())
        try:
            return raw.decode()
        except UnicodeDecodeError as exc:
            raise BodyError(f"string is not valid UTF-8: {exc}") from exc

    def nullable_bytes(self) -> bytes | None:
        length = self.i32()
        if length == NULL_LENGTH:
            return None
        if length < 0:
            raise BodyError(f"invalid bytes length {length}")
        return self._take(length)

    def required_bytes(self) -> bytes:
        value = self.nullable_bytes()
        if value is None:
            raise BodyError("bytes must not be null here")
        return value

    def count(self) -> int:
        count = self.i32()
        if count < 0:
            raise BodyError(f"invalid array count {count}")
        return count

    def error_code(self) -> ErrorCode:
        raw = self.u16()
        try:
            return ErrorCode(raw)
        except ValueError as exc:
            raise BodyError(f"unknown error code {raw}") from exc

    def finish(self) -> None:
        if self._position != len(self._data):
            raise BodyError(f"{len(self._data) - self._position} unexpected bytes after body")


@dataclass(frozen=True, slots=True)
class CreateTopicRequest:
    topic: str
    partitions: int

    def encode(self) -> bytes:
        w = _Writer()
        w.string(self.topic)
        w.i32(self.partitions)
        return w.getvalue()

    @classmethod
    def decode(cls, body: bytes) -> Self:
        r = _Reader(body)
        message = cls(r.string(), r.i32())
        r.finish()
        return message


@dataclass(frozen=True, slots=True)
class CreateTopicResponse:
    error: ErrorCode

    def encode(self) -> bytes:
        w = _Writer()
        w.u16(self.error)
        return w.getvalue()

    @classmethod
    def decode(cls, body: bytes) -> Self:
        r = _Reader(body)
        message = cls(r.error_code())
        r.finish()
        return message


@dataclass(frozen=True, slots=True)
class ProduceRecord:
    value: bytes
    key: bytes | None = None


@dataclass(frozen=True, slots=True)
class ProduceRequest:
    topic: str
    partition: int
    records: tuple[ProduceRecord, ...]

    def encode(self) -> bytes:
        w = _Writer()
        w.string(self.topic)
        w.i32(self.partition)
        w.i32(len(self.records))
        for record in self.records:
            w.nullable_bytes(record.key)
            w.nullable_bytes(record.value)
        return w.getvalue()

    @classmethod
    def decode(cls, body: bytes) -> Self:
        r = _Reader(body)
        topic = r.string()
        partition = r.i32()
        records = []
        for _ in range(r.count()):
            key = r.nullable_bytes()
            records.append(ProduceRecord(r.required_bytes(), key))
        r.finish()
        return cls(topic, partition, tuple(records))


@dataclass(frozen=True, slots=True)
class ProduceResponse:
    error: ErrorCode
    base_offset: int = -1

    def encode(self) -> bytes:
        w = _Writer()
        w.u16(self.error)
        w.i64(self.base_offset)
        return w.getvalue()

    @classmethod
    def decode(cls, body: bytes) -> Self:
        r = _Reader(body)
        message = cls(r.error_code(), r.i64())
        r.finish()
        return message


@dataclass(frozen=True, slots=True)
class FetchRequest:
    topic: str
    partition: int
    offset: int
    max_bytes: int

    def encode(self) -> bytes:
        w = _Writer()
        w.string(self.topic)
        w.i32(self.partition)
        w.i64(self.offset)
        w.i32(self.max_bytes)
        return w.getvalue()

    @classmethod
    def decode(cls, body: bytes) -> Self:
        r = _Reader(body)
        message = cls(r.string(), r.i32(), r.i64(), r.i32())
        r.finish()
        return message


@dataclass(frozen=True, slots=True)
class FetchResponse:
    error: ErrorCode
    high_watermark: int = -1
    records: tuple[Record, ...] = ()

    def encode(self) -> bytes:
        w = _Writer()
        w.u16(self.error)
        w.i64(self.high_watermark)
        w.i32(len(self.records))
        for record in self.records:
            w.i64(record.offset)
            w.i64(record.timestamp_ms)
            w.nullable_bytes(record.key)
            w.nullable_bytes(record.value)
        return w.getvalue()

    @classmethod
    def decode(cls, body: bytes) -> Self:
        r = _Reader(body)
        error = r.error_code()
        high_watermark = r.i64()
        records = []
        for _ in range(r.count()):
            offset = r.i64()
            timestamp_ms = r.i64()
            key = r.nullable_bytes()
            records.append(Record(offset, timestamp_ms, key, r.required_bytes()))
        r.finish()
        return cls(error, high_watermark, tuple(records))


@dataclass(frozen=True, slots=True)
class CommitOffsetRequest:
    group: str
    topic: str
    partition: int
    offset: int

    def encode(self) -> bytes:
        w = _Writer()
        w.string(self.group)
        w.string(self.topic)
        w.i32(self.partition)
        w.i64(self.offset)
        return w.getvalue()

    @classmethod
    def decode(cls, body: bytes) -> Self:
        r = _Reader(body)
        message = cls(r.string(), r.string(), r.i32(), r.i64())
        r.finish()
        return message


@dataclass(frozen=True, slots=True)
class CommitOffsetResponse:
    error: ErrorCode

    def encode(self) -> bytes:
        w = _Writer()
        w.u16(self.error)
        return w.getvalue()

    @classmethod
    def decode(cls, body: bytes) -> Self:
        r = _Reader(body)
        message = cls(r.error_code())
        r.finish()
        return message


@dataclass(frozen=True, slots=True)
class FetchOffsetRequest:
    group: str
    topic: str
    partition: int

    def encode(self) -> bytes:
        w = _Writer()
        w.string(self.group)
        w.string(self.topic)
        w.i32(self.partition)
        return w.getvalue()

    @classmethod
    def decode(cls, body: bytes) -> Self:
        r = _Reader(body)
        message = cls(r.string(), r.string(), r.i32())
        r.finish()
        return message


@dataclass(frozen=True, slots=True)
class FetchOffsetResponse:
    error: ErrorCode
    offset: int = -1

    def encode(self) -> bytes:
        w = _Writer()
        w.u16(self.error)
        w.i64(self.offset)
        return w.getvalue()

    @classmethod
    def decode(cls, body: bytes) -> Self:
        r = _Reader(body)
        message = cls(r.error_code(), r.i64())
        r.finish()
        return message


@dataclass(frozen=True, slots=True)
class MetadataRequest:
    def encode(self) -> bytes:
        return b""

    @classmethod
    def decode(cls, body: bytes) -> Self:
        _Reader(body).finish()
        return cls()


@dataclass(frozen=True, slots=True)
class TopicMetadata:
    name: str
    partitions: int


@dataclass(frozen=True, slots=True)
class MetadataResponse:
    error: ErrorCode
    topics: tuple[TopicMetadata, ...] = ()

    def encode(self) -> bytes:
        w = _Writer()
        w.u16(self.error)
        w.i32(len(self.topics))
        for topic in self.topics:
            w.string(topic.name)
            w.i32(topic.partitions)
        return w.getvalue()

    @classmethod
    def decode(cls, body: bytes) -> Self:
        r = _Reader(body)
        error = r.error_code()
        topics = [TopicMetadata(r.string(), r.i32()) for _ in range(r.count())]
        r.finish()
        return cls(error, tuple(topics))
