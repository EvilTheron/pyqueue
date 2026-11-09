"""On-disk format of a single log record.

Record layout (all integers big-endian):

    +----------------+---------------+----------------+---------------------+
    | length: uint32 | crc32: uint32 | offset: uint64 | timestamp_ms: int64 |
    +----------------+---------------+----------------+---------------------+
    | key_len: int32 | key           | value                                |
    +----------------+---------------+--------------------------------------+

`length` counts every byte after itself. `crc32` covers every byte after itself.
`key_len` is -1 when the record has no key; the value takes the remaining bytes.
"""

import struct
import zlib
from dataclasses import dataclass

LENGTH = struct.Struct(">I")
CRC = struct.Struct(">I")
FIELDS = struct.Struct(">Qqi")  # offset, timestamp_ms, key_len
MIN_LENGTH = CRC.size + FIELDS.size
MAX_RECORD_SIZE = 1024 * 1024
NO_KEY = -1


class RecordError(Exception):
    """Base class for records that cannot be decoded."""


class IncompleteRecordError(RecordError):
    """The buffer ends before the record does (for example, a write cut short by a crash)."""


class CorruptRecordError(RecordError):
    """The bytes are present but do not form a valid record."""


@dataclass(frozen=True, slots=True)
class Record:
    offset: int
    timestamp_ms: int
    key: bytes | None
    value: bytes


def encode_record(record: Record) -> bytes:
    """Serialise a record, including its length prefix and checksum."""
    key_len = NO_KEY if record.key is None else len(record.key)
    try:
        fields = FIELDS.pack(record.offset, record.timestamp_ms, key_len)
    except struct.error as exc:
        raise RecordError(f"invalid record fields: {exc}") from exc
    body = fields + (record.key or b"") + record.value
    length = CRC.size + len(body)
    if length > MAX_RECORD_SIZE:
        raise RecordError(f"record of {length} bytes exceeds {MAX_RECORD_SIZE}")
    return LENGTH.pack(length) + CRC.pack(zlib.crc32(body)) + body


def decode_record(data: bytes, position: int = 0) -> tuple[Record, int]:
    """Decode the record that starts at `position`.

    Return the record and its total size in bytes, so the caller can move to the next one.
    """
    if len(data) - position < LENGTH.size:
        raise IncompleteRecordError(f"no complete length prefix at position {position}")
    length: int = LENGTH.unpack_from(data, position)[0]
    if length < MIN_LENGTH or length > MAX_RECORD_SIZE:
        raise CorruptRecordError(f"invalid record length {length} at position {position}")

    start = position + LENGTH.size
    end = start + length
    if end > len(data):
        raise IncompleteRecordError(
            f"record at position {position} needs {length} bytes, {len(data) - start} available"
        )

    stored_crc: int = CRC.unpack_from(data, start)[0]
    body = data[start + CRC.size : end]
    if zlib.crc32(body) != stored_crc:
        raise CorruptRecordError(f"checksum mismatch at position {position}")

    values = FIELDS.unpack_from(body)
    offset: int = values[0]
    timestamp_ms: int = values[1]
    key_len: int = values[2]
    rest = body[FIELDS.size :]
    if key_len == NO_KEY:
        key = None
        value = rest
    elif 0 <= key_len <= len(rest):
        key = rest[:key_len]
        value = rest[key_len:]
    else:
        raise CorruptRecordError(f"invalid key length {key_len} at position {position}")

    return Record(offset, timestamp_ms, key, value), LENGTH.size + length
