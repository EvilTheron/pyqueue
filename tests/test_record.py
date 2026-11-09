import struct
import zlib

import pytest

from pyqueue.record import (
    MAX_RECORD_SIZE,
    CorruptRecordError,
    IncompleteRecordError,
    Record,
    RecordError,
    decode_record,
    encode_record,
)

RECORD = Record(offset=7, timestamp_ms=1_700_000_000_000, key=b"user-1", value=b"hello")


def test_encode_layout() -> None:
    data = encode_record(RECORD)
    body = struct.pack(">Qqi", 7, 1_700_000_000_000, 6) + b"user-1" + b"hello"
    assert data == struct.pack(">II", 4 + len(body), zlib.crc32(body)) + body


@pytest.mark.parametrize(
    "record",
    [
        RECORD,
        Record(0, 0, None, b"no key"),
        Record(1, 0, b"", b"empty key"),
        Record(2, -1, b"key", b""),
        Record(2**64 - 1, 2**63 - 1, None, bytes(range(256)) * 100),
    ],
)
def test_roundtrip(record: Record) -> None:
    data = encode_record(record)
    assert decode_record(data) == (record, len(data))


def test_decodes_records_one_after_another() -> None:
    records = [Record(i, i, None, f"value {i}".encode()) for i in range(3)]
    data = b"".join(encode_record(r) for r in records)
    position = 0
    decoded = []
    while position < len(data):
        record, size = decode_record(data, position)
        decoded.append(record)
        position += size
    assert decoded == records


@pytest.mark.parametrize("cut", range(len(encode_record(RECORD))))
def test_truncated_record_is_incomplete(cut: int) -> None:
    with pytest.raises(IncompleteRecordError):
        decode_record(encode_record(RECORD)[:cut])


@pytest.mark.parametrize("index", range(4, len(encode_record(RECORD))))
def test_any_flipped_byte_after_length_is_detected(index: int) -> None:
    data = bytearray(encode_record(RECORD))
    data[index] ^= 0xFF
    with pytest.raises(CorruptRecordError):
        decode_record(bytes(data))


@pytest.mark.parametrize("length", [0, 19, MAX_RECORD_SIZE + 1, 2**32 - 1])
def test_invalid_length_is_corrupt(length: int) -> None:
    data = struct.pack(">I", length) + bytes(64)
    with pytest.raises(CorruptRecordError):
        decode_record(data)


@pytest.mark.parametrize("key_len", [-2, 100])
def test_invalid_key_length_is_corrupt(key_len: int) -> None:
    # A valid checksum over a bad key length: only the key length check can catch it.
    body = struct.pack(">Qqi", 0, 0, key_len) + b"abc"
    data = struct.pack(">II", 4 + len(body), zlib.crc32(body)) + body
    with pytest.raises(CorruptRecordError):
        decode_record(data)


@pytest.mark.parametrize("offset", [-1, 2**64])
def test_encode_rejects_out_of_range_offset(offset: int) -> None:
    with pytest.raises(RecordError):
        encode_record(Record(offset, 0, None, b""))


def test_encode_rejects_oversized_record() -> None:
    with pytest.raises(RecordError):
        encode_record(Record(0, 0, None, bytes(MAX_RECORD_SIZE)))
