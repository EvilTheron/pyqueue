"""Wire format of PyQueue frames.

Frame layout (all integers big-endian):

    +----------------+--------------+------------------------+------+
    | length: uint32 | type: uint16 | correlation id: uint32 | body |
    +----------------+--------------+------------------------+------+

`length` counts every byte after itself: type, correlation id and body.
"""

import struct
from dataclasses import dataclass
from enum import IntEnum

LENGTH = struct.Struct(">I")
HEADER = struct.Struct(">HI")
MAX_FRAME_SIZE = 1024 * 1024


class RequestType(IntEnum):
    PRODUCE = 0
    FETCH = 1
    COMMIT_OFFSET = 2
    FETCH_OFFSET = 3
    METADATA = 4


class ProtocolError(Exception):
    """Raised when bytes on the wire do not form a valid frame."""


@dataclass(frozen=True, slots=True)
class Frame:
    request_type: RequestType
    correlation_id: int
    body: bytes = b""


def encode_frame(frame: Frame) -> bytes:
    """Serialise a frame, including its length prefix."""
    try:
        header = HEADER.pack(frame.request_type, frame.correlation_id)
    except struct.error as exc:
        raise ProtocolError(f"invalid frame header: {exc}") from exc
    payload = header + frame.body
    if len(payload) > MAX_FRAME_SIZE:
        raise ProtocolError(f"frame of {len(payload)} bytes exceeds {MAX_FRAME_SIZE}")
    return LENGTH.pack(len(payload)) + payload


def decode_length(prefix: bytes) -> int:
    """Validate the 4-byte length prefix and return the payload size."""
    if len(prefix) != LENGTH.size:
        raise ProtocolError(f"length prefix must be {LENGTH.size} bytes, got {len(prefix)}")
    length: int = LENGTH.unpack(prefix)[0]
    if length < HEADER.size:
        raise ProtocolError(f"frame of {length} bytes is shorter than the header")
    if length > MAX_FRAME_SIZE:
        raise ProtocolError(f"frame of {length} bytes exceeds {MAX_FRAME_SIZE}")
    return length


def decode_payload(payload: bytes) -> Frame:
    """Parse the bytes that follow the length prefix."""
    if len(payload) < HEADER.size:
        raise ProtocolError(f"payload of {len(payload)} bytes is shorter than the header")
    values = HEADER.unpack_from(payload)
    raw_type: int = values[0]
    correlation_id: int = values[1]
    try:
        request_type = RequestType(raw_type)
    except ValueError as exc:
        raise ProtocolError(f"unknown request type {raw_type}") from exc
    return Frame(request_type, correlation_id, payload[HEADER.size :])
