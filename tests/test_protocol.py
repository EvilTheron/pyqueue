import pytest

from pyqueue.protocol import (
    HEADER,
    LENGTH,
    MAX_FRAME_SIZE,
    Frame,
    ProtocolError,
    RequestType,
    decode_length,
    decode_payload,
    encode_frame,
)


def roundtrip(frame: Frame) -> Frame:
    data = encode_frame(frame)
    length = decode_length(data[: LENGTH.size])
    payload = data[LENGTH.size :]
    assert len(payload) == length
    return decode_payload(payload)


def test_encode_layout() -> None:
    data = encode_frame(Frame(RequestType.PRODUCE, 7, b"hi"))
    assert data == b"\x00\x00\x00\x08" + b"\x00\x00" + b"\x00\x00\x00\x07" + b"hi"


@pytest.mark.parametrize("body", [b"", b"hello", bytes(1000)])
def test_roundtrip(body: bytes) -> None:
    frame = Frame(RequestType.FETCH, 42, body)
    assert roundtrip(frame) == frame


def test_decode_length_rejects_wrong_prefix_size() -> None:
    with pytest.raises(ProtocolError):
        decode_length(b"\x00\x00\x08")


def test_decode_length_rejects_frame_shorter_than_header() -> None:
    with pytest.raises(ProtocolError):
        decode_length(LENGTH.pack(HEADER.size - 1))


def test_decode_length_rejects_oversized_frame() -> None:
    with pytest.raises(ProtocolError):
        decode_length(LENGTH.pack(MAX_FRAME_SIZE + 1))


def test_decode_payload_rejects_unknown_type() -> None:
    with pytest.raises(ProtocolError):
        decode_payload(HEADER.pack(999, 1))


def test_decode_payload_rejects_truncated_header() -> None:
    with pytest.raises(ProtocolError):
        decode_payload(b"\x00\x01")


def test_encode_rejects_oversized_body() -> None:
    with pytest.raises(ProtocolError):
        encode_frame(Frame(RequestType.PRODUCE, 1, bytes(MAX_FRAME_SIZE)))


@pytest.mark.parametrize("correlation_id", [-1, 2**32])
def test_encode_rejects_out_of_range_correlation_id(correlation_id: int) -> None:
    with pytest.raises(ProtocolError):
        encode_frame(Frame(RequestType.PRODUCE, correlation_id))
