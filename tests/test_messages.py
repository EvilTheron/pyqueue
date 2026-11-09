import pytest

from pyqueue.messages import (
    MAX_RECORD_DATA_SIZE,
    MAX_STRING_SIZE,
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
    ProduceRecord,
    ProduceRequest,
    ProduceResponse,
    TopicMetadata,
)
from pyqueue.protocol import Frame, ProtocolError, RequestType, encode_frame
from pyqueue.record import Record

Message = (
    CreateTopicRequest
    | CreateTopicResponse
    | ProduceRequest
    | ProduceResponse
    | FetchRequest
    | FetchResponse
    | CommitOffsetRequest
    | CommitOffsetResponse
    | FetchOffsetRequest
    | FetchOffsetResponse
    | MetadataRequest
    | MetadataResponse
)

MESSAGES: list[Message] = [
    CreateTopicRequest("orders", 3),
    CreateTopicResponse(ErrorCode.TOPIC_ALREADY_EXISTS),
    ProduceRequest("orders", 1, (ProduceRecord(b"v1", b"k1"), ProduceRecord(b""))),
    ProduceRequest("orders", 0, ()),
    ProduceResponse(ErrorCode.NONE, 42),
    ProduceResponse(ErrorCode.STORAGE_ERROR),
    FetchRequest("orders", 2, 10, 4096),
    FetchResponse(
        ErrorCode.NONE, 12, (Record(10, 1_700_000_000_000, None, b"a"), Record(11, 1, b"k", b""))
    ),
    FetchResponse(ErrorCode.OFFSET_OUT_OF_RANGE),
    CommitOffsetRequest("billing", "orders", 0, 7),
    CommitOffsetResponse(ErrorCode.NONE),
    FetchOffsetRequest("billing", "orders", 0),
    FetchOffsetResponse(ErrorCode.NONE, 7),
    FetchOffsetResponse(ErrorCode.NONE),
    MetadataRequest(),
    MetadataResponse(ErrorCode.NONE, (TopicMetadata("orders", 3), TopicMetadata("логи", 1))),
    MetadataResponse(ErrorCode.NONE),
]


@pytest.mark.parametrize("message", MESSAGES, ids=repr)
def test_round_trip(message: Message) -> None:
    assert type(message).decode(message.encode()) == message


@pytest.mark.parametrize("message", [m for m in MESSAGES if m.encode()], ids=repr)
def test_every_truncation_is_rejected(message: Message) -> None:
    body = message.encode()
    for size in range(len(body)):
        with pytest.raises(BodyError):
            type(message).decode(body[:size])


@pytest.mark.parametrize("message", MESSAGES, ids=repr)
def test_trailing_bytes_are_rejected(message: Message) -> None:
    with pytest.raises(BodyError, match="unexpected bytes"):
        type(message).decode(message.encode() + b"\x00")


def test_produce_request_layout() -> None:
    body = ProduceRequest("ab", 1, (ProduceRecord(b"v", b"k"), ProduceRecord(b"w"))).encode()
    assert body == (
        b"\x00\x02ab"  # topic
        b"\x00\x00\x00\x01"  # partition
        b"\x00\x00\x00\x02"  # record count
        b"\x00\x00\x00\x01k\x00\x00\x00\x01v"  # key, value
        b"\xff\xff\xff\xff\x00\x00\x00\x01w"  # null key, value
    )


def test_fetch_response_layout() -> None:
    body = FetchResponse(ErrorCode.NONE, 6, (Record(5, 9, None, b"x"),)).encode()
    assert body == (
        b"\x00\x00"  # error
        b"\x00\x00\x00\x00\x00\x00\x00\x06"  # high watermark
        b"\x00\x00\x00\x01"  # record count
        b"\x00\x00\x00\x00\x00\x00\x00\x05"  # offset
        b"\x00\x00\x00\x00\x00\x00\x00\x09"  # timestamp
        b"\xff\xff\xff\xff"  # null key
        b"\x00\x00\x00\x01x"  # value
    )


def test_null_value_is_rejected() -> None:
    body = b"\x00\x01t" + b"\x00\x00\x00\x00" + b"\x00\x00\x00\x01" + b"\xff" * 8
    with pytest.raises(BodyError, match="must not be null"):
        ProduceRequest.decode(body)


def test_negative_bytes_length_is_rejected() -> None:
    body = b"\x00\x01t" + b"\x00\x00\x00\x00" + b"\x00\x00\x00\x01" + b"\xff\xff\xff\xfe"
    with pytest.raises(BodyError, match="invalid bytes length -2"):
        ProduceRequest.decode(body)


def test_negative_count_is_rejected() -> None:
    body = b"\x00\x01t" + b"\x00\x00\x00\x00" + b"\xff\xff\xff\xff"
    with pytest.raises(BodyError, match="invalid array count -1"):
        ProduceRequest.decode(body)


def test_huge_count_without_items_is_rejected() -> None:
    body = b"\x00\x01t" + b"\x00\x00\x00\x00" + b"\x7f\xff\xff\xff"
    with pytest.raises(BodyError):
        ProduceRequest.decode(body)


def test_invalid_utf8_is_rejected() -> None:
    with pytest.raises(BodyError, match="UTF-8"):
        CreateTopicRequest.decode(b"\x00\x01\xff\x00\x00\x00\x01")


def test_unknown_error_code_is_rejected() -> None:
    with pytest.raises(BodyError, match="unknown error code 999"):
        CreateTopicResponse.decode(b"\x03\xe7")


def test_too_long_string_is_rejected() -> None:
    with pytest.raises(BodyError, match="exceeds"):
        CreateTopicRequest("x" * (MAX_STRING_SIZE + 1), 1).encode()


def test_out_of_range_integer_is_rejected() -> None:
    with pytest.raises(BodyError, match="does not fit"):
        CreateTopicRequest("orders", 2**31).encode()


def test_largest_record_fits_in_one_fetch_frame() -> None:
    def frame(data_size: int) -> bytes:
        record = Record(0, 0, None, b"x" * data_size)
        body = FetchResponse(ErrorCode.NONE, 1, (record,)).encode()
        return encode_frame(Frame(RequestType.FETCH, 0, body))

    frame(MAX_RECORD_DATA_SIZE)
    with pytest.raises(ProtocolError):
        frame(MAX_RECORD_DATA_SIZE + 1)
