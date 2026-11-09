from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from pyqueue.broker import (
    CREATING_SUFFIX,
    MAX_PARTITIONS,
    Broker,
    BrokerCorruptError,
    BrokerError,
    InvalidRequestError,
    RecordTooLargeError,
    TopicExistsError,
    UnknownTopicOrPartitionError,
    validate_topic_name,
)
from pyqueue.lock import DirectoryLock, DirectoryLockedError
from pyqueue.log import Log, OffsetOutOfRangeError
from pyqueue.messages import (
    MAX_RECORD_DATA_SIZE,
    ErrorCode,
    FetchResponse,
    ProduceRecord,
)
from pyqueue.protocol import Frame, RequestType, encode_frame


def records(*values: bytes) -> list[ProduceRecord]:
    return [ProduceRecord(value) for value in values]


def test_create_topic_and_list(tmp_path: Path) -> None:
    with Broker(tmp_path) as broker:
        broker.create_topic("orders", 3)
        broker.create_topic("audit", 1)
        assert broker.topics() == {"audit": 1, "orders": 3}
        assert sorted(p.name for p in (tmp_path / "orders").iterdir()) == ["0", "1", "2"]


def test_topics_survive_restart(tmp_path: Path) -> None:
    with Broker(tmp_path) as broker:
        broker.create_topic("orders", 2)
        broker.produce("orders", 1, records(b"a", b"b"))
    with Broker(tmp_path) as broker:
        assert broker.topics() == {"orders": 2}
        fetched, high_watermark = broker.fetch("orders", 1, 0, 1024)
        assert [record.value for record in fetched] == [b"a", b"b"]
        assert high_watermark == 2


def test_existing_topic_is_rejected(tmp_path: Path) -> None:
    with Broker(tmp_path) as broker:
        broker.create_topic("orders", 1)
        with pytest.raises(TopicExistsError):
            broker.create_topic("orders", 2)
    with Broker(tmp_path) as broker, pytest.raises(TopicExistsError):
        broker.create_topic("orders", 1)


@pytest.mark.parametrize(
    "name", ["orders", "a", "a.b_c-d", "_x", "-x", "x" * 249, "console", "com10", "nul-x"]
)
def test_valid_topic_names(name: str) -> None:
    validate_topic_name(name)


@pytest.mark.parametrize(
    "name",
    [
        "",
        ".",
        "..",
        "../escape",
        "a/b",
        "a\\b",
        ".hidden",
        "orders.",
        "Orders",
        "x" * 250,
        "a b",
        "тест",
        f"orders{CREATING_SUFFIX}",
        "con",
        "nul.txt",
        "com1",
        "lpt9.x.y",
    ],
)
def test_invalid_topic_names(tmp_path: Path, name: str) -> None:
    with Broker(tmp_path) as broker:
        with pytest.raises(InvalidRequestError):
            broker.create_topic(name, 1)
        assert broker.topics() == {}
    assert [p.name for p in tmp_path.iterdir()] == [".lock"]


@pytest.mark.parametrize("partitions", [0, -1, MAX_PARTITIONS + 1])
def test_invalid_partition_count(tmp_path: Path, partitions: int) -> None:
    with Broker(tmp_path) as broker, pytest.raises(InvalidRequestError):
        broker.create_topic("orders", partitions)


def test_produce_and_fetch(tmp_path: Path) -> None:
    with Broker(tmp_path) as broker:
        broker.create_topic("orders", 2)
        assert broker.produce("orders", 0, [ProduceRecord(b"a", b"k"), ProduceRecord(b"b")]) == 0
        assert broker.produce("orders", 0, records(b"c")) == 2
        assert broker.produce("orders", 1, records(b"other")) == 0

        fetched, high_watermark = broker.fetch("orders", 0, 1, 1024)
        assert [(r.offset, r.key, r.value) for r in fetched] == [(1, None, b"b"), (2, None, b"c")]
        assert high_watermark == 3
        first, _ = broker.fetch("orders", 0, 0, 1024)
        assert first[0].key == b"k"
        assert first[0].timestamp_ms == first[1].timestamp_ms  # one timestamp per batch


def test_fetch_at_the_end_is_empty(tmp_path: Path) -> None:
    with Broker(tmp_path) as broker:
        broker.create_topic("orders", 1)
        assert broker.fetch("orders", 0, 0, 1024) == ([], 0)
        broker.produce("orders", 0, records(b"a"))
        assert broker.fetch("orders", 0, 1, 1024) == ([], 1)


@pytest.mark.parametrize("offset", [-1, 2])
def test_fetch_out_of_range(tmp_path: Path, offset: int) -> None:
    with Broker(tmp_path) as broker:
        broker.create_topic("orders", 1)
        broker.produce("orders", 0, records(b"a"))
        with pytest.raises(OffsetOutOfRangeError):
            broker.fetch("orders", 0, offset, 1024)


@pytest.mark.parametrize(("topic", "partition"), [("missing", 0), ("orders", 2), ("orders", -1)])
def test_unknown_topic_or_partition(tmp_path: Path, topic: str, partition: int) -> None:
    with Broker(tmp_path) as broker:
        broker.create_topic("orders", 2)
        with pytest.raises(UnknownTopicOrPartitionError):
            broker.produce(topic, partition, records(b"a"))
        with pytest.raises(UnknownTopicOrPartitionError):
            broker.fetch(topic, partition, 0, 1024)


def test_empty_produce_is_rejected(tmp_path: Path) -> None:
    with Broker(tmp_path) as broker:
        broker.create_topic("orders", 1)
        with pytest.raises(InvalidRequestError):
            broker.produce("orders", 0, [])


@pytest.mark.parametrize("max_bytes", [0, -1])
def test_invalid_max_bytes(tmp_path: Path, max_bytes: int) -> None:
    with Broker(tmp_path) as broker:
        broker.create_topic("orders", 1)
        with pytest.raises(InvalidRequestError):
            broker.fetch("orders", 0, 0, max_bytes)


def test_too_large_record_rejects_whole_batch(tmp_path: Path) -> None:
    with Broker(tmp_path) as broker:
        broker.create_topic("orders", 1)
        batch = [ProduceRecord(b"ok"), ProduceRecord(bytes(MAX_RECORD_DATA_SIZE), b"k")]
        with pytest.raises(RecordTooLargeError):
            broker.produce("orders", 0, batch)
        assert broker.fetch("orders", 0, 0, 1024) == ([], 0)


def fetch_frame(broker: Broker, offset: int, max_bytes: int) -> tuple[int, bytes]:
    fetched, high_watermark = broker.fetch("orders", 0, offset, max_bytes)
    body = FetchResponse(ErrorCode.NONE, high_watermark, tuple(fetched)).encode()
    return len(fetched), encode_frame(Frame(RequestType.FETCH, 0, body))


def test_largest_record_can_be_fetched(tmp_path: Path) -> None:
    with Broker(tmp_path) as broker:
        broker.create_topic("orders", 1)
        broker.produce("orders", 0, [ProduceRecord(bytes(MAX_RECORD_DATA_SIZE - 1), b"k")])
        count, _ = fetch_frame(broker, 0, 1)  # encoding the frame must not fail
        assert count == 1


def test_fetch_response_always_fits_in_a_frame(tmp_path: Path) -> None:
    with Broker(tmp_path) as broker:
        broker.create_topic("orders", 1)
        for _ in range(12):
            broker.produce("orders", 0, records(bytes(100_000)))
        count, _ = fetch_frame(broker, 0, 2**31 - 1)
        assert 0 < count < 12


def test_concurrent_produce_to_one_partition(tmp_path: Path) -> None:
    with Broker(tmp_path) as broker:
        broker.create_topic("orders", 1)

        def produce(number: int) -> int:
            return broker.produce("orders", 0, records(str(number).encode(), b"second"))

        with ThreadPoolExecutor(max_workers=8) as pool:
            base_offsets = list(pool.map(produce, range(100)))

        assert sorted(base_offsets) == list(range(0, 200, 2))
        fetched, high_watermark = broker.fetch("orders", 0, 0, 2**31 - 1)
        assert high_watermark == 200
        assert [record.offset for record in fetched] == list(range(200))
        for record in fetched[1::2]:
            assert record.value == b"second"  # batches were not interleaved


def test_unfinished_creation_is_removed(tmp_path: Path) -> None:
    leftover = tmp_path / f"orders{CREATING_SUFFIX}"
    (leftover / "0").mkdir(parents=True)
    with Broker(tmp_path) as broker:
        assert broker.topics() == {}
        assert not leftover.exists()
        broker.create_topic("orders", 1)


def test_creation_cleans_up_a_stale_temporary_directory(tmp_path: Path) -> None:
    with Broker(tmp_path) as broker:
        (tmp_path / f"orders{CREATING_SUFFIX}" / "7").mkdir(parents=True)
        broker.create_topic("orders", 1)
        assert broker.topics() == {"orders": 1}
        assert not (tmp_path / f"orders{CREATING_SUFFIX}").exists()


def test_files_in_data_directory_are_ignored(tmp_path: Path) -> None:
    (tmp_path / "notes.txt").write_text("hello")
    with Broker(tmp_path) as broker:
        assert broker.topics() == {}


@pytest.mark.parametrize(
    "layout",
    [
        ["Bad Name/0"],  # not a valid topic name
        ["orders/0", "orders/2"],  # gap in partitions
        ["orders/1"],  # does not start at 0
        ["orders/0", "orders/01"],  # not a canonical number
        ["orders/"],  # no partitions
    ],
)
def test_unexpected_layout_refuses_to_start(tmp_path: Path, layout: list[str]) -> None:
    for path in layout:
        (tmp_path / path).mkdir(parents=True, exist_ok=True)
    before = sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*"))
    with pytest.raises(BrokerCorruptError):
        Broker(tmp_path)
    after = sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*") if p.name != ".lock")
    assert after == before  # nothing was touched
    with DirectoryLock(tmp_path):  # the failed broker released its lock
        pass


def test_file_instead_of_partition_refuses_to_start(tmp_path: Path) -> None:
    (tmp_path / "orders").mkdir()
    (tmp_path / "orders" / "0").write_text("not a directory")
    with pytest.raises(BrokerCorruptError, match="file"):
        Broker(tmp_path)


def test_failed_partition_closes_opened_ones(tmp_path: Path) -> None:
    with Broker(tmp_path) as broker:
        broker.create_topic("orders", 3)
    with Log(tmp_path / "orders" / "2"), pytest.raises(DirectoryLockedError):
        Broker(tmp_path)
    # Partitions 0 and 1 and the data directory were unlocked again.
    with Broker(tmp_path) as broker:
        assert broker.topics() == {"orders": 3}


def test_second_broker_is_rejected(tmp_path: Path) -> None:
    with Broker(tmp_path), pytest.raises(DirectoryLockedError):
        Broker(tmp_path)


def test_closed_broker_rejects_operations(tmp_path: Path) -> None:
    broker = Broker(tmp_path)
    broker.create_topic("orders", 1)
    broker.close()
    broker.close()  # idempotent
    assert broker.closed
    with pytest.raises(BrokerError, match="closed"):
        broker.topics()
    with pytest.raises(BrokerError, match="closed"):
        broker.create_topic("audit", 1)
    with pytest.raises(BrokerError, match="closed"):
        broker.produce("orders", 0, records(b"a"))


def test_close_closes_every_log_and_raises_the_first_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    broker = Broker(tmp_path)
    broker.create_topic("orders", 2)
    real_close = Log.close
    closed: list[Path] = []

    def failing_close(self: Log) -> None:
        closed.append(self.directory)
        real_close(self)
        raise OSError("disk went away")

    monkeypatch.setattr(Log, "close", failing_close)
    with pytest.raises(OSError, match="disk went away"):
        broker.close()
    assert len(closed) == 2
    monkeypatch.undo()
    Broker(tmp_path).close()  # the directory lock was released anyway
