import asyncio
import threading
import time
from collections.abc import Coroutine
from pathlib import Path

import pytest

from pyqueue.broker import Broker, BrokerError
from pyqueue.client import Client, RequestFailedError
from pyqueue.handler import _ERROR_BODIES, RequestHandler, error_code_for
from pyqueue.log import LogError, LogFailedError, OffsetOutOfRangeError
from pyqueue.messages import (
    MAX_RECORD_DATA_SIZE,
    CreateTopicResponse,
    ErrorCode,
    ProduceRecord,
)
from pyqueue.protocol import Frame, RequestType
from pyqueue.server import Server


async def test_create_produce_fetch_metadata(server: Server) -> None:
    async with Client(port=server.port) as client:
        await client.create_topic("orders", 2)
        assert await client.metadata() == {"orders": 2}
        batch = [ProduceRecord(b"a", b"k"), ProduceRecord(b"b")]
        assert await client.produce("orders", 1, batch) == 0
        assert await client.produce("orders", 1, [ProduceRecord(b"c")]) == 2

        records, high_watermark = await client.fetch("orders", 1, 0)
        assert [(r.offset, r.key, r.value) for r in records] == [
            (0, b"k", b"a"),
            (1, None, b"b"),
            (2, None, b"c"),
        ]
        assert high_watermark == 3
        assert await client.fetch("orders", 0, 0) == ([], 0)


async def test_data_survives_server_restart(tmp_path: Path) -> None:
    for round_number in range(2):
        with Broker(tmp_path) as broker:
            srv = Server(RequestHandler(broker), port=0)
            await srv.start()
            try:
                async with Client(port=srv.port) as client:
                    if round_number == 0:
                        await client.create_topic("orders", 1)
                        await client.produce("orders", 0, [ProduceRecord(b"kept")])
                    else:
                        records, _ = await client.fetch("orders", 0, 0)
                        assert [r.value for r in records] == [b"kept"]
            finally:
                await srv.stop()


async def expect_error(request: Coroutine[object, object, object], code: ErrorCode) -> None:
    with pytest.raises(RequestFailedError) as info:
        await request
    assert info.value.error is code


async def test_broker_refusals_become_error_codes(server: Server) -> None:
    async with Client(port=server.port) as client:
        await client.create_topic("orders", 1)
        await client.produce("orders", 0, [ProduceRecord(b"a")])

        await expect_error(client.create_topic("orders", 1), ErrorCode.TOPIC_ALREADY_EXISTS)
        await expect_error(client.create_topic("../escape", 1), ErrorCode.INVALID_REQUEST)
        await expect_error(client.create_topic("big", 0), ErrorCode.INVALID_REQUEST)
        await expect_error(
            client.produce("missing", 0, [ProduceRecord(b"a")]),
            ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
        )
        await expect_error(
            client.produce("orders", 1, [ProduceRecord(b"a")]),
            ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
        )
        await expect_error(client.produce("orders", 0, []), ErrorCode.INVALID_REQUEST)
        await expect_error(client.fetch("orders", 0, 5), ErrorCode.OFFSET_OUT_OF_RANGE)
        await expect_error(client.fetch("orders", 0, 0, 0), ErrorCode.INVALID_REQUEST)

        # The connection survived every refusal.
        records, _ = await client.fetch("orders", 0, 0)
        assert [r.value for r in records] == [b"a"]


async def test_too_large_record_is_rejected(server: Server) -> None:
    async with Client(port=server.port) as client:
        await client.create_topic("orders", 1)
        # Fits in a Produce frame, but would not fit in a Fetch response frame.
        record = ProduceRecord(bytes(MAX_RECORD_DATA_SIZE + 1))
        await expect_error(client.produce("orders", 0, [record]), ErrorCode.RECORD_TOO_LARGE)
        assert await client.fetch("orders", 0, 0) == ([], 0)


async def test_malformed_body_keeps_connection_open(server: Server) -> None:
    async with Client(port=server.port) as client:
        response = await client.request(RequestType.CREATE_TOPIC, b"\x00\x05ab")
        assert CreateTopicResponse.decode(response.body).error is ErrorCode.INVALID_REQUEST
        response = await client.request(RequestType.METADATA, b"unexpected body")
        assert response.body[:2] == b"\x00\x01"  # INVALID_REQUEST
        assert await client.metadata() == {}


async def test_commit_and_fetch_offset(server: Server) -> None:
    async with Client(port=server.port) as client:
        await client.create_topic("orders", 1)
        await client.produce("orders", 0, [ProduceRecord(b"a"), ProduceRecord(b"b")])
        assert await client.fetch_offset("billing", "orders", 0) is None
        await client.commit_offset("billing", "orders", 0, 2)
        assert await client.fetch_offset("billing", "orders", 0) == 2

        await expect_error(
            client.commit_offset("billing", "orders", 0, 3), ErrorCode.OFFSET_OUT_OF_RANGE
        )
        await expect_error(client.commit_offset("", "orders", 0, 0), ErrorCode.INVALID_REQUEST)
        await expect_error(
            client.fetch_offset("billing", "missing", 0), ErrorCode.UNKNOWN_TOPIC_OR_PARTITION
        )
        assert await client.fetch_offset("billing", "orders", 0) == 2


@pytest.mark.parametrize(
    "error", [OSError("disk full"), LogFailedError("broken"), BrokerError("broker is closed")]
)
async def test_storage_failures_become_storage_error(
    server: Server, broker: Broker, monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    def failing_produce(*args: object) -> int:
        raise error

    async with Client(port=server.port) as client:
        await client.create_topic("orders", 1)
        monkeypatch.setattr(broker, "produce", failing_produce)
        await expect_error(
            client.produce("orders", 0, [ProduceRecord(b"a")]), ErrorCode.STORAGE_ERROR
        )


async def test_closed_broker_answers_storage_error(server: Server, broker: Broker) -> None:
    async with Client(port=server.port) as client:
        broker.close()
        await expect_error(client.metadata(), ErrorCode.STORAGE_ERROR)


async def test_bug_in_broker_propagates(broker: Broker, monkeypatch: pytest.MonkeyPatch) -> None:
    def buggy_topics() -> dict[str, int]:
        raise RuntimeError("bug")

    monkeypatch.setattr(broker, "topics", buggy_topics)
    with pytest.raises(RuntimeError, match="bug"):
        await RequestHandler(broker)(Frame(RequestType.METADATA, 1))


async def test_broker_runs_in_a_worker_thread(
    broker: Broker, monkeypatch: pytest.MonkeyPatch
) -> None:
    threads: list[threading.Thread] = []
    real_topics = broker.topics

    def recording_topics() -> dict[str, int]:
        threads.append(threading.current_thread())
        return real_topics()

    monkeypatch.setattr(broker, "topics", recording_topics)
    response = await RequestHandler(broker)(Frame(RequestType.METADATA, 7))
    assert response.correlation_id == 7
    assert threads and threads[0] is not threading.main_thread()


async def test_slow_disk_does_not_block_other_clients(
    server: Server, broker: Broker, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_produce = broker.produce

    def slow_produce(topic: str, partition: int, records: list[ProduceRecord]) -> int:
        time.sleep(0.5)  # imitates a slow fsync
        return real_produce(topic, partition, records)

    async with Client(port=server.port) as writer, Client(port=server.port) as reader:
        await writer.create_topic("orders", 1)
        monkeypatch.setattr(broker, "produce", slow_produce)
        produce = asyncio.create_task(writer.produce("orders", 0, [ProduceRecord(b"a")]))
        await asyncio.sleep(0.05)
        started = time.monotonic()
        assert await reader.metadata() == {"orders": 1}
        assert time.monotonic() - started < 0.3
        assert not produce.done()
        assert await produce == 0


def test_every_request_type_has_an_error_body() -> None:
    assert set(_ERROR_BODIES) == set(RequestType)


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (OffsetOutOfRangeError("x"), ErrorCode.OFFSET_OUT_OF_RANGE),  # before its base LogError
        (LogError("x"), ErrorCode.STORAGE_ERROR),
        (RuntimeError("x"), None),
    ],
)
def test_error_code_for(error: Exception, code: ErrorCode | None) -> None:
    assert error_code_for(error) is code
