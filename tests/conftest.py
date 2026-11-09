from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest

from pyqueue.broker import Broker
from pyqueue.handler import RequestHandler
from pyqueue.protocol import Frame
from pyqueue.server import Server


async def echo(frame: Frame) -> Frame:
    return frame


@pytest.fixture
async def echo_server() -> AsyncIterator[Server]:
    """Server that returns every frame unchanged, for transport-level tests."""
    srv = Server(echo, port=0)
    await srv.start()
    yield srv
    await srv.stop()


@pytest.fixture
def broker(tmp_path: Path) -> Iterator[Broker]:
    with Broker(tmp_path / "data") as b:
        yield b


@pytest.fixture
async def server(broker: Broker) -> AsyncIterator[Server]:
    """Server backed by a real broker in a temporary directory."""
    srv = Server(RequestHandler(broker), port=0)
    await srv.start()
    yield srv
    await srv.stop()
