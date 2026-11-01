from collections.abc import AsyncIterator

import pytest

from pyqueue.server import Server


@pytest.fixture
async def server() -> AsyncIterator[Server]:
    srv = Server(port=0)
    await srv.start()
    yield srv
    await srv.stop()
