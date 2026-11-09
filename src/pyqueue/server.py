"""Asyncio TCP server that speaks the PyQueue frame protocol.

The server only moves frames: it reads a request frame, passes it to a handler and writes
back the frame the handler returns. What a request means is up to the handler
(`pyqueue.handler.RequestHandler` in production, an echo function in transport tests).
"""

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable

from pyqueue.protocol import Frame, ProtocolError
from pyqueue.transport import read_frame, write_frame

logger = logging.getLogger(__name__)

Handler = Callable[[Frame], Awaitable[Frame]]


class Server:
    def __init__(self, handler: Handler, host: str = "127.0.0.1", port: int = 9092) -> None:
        self._handler = handler
        self._host = host
        self._port = port
        self._server: asyncio.Server | None = None

    @property
    def port(self) -> int:
        """Actual listening port (useful when started with port 0)."""
        if self._server is None:
            raise RuntimeError("server is not started")
        port: int = self._server.sockets[0].getsockname()[1]
        return port

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle_connection, self._host, self._port)
        logger.info("listening on %s:%d", self._host, self.port)

    async def serve_forever(self) -> None:
        """Serve until cancelled, then stop and close client connections.

        `asyncio.Server.serve_forever()` is not used: when cancelled, it waits until every
        client disconnects by itself, so one idle client would keep the server from
        stopping.
        """
        await self.start()
        try:
            await asyncio.get_running_loop().create_future()  # never completes
        finally:
            await self.stop()

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            # Without this, wait_closed() waits until every client disconnects by itself.
            self._server.close_clients()
            await self._server.wait_closed()
            self._server = None

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        logger.info("client connected: %s", peer)
        try:
            while (frame := await read_frame(reader)) is not None:
                response = await self._handler(frame)
                await write_frame(writer, response)
        except ProtocolError as exc:
            logger.warning("protocol error from %s: %s", peer, exc)
        except ConnectionError as exc:
            logger.info("connection lost from %s: %s", peer, exc)
        except Exception:
            logger.exception("unexpected error while serving %s", peer)
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            logger.info("client disconnected: %s", peer)
