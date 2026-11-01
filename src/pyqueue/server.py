"""Asyncio TCP server that speaks the PyQueue frame protocol."""

import asyncio
import contextlib
import logging

from pyqueue.protocol import Frame, ProtocolError
from pyqueue.transport import read_frame, write_frame

logger = logging.getLogger(__name__)


class Server:
    def __init__(self, host: str = "127.0.0.1", port: int = 9092) -> None:
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
        await self.start()
        assert self._server is not None
        await self._server.serve_forever()

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        logger.info("client connected: %s", peer)
        try:
            while (frame := await read_frame(reader)) is not None:
                response = await self._handle_frame(frame)
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

    async def _handle_frame(self, frame: Frame) -> Frame:
        # Echo for now; real request handling arrives with the storage layer.
        return frame
