"""Reading and writing frames on asyncio streams."""

import asyncio

from pyqueue.protocol import (
    LENGTH,
    Frame,
    ProtocolError,
    decode_length,
    decode_payload,
    encode_frame,
)


async def read_frame(reader: asyncio.StreamReader) -> Frame | None:
    """Read one frame. Return None if the peer closed the connection between frames."""
    try:
        prefix = await reader.readexactly(LENGTH.size)
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            return None
        raise ProtocolError("connection closed inside the length prefix") from exc

    length = decode_length(prefix)
    try:
        payload = await reader.readexactly(length)
    except asyncio.IncompleteReadError as exc:
        raise ProtocolError(
            f"connection closed after {len(exc.partial)} of {length} payload bytes"
        ) from exc
    return decode_payload(payload)


async def write_frame(writer: asyncio.StreamWriter, frame: Frame) -> None:
    """Write one frame and wait until the transport buffer drains."""
    writer.write(encode_frame(frame))
    await writer.drain()
