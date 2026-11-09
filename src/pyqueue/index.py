"""Sparse offset index of a log segment.

The index file sits next to its segment (`00000000000000000000.index`) and holds
fixed-size entries, each mapping a record offset to the record's position in the
segment file (all integers big-endian):

    +---------------------------+-----------------+
    | relative offset: uint32   | position: uint32 |
    +---------------------------+-----------------+

The offset is stored relative to the segment's base offset, so it fits in 4 bytes.
Only some records are indexed: a lookup returns the closest indexed position at or
before the wanted offset, and the segment scans forward from there.

The index can always be rebuilt from its segment, so it is not forced to disk on
every write; it is synced only when closed.
"""

import bisect
import os
import struct
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Self

ENTRY = struct.Struct(">II")
SUFFIX = ".index"
MAX_UINT32 = 2**32 - 1


def index_path(directory: Path, base_offset: int) -> Path:
    return directory / f"{base_offset:020d}{SUFFIX}"


class Index:
    def __init__(self, directory: Path, base_offset: int) -> None:
        self.base_offset = base_offset
        self.path = index_path(directory, base_offset)
        self.path.touch(exist_ok=True)
        self._file: BinaryIO = self.path.open("r+b")
        self._offsets: list[int] = []
        self._positions: list[int] = []
        try:
            self._load()
        except BaseException:
            self._file.close()
            raise

    @property
    def closed(self) -> bool:
        return self._file.closed

    def __len__(self) -> int:
        return len(self._offsets)

    def entries(self) -> list[tuple[int, int]]:
        """Return (absolute offset, position) pairs."""
        return list(zip(self._offsets, self._positions, strict=True))

    def append(self, offset: int, position: int) -> None:
        relative = offset - self.base_offset
        if not 0 <= relative <= MAX_UINT32 or not 0 <= position <= MAX_UINT32:
            raise ValueError(f"entry ({offset}, {position}) does not fit the index format")
        if self._offsets and (offset <= self._offsets[-1] or position <= self._positions[-1]):
            raise ValueError(f"entry ({offset}, {position}) is not after the last entry")
        self._file.seek(len(self) * ENTRY.size)
        self._file.write(ENTRY.pack(relative, position))
        self._file.flush()
        self._offsets.append(offset)
        self._positions.append(position)

    def lookup(self, offset: int) -> int:
        """Return the position of the last indexed record at or before `offset`, or 0."""
        i = bisect.bisect_right(self._offsets, offset)
        return self._positions[i - 1] if i else 0

    def clear(self) -> None:
        self._file.truncate(0)
        self._file.flush()
        self._offsets.clear()
        self._positions.clear()

    def close(self) -> None:
        if not self._file.closed:
            try:
                self._file.flush()
                os.fsync(self._file.fileno())
            finally:
                self._file.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def _load(self) -> None:
        data = self._file.read()
        whole = len(data) - len(data) % ENTRY.size
        for relative, position in ENTRY.iter_unpack(data[:whole]):
            offset = self.base_offset + relative
            if self._offsets and (offset <= self._offsets[-1] or position <= self._positions[-1]):
                break
            self._offsets.append(offset)
            self._positions.append(position)
        valid = len(self) * ENTRY.size
        if valid != len(data):
            self._file.truncate(valid)
            self._file.flush()
