"""A log segment: one append-only file of records.

A segment is named after the offset of its first record, zero-padded to 20 digits
(`00000000000000000000.log`), so a directory listing sorts segments in offset order.

When a segment is opened, every record is checked. The first record that is incomplete,
fails its checksum or has an unexpected offset marks the end of valid data, and the file
is truncated there. This removes a half-written tail left by a crash.

The segment does not decide when data reaches the disk: `sync()` is called by the owner
according to its fsync policy.
"""

import logging
import os
import time
from collections.abc import Iterator
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Self

from pyqueue.record import (
    LENGTH,
    MAX_RECORD_SIZE,
    MIN_LENGTH,
    CorruptRecordError,
    IncompleteRecordError,
    Record,
    RecordError,
    decode_record,
    encode_record,
)

logger = logging.getLogger(__name__)

SUFFIX = ".log"


def segment_path(directory: Path, base_offset: int) -> Path:
    return directory / f"{base_offset:020d}{SUFFIX}"


class Segment:
    def __init__(self, directory: Path, base_offset: int) -> None:
        if base_offset < 0:
            raise ValueError(f"base offset must not be negative, got {base_offset}")
        self.base_offset = base_offset
        self.path = segment_path(directory, base_offset)
        self.path.touch(exist_ok=True)
        self._file: BinaryIO = self.path.open("r+b")
        self.next_offset = base_offset
        self.size = 0
        self.truncated_bytes = 0
        try:
            self._recover()
        except BaseException:
            self._file.close()
            raise

    def append(
        self, value: bytes, key: bytes | None = None, timestamp_ms: int | None = None
    ) -> int:
        """Write a record at the end of the segment and return its offset.

        The data is handed to the operating system but not forced to disk; call `sync()` for that.
        """
        if timestamp_ms is None:
            timestamp_ms = time.time_ns() // 1_000_000
        offset = self.next_offset
        data = encode_record(Record(offset, timestamp_ms, key, value))
        self._file.seek(self.size)
        self._file.write(data)
        self._file.flush()
        self.size += len(data)
        self.next_offset += 1
        return offset

    def read_at(self, position: int) -> tuple[Record, int]:
        """Read the record that starts at `position`. Return it and its size in bytes."""
        if not 0 <= position < self.size:
            raise ValueError(f"position {position} is outside the segment (size {self.size})")
        return self._read_raw(position)

    def records(self, position: int = 0) -> Iterator[tuple[int, Record]]:
        """Yield (position, record) pairs from `position` to the end of the segment."""
        while position < self.size:
            record, size = self._read_raw(position)
            yield position, record
            position += size

    def sync(self) -> None:
        """Force written data to disk."""
        self._file.flush()
        os.fsync(self._file.fileno())

    def close(self) -> None:
        if not self._file.closed:
            self.sync()
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

    def _read_raw(self, position: int) -> tuple[Record, int]:
        self._file.seek(position)
        prefix = self._file.read(LENGTH.size)
        if len(prefix) < LENGTH.size:
            raise IncompleteRecordError(f"no complete length prefix at position {position}")
        length: int = LENGTH.unpack(prefix)[0]
        # Check the length before reading, so a corrupt prefix cannot trigger a huge read.
        if length < MIN_LENGTH or length > MAX_RECORD_SIZE:
            raise CorruptRecordError(f"invalid record length {length} at position {position}")
        return decode_record(prefix + self._file.read(length))

    def _recover(self) -> None:
        file_size = os.fstat(self._file.fileno()).st_size
        position = 0
        while position < file_size:
            try:
                record, size = self._read_raw(position)
            except RecordError as exc:
                self._truncate(position, file_size, str(exc))
                return
            if record.offset != self.next_offset:
                reason = f"expected offset {self.next_offset}, found {record.offset}"
                self._truncate(position, file_size, reason)
                return
            position += size
            self.next_offset += 1
        self.size = position

    def _truncate(self, position: int, file_size: int, reason: str) -> None:
        self._file.truncate(position)
        self.sync()
        self.size = position
        self.truncated_bytes = file_size - position
        logger.warning(
            "%s: truncated %d bytes at position %d (%s)",
            self.path.name,
            self.truncated_bytes,
            position,
            reason,
        )
