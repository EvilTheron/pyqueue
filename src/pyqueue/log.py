"""A partition log: an ordered sequence of segments in one directory.

Records are appended to the last (active) segment. When it reaches `segment_bytes`,
it is synced and a new segment starts at the next offset. A segment may exceed the
limit by at most one record, because the check runs before each append.

Durability is controlled by `fsync_interval_ms`:

* `0` (default): every append is synced before `append()` returns, so a record whose
  offset was returned survives a crash of the process or the machine.
* `N > 0`: appends are synced by `run_periodic_sync()` every N ms. Records written
  since the last sync can be lost if the machine crashes (not if only the process
  dies: the data is already in the operating system's cache). This trades durability
  for throughput.

Crash recovery relies on one invariant: a segment is fully synced before the next one
is created, whatever the policy. So after a crash only the active segment can end with
a half-written record, and only that segment is repaired (its tail truncated). Damage
in any older segment, or a gap between segments, means a disk or operator problem: the
log refuses to open rather than silently dropping data.

If writing or syncing fails with `OSError`, the log stops accepting writes until it is
reopened. After a failed fsync the operating system may already have dropped the dirty
pages, so retrying the fsync could falsely report success. Reopening re-reads the files
and repairs the tail. Reads stay available.

Retention removes whole segments from the start of the log, oldest first, so the log
never gets a gap. The active segment is never removed. A segment is removed when

* `retention_bytes` is set and the log without it is still at least that large
  (the log never shrinks below the limit), or
* `retention_ms` is set and its newest record is older than that.

`apply_retention()` does the work; the owner calls it periodically. A removed segment
is first dropped from memory, then closed (Windows cannot delete open files), then
deleted from disk. If deleting fails, the log stops accepting writes; after reopening,
the segment that was not deleted is simply the first one again.

Only one `Log` may use a directory at a time: an exclusive lock on the directory is
taken before any segment is read and released after all segments are closed. A second
`Log` on the same directory, in this or another process, fails with
`DirectoryLockedError`. The operating system drops the lock if the process dies.

A log is not thread-safe; it is meant to be used from a single event loop. File
operations are blocking.
"""

import asyncio
import bisect
import contextlib
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import Self

from pyqueue.index import MAX_UINT32
from pyqueue.lock import DirectoryLock
from pyqueue.record import MAX_RECORD_SIZE, Record, encoded_size
from pyqueue.segment import (
    DEFAULT_INDEX_INTERVAL_BYTES,
    Segment,
    SegmentCorruptError,
)

logger = logging.getLogger(__name__)

SEGMENT_NAME = re.compile(r"\d{20}\.log")
DEFAULT_SEGMENT_BYTES = 64 * 1024 * 1024
# Positions in the index are uint32, and every record starts before the size limit.
MAX_SEGMENT_BYTES = MAX_UINT32

# Windows cannot open a directory with os.open, so a directory fsync is not possible there.
DIRECTORY_FSYNC_SUPPORTED = os.name != "nt"


class LogError(Exception):
    """Base class for log errors."""


class LogCorruptError(LogError):
    """The segments on disk are damaged or inconsistent; the log cannot be opened."""


class LogFailedError(LogError):
    """An earlier write or sync failed; the log must be reopened before writing again."""


class OffsetOutOfRangeError(LogError):
    """The requested offset is not in the log."""


@dataclass(frozen=True, slots=True)
class LogConfig:
    segment_bytes: int = DEFAULT_SEGMENT_BYTES
    index_interval_bytes: int = DEFAULT_INDEX_INTERVAL_BYTES
    fsync_interval_ms: int = 0
    retention_bytes: int | None = None
    retention_ms: int | None = None

    def __post_init__(self) -> None:
        if not 0 < self.segment_bytes <= MAX_SEGMENT_BYTES:
            raise ValueError(
                f"segment_bytes must be in (0, {MAX_SEGMENT_BYTES}], got {self.segment_bytes}"
            )
        if self.index_interval_bytes <= 0:
            raise ValueError(
                f"index_interval_bytes must be positive, got {self.index_interval_bytes}"
            )
        if self.fsync_interval_ms < 0:
            raise ValueError(
                f"fsync_interval_ms must not be negative, got {self.fsync_interval_ms}"
            )
        if self.retention_bytes is not None and self.retention_bytes <= 0:
            raise ValueError(f"retention_bytes must be positive, got {self.retention_bytes}")
        if self.retention_ms is not None and self.retention_ms <= 0:
            raise ValueError(f"retention_ms must be positive, got {self.retention_ms}")


def fsync_directory(path: Path) -> None:
    """Make a newly created or deleted file name in `path` durable (no-op on Windows)."""
    if not DIRECTORY_FSYNC_SUPPORTED:
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Log:
    def __init__(self, directory: Path, config: LogConfig | None = None) -> None:
        self.directory = directory
        self.config = config or LogConfig()
        self._segments: list[Segment] = []
        self._base_offsets: list[int] = []
        self._dirty = False
        self._failure: OSError | None = None
        self._closed = False
        directory.mkdir(parents=True, exist_ok=True)
        self._lock = DirectoryLock(directory)
        self._lock.acquire()
        try:
            self._load()
        except BaseException:
            # Keep the original error; a second one from cleanup would only hide it.
            with contextlib.suppress(Exception):
                self._close_segments()
            with contextlib.suppress(Exception):
                self._lock.release()
            raise

    @property
    def start_offset(self) -> int:
        """Offset of the oldest record still in the log."""
        return self._segments[0].base_offset

    @property
    def next_offset(self) -> int:
        """Offset the next appended record will get (the log end offset)."""
        return self._segments[-1].next_offset

    @property
    def size_bytes(self) -> int:
        return sum(segment.size for segment in self._segments)

    @property
    def segment_count(self) -> int:
        return len(self._segments)

    @property
    def closed(self) -> bool:
        return self._closed

    def append(
        self, value: bytes, key: bytes | None = None, timestamp_ms: int | None = None
    ) -> int:
        """Append a record and return its offset.

        With the default policy the record is on disk when this returns. If an `OSError`
        is raised, the record may or may not be in the log after a restart, so the caller
        must treat it as not acknowledged.
        """
        self._check_writable()
        try:
            if self._segments[-1].size >= self.config.segment_bytes:
                self._roll()
            offset = self._segments[-1].append(value, key, timestamp_ms)
        except OSError as exc:
            self._failure = exc
            raise
        self._dirty = True
        if self.config.fsync_interval_ms == 0:
            self.sync()
        return offset

    def read(self, offset: int, max_bytes: int = MAX_RECORD_SIZE) -> list[Record]:
        """Read records starting at `offset`, up to about `max_bytes` of encoded data.

        At least one record is returned if `offset` is in the log, even if it is larger
        than `max_bytes`, so a reader can never get stuck. Reading at `next_offset`
        returns an empty list: the reader has caught up.
        """
        self._check_open()
        if max_bytes <= 0:
            raise ValueError(f"max_bytes must be positive, got {max_bytes}")
        if offset == self.next_offset:
            return []
        if not self.start_offset <= offset < self.next_offset:
            raise OffsetOutOfRangeError(
                f"offset {offset} is outside the log [{self.start_offset}, {self.next_offset})"
            )

        first = bisect.bisect_right(self._base_offsets, offset) - 1
        records: list[Record] = []
        total = 0
        for segment in self._segments[first:]:
            start = (
                segment.find(offset) if segment.base_offset <= offset < segment.next_offset else 0
            )
            for _, record in segment.records(start):
                size = encoded_size(record)
                if records and total + size > max_bytes:
                    return records
                records.append(record)
                total += size
        return records

    def sync(self) -> None:
        """Force appended records to disk. Does nothing if there is nothing new."""
        self._check_writable()
        if not self._dirty:
            return
        try:
            self._segments[-1].sync()
        except OSError as exc:
            self._failure = exc
            raise
        self._dirty = False

    def apply_retention(self, now_ms: int | None = None) -> int:
        """Delete expired segments from the start of the log. Return how many were deleted."""
        self._check_writable()
        if now_ms is None:
            now_ms = time.time_ns() // 1_000_000
        size = self.size_bytes
        expired = 0
        for segment in self._segments[:-1]:  # never the active segment
            if not self._is_expired(segment, size, now_ms):
                break  # only a prefix is deleted, so the log never gets a gap
            size -= segment.size
            expired += 1
        if expired == 0:
            return 0
        try:
            for _ in range(expired):
                self._delete_oldest()
            fsync_directory(self.directory)
        except OSError as exc:
            self._failure = exc
            raise
        logger.info(
            "%s: retention deleted %d segment(s), log now starts at offset %d",
            self.directory,
            expired,
            self.start_offset,
        )
        return expired

    def close(self) -> None:
        """Sync and close all segments. Safe to call more than once."""
        if self._closed:
            return
        self._closed = True
        try:
            self._close_segments()
        finally:
            # Released last: no other process may open the files while they are still open here.
            self._lock.release()

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
        base_offsets = sorted(
            int(path.name[:20])
            for path in self.directory.iterdir()
            if SEGMENT_NAME.fullmatch(path.name)
        )
        if not base_offsets:
            self._add_segment(self._create_segment(0))
            return
        for i, base_offset in enumerate(base_offsets):
            is_active = i == len(base_offsets) - 1
            try:
                segment = Segment(
                    self.directory,
                    base_offset,
                    self.config.index_interval_bytes,
                    repair=is_active,
                )
            except SegmentCorruptError as exc:
                raise LogCorruptError(str(exc)) from exc
            if self._segments and base_offset != self.next_offset:
                expected = self.next_offset
                with contextlib.suppress(Exception):
                    segment.close()
                raise LogCorruptError(
                    f"{segment.path.name} starts at offset {base_offset}, "
                    f"but the previous segment ends at {expected}"
                )
            self._add_segment(segment)

    def _create_segment(self, base_offset: int) -> Segment:
        segment = Segment(self.directory, base_offset, self.config.index_interval_bytes)
        try:
            fsync_directory(self.directory)
        except BaseException:
            with contextlib.suppress(Exception):
                segment.close()
            raise
        return segment

    def _add_segment(self, segment: Segment) -> None:
        self._segments.append(segment)
        self._base_offsets.append(segment.base_offset)

    def _roll(self) -> None:
        active = self._segments[-1]
        # The invariant recovery relies on: the old segment is on disk before the new one exists.
        active.sync()
        self._dirty = False
        self._add_segment(self._create_segment(active.next_offset))

    def _is_expired(self, segment: Segment, log_size: int, now_ms: int) -> bool:
        retention_bytes = self.config.retention_bytes
        if retention_bytes is not None and log_size - segment.size >= retention_bytes:
            return True
        retention_ms = self.config.retention_ms
        return (
            retention_ms is not None
            and segment.max_timestamp_ms is not None
            and now_ms - segment.max_timestamp_ms > retention_ms
        )

    def _delete_oldest(self) -> None:
        # Drop it from memory first: whatever fails below, memory never refers to a closed
        # or missing segment, and the files left on disk are still a valid log prefix.
        segment = self._segments.pop(0)
        self._base_offsets.pop(0)
        segment.close()
        segment.path.unlink()
        try:
            segment.index.path.unlink(missing_ok=True)
        except OSError as exc:
            # A leftover index is harmless: index files are never trusted or loaded alone.
            logger.warning("could not delete %s: %s", segment.index.path.name, exc)

    def _close_segments(self) -> None:
        error: BaseException | None = None
        for segment in self._segments:
            try:
                segment.close()
            except BaseException as exc:
                error = error or exc
        if error is not None:
            raise error

    def _check_open(self) -> None:
        if self._closed:
            raise LogError("log is closed")

    def _check_writable(self) -> None:
        self._check_open()
        if self._failure is not None:
            raise LogFailedError("an earlier write or sync failed; reopen the log") from (
                self._failure
            )


async def run_periodic_sync(log: Log) -> None:
    """Sync the log every `fsync_interval_ms` until cancelled.

    An error ends the task with that exception: the owner must treat it as fatal for the
    log. Cancel this task before closing the log. The fsync itself blocks the event loop.
    """
    if log.config.fsync_interval_ms == 0:
        raise ValueError("the log syncs on every append; periodic sync is not used")
    interval = log.config.fsync_interval_ms / 1000
    while True:
        await asyncio.sleep(interval)
        log.sync()
