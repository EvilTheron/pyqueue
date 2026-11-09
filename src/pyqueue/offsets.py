"""Committed offsets of consumer groups, kept in an internal log.

Every commit is appended to a `Log` in `data_dir/~offsets`: the record key is
(group, topic, partition) and the value is the committed offset. At startup the log is
read from the start and the last value for each key wins. Commits therefore get the same
guarantees as messages: a commit is acknowledged only after fsync, and a half-written
commit at the end of the log is cut off on restart.

The log only grows, so it is compacted. When it reaches `compact_bytes` and is at least
twice the size of the live data (one record per key), it is replaced by a new log that
holds just the latest value of every key:

    1. write `~offsets~compacting` with one record per key, sync and close it
    2. close the current log, rename `~offsets` to `~offsets~old`
    3. rename `~offsets~compacting` to `~offsets`, sync the data directory
    4. delete `~offsets~old`, open the new log

Startup finishes or undoes a compaction that a crash interrupted:

* `~offsets~old` without `~offsets`: the crash came between steps 2 and 3. The new log is
  complete (step 1 ends before step 2 starts), so steps 3 and 4 are redone.
* `~offsets~old` next to `~offsets`: the crash came during step 4; the old log is deleted.
* `~offsets~compacting` left over otherwise: the crash came during step 1; it is deleted
  and the current log, which was never touched, is used.

A commit is durable once it is in the current log, so a failed compaction never loses it.
If compaction fails in step 1, the current log simply stays. If it fails in steps 2-3,
commits are refused until restart, where the recovery above takes over.

Kafka compacts `__consumer_offsets` in place and keeps record offsets; here the log is
rewritten, so offsets inside the internal log start again from 0. Nothing reads them.

All directory names start with `~`, which topic names may not, so the broker never takes
them for topics. The store is thread-safe: one lock covers every operation.
"""

import contextlib
import logging
import shutil
import struct
import threading
from collections.abc import Iterator
from pathlib import Path

from pyqueue.log import Log, LogError, LogFailedError, fsync_directory
from pyqueue.record import Record, encoded_size

logger = logging.getLogger(__name__)

_U16 = struct.Struct(">H")
_I32 = struct.Struct(">i")
_VALUE = struct.Struct(">q")
DEFAULT_COMPACT_BYTES = 16 * 1024 * 1024
SNAPSHOT_BATCH = 1000
COMPACTING_SUFFIX = "~compacting"
OLD_SUFFIX = "~old"

OffsetKey = tuple[str, str, int]


class OffsetStoreCorruptError(Exception):
    """The offsets directory or log contains something the store did not write."""


def encode_key(group: str, topic: str, partition: int) -> bytes:
    group_bytes, topic_bytes = group.encode(), topic.encode()
    return (
        _U16.pack(len(group_bytes))
        + group_bytes
        + _U16.pack(len(topic_bytes))
        + topic_bytes
        + _I32.pack(partition)
    )


def decode_key(data: bytes) -> OffsetKey:
    try:
        position = 0
        (group_length,) = _U16.unpack_from(data, position)
        position += _U16.size
        group = data[position : position + group_length].decode()
        position += group_length
        (topic_length,) = _U16.unpack_from(data, position)
        position += _U16.size
        topic = data[position : position + topic_length].decode()
        position += topic_length
        (partition,) = _I32.unpack_from(data, position)
        position += _I32.size
    except (struct.error, UnicodeDecodeError) as exc:
        raise OffsetStoreCorruptError(f"invalid offset key {data!r}") from exc
    if position != len(data) or len(group.encode()) != group_length:
        raise OffsetStoreCorruptError(f"invalid offset key {data!r}")
    return group, topic, partition


def _record_size(key: OffsetKey) -> int:
    return encoded_size(Record(0, 0, encode_key(*key), bytes(_VALUE.size)))


class OffsetStore:
    def __init__(self, directory: Path, compact_bytes: int = DEFAULT_COMPACT_BYTES) -> None:
        if compact_bytes <= 0:
            raise ValueError(f"compact_bytes must be positive, got {compact_bytes}")
        self.directory = directory
        self.compact_bytes = compact_bytes
        self._compacting = directory.with_name(directory.name + COMPACTING_SUFFIX)
        self._old = directory.with_name(directory.name + OLD_SUFFIX)
        self._offsets: dict[OffsetKey, int] = {}
        self._live_bytes = 0
        self._lock = threading.Lock()
        self._failure: Exception | None = None
        self._closed = False
        self._recover_directories()
        self._log = Log(directory)
        try:
            self._load()
        except BaseException:
            with contextlib.suppress(Exception):
                self._log.close()
            raise

    @property
    def size_bytes(self) -> int:
        """Size of the internal log on disk."""
        with self._lock:
            return self._log.size_bytes

    def get(self, group: str, topic: str, partition: int) -> int | None:
        """Return the committed offset, or None if the group never committed one."""
        with self._lock:
            self._check_open()
            return self._offsets.get((group, topic, partition))

    def commit(self, group: str, topic: str, partition: int, offset: int) -> None:
        """Store an offset; it is on disk when this returns."""
        if offset < 0:
            raise ValueError(f"offset must not be negative, got {offset}")
        key = (group, topic, partition)
        with self._lock:
            self._check_open()
            if self._failure is not None:
                raise LogFailedError("an earlier compaction failed; restart the broker") from (
                    self._failure
                )
            self._log.append(_VALUE.pack(offset), encode_key(*key))
            if key not in self._offsets:
                self._live_bytes += _record_size(key)
            self._offsets[key] = offset
            if self._log.size_bytes >= max(self.compact_bytes, 2 * self._live_bytes):
                self._compact()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._log.close()

    def _recover_directories(self) -> None:
        if self._old.exists():
            if not self.directory.exists():
                if not self._compacting.exists():
                    raise OffsetStoreCorruptError(
                        f"{self._old} exists without {self.directory} or {self._compacting}"
                    )
                logger.warning("finishing an interrupted compaction of %s", self.directory)
                self._compacting.rename(self.directory)
                fsync_directory(self.directory.parent)
            shutil.rmtree(self._old)
        if self._compacting.exists():
            logger.warning("removing an unfinished compaction of %s", self.directory)
            shutil.rmtree(self._compacting)

    def _load(self) -> None:
        offset = self._log.start_offset
        while offset < self._log.next_offset:
            for record in self._log.read(offset):
                if record.key is None or len(record.value) != _VALUE.size:
                    raise OffsetStoreCorruptError(f"invalid offset record at {record.offset}")
                (value,) = _VALUE.unpack(record.value)
                self._offsets[decode_key(record.key)] = value
                offset = record.offset + 1
        self._live_bytes = sum(_record_size(key) for key in self._offsets)

    def _snapshot_batches(self) -> Iterator[list[tuple[bytes, bytes | None]]]:
        items = list(self._offsets.items())
        for start in range(0, len(items), SNAPSHOT_BATCH):
            yield [
                (_VALUE.pack(value), encode_key(*key))
                for key, value in items[start : start + SNAPSHOT_BATCH]
            ]

    def _compact(self) -> None:
        before = self._log.size_bytes
        # Step 1. Failing here is harmless: the current log is untouched.
        try:
            if self._old.exists():
                shutil.rmtree(self._old)  # left over if an earlier step 4 failed
            if self._compacting.exists():
                shutil.rmtree(self._compacting)
            with Log(self._compacting) as snapshot:
                for batch in self._snapshot_batches():
                    snapshot.append_batch(batch)
        except OSError, LogError:
            logger.exception("compaction of %s failed; keeping the current log", self.directory)
            with contextlib.suppress(OSError):
                shutil.rmtree(self._compacting)
            return

        # Steps 2 and 3. If they fail, the next startup finishes or undoes the swap.
        try:
            self._log.close()
            self.directory.rename(self._old)
            self._compacting.rename(self.directory)
            fsync_directory(self.directory.parent)
            self._log = Log(self.directory)
        except (OSError, LogError) as exc:
            logger.exception("compaction of %s failed; refusing commits", self.directory)
            self._failure = exc
            return

        # Step 4. A leftover old log is deleted at the next compaction or startup.
        try:
            shutil.rmtree(self._old)
        except OSError:
            logger.warning("could not delete %s", self._old, exc_info=True)
        logger.info("compacted %s: %d -> %d bytes", self.directory, before, self._log.size_bytes)

    def _check_open(self) -> None:
        if self._closed:
            raise LogError("offset store is closed")
