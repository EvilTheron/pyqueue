"""Topics and partitions stored in one data directory.

Layout:

    data_dir/
    ├── .lock          only one broker may use the directory
    ├── ~offsets/      committed offsets of consumer groups (see `pyqueue.offsets`)
    └── orders/        topic
        ├── 0/         partition: a `Log` directory with segments and its own lock
        └── 1/

A topic is created atomically: its partition directories are made inside a temporary
directory `<name>~creating`, which is then renamed to `<name>` in one step. After a crash
a topic therefore exists with all its partitions or not at all. Leftovers of an
interrupted creation are deleted at startup; `~` is not allowed in topic names, so a
leftover can never be mistaken for a topic. Directories starting with `~` belong to
the offset store and are skipped. Any other unexpected directory, or a topic
whose partitions are not exactly `0..N-1`, stops the broker from starting: as with a
damaged log, guessing could lose data.

Topic names become directory names, so they are restricted to lowercase ASCII letters,
digits, `.`, `_` and `-`, up to 249 characters, and

* may not start with `.` (this also excludes `.` and `..`, so no path traversal),
* may not end with `.` (Windows silently drops a trailing dot: `orders.` would be
  `orders`),
* are lowercase only (Windows file names are case-insensitive: `Orders` and `orders`
  would be one directory),
* may not be a reserved Windows device name such as `con` or `nul`, with or without an
  extension.

The server calls the broker from worker threads (`asyncio.to_thread`), while `Log` is
not thread-safe. Each partition therefore has its own lock, so a slow fsync in one
partition does not hold up another, and the topic table has a separate lock. Lock order:
the topic table first, then a partition, never the other way round.
"""

import contextlib
import logging
import re
import shutil
import threading
from collections.abc import Sequence
from pathlib import Path
from types import TracebackType
from typing import Self

from pyqueue.lock import DirectoryLock
from pyqueue.log import Log, LogConfig, OffsetOutOfRangeError, fsync_directory
from pyqueue.messages import MAX_FETCH_RECORDS_SIZE, MAX_RECORD_DATA_SIZE, ProduceRecord
from pyqueue.offsets import DEFAULT_COMPACT_BYTES, OffsetStore
from pyqueue.record import Record

logger = logging.getLogger(__name__)

TOPIC_NAME = re.compile(r"[a-z0-9_-][a-z0-9._-]{0,248}")
# COM0 and LPT0 are included to be safe; rejecting a few extra names costs nothing.
WINDOWS_RESERVED_NAMES = frozenset(
    {"con", "prn", "aux", "nul"} | {f"com{i}" for i in range(10)} | {f"lpt{i}" for i in range(10)}
)
CREATING_SUFFIX = "~creating"
INTERNAL_PREFIX = "~"
OFFSETS_DIR = "~offsets"
MAX_GROUP_LENGTH = 249
# Every partition keeps files open (lock, segment, index), so the count is capped.
MAX_PARTITIONS = 1000


class BrokerError(Exception):
    """Base class for broker errors."""


class InvalidRequestError(BrokerError):
    """The request is well-formed but its values are not acceptable."""


class UnknownTopicOrPartitionError(BrokerError):
    pass


class TopicExistsError(BrokerError):
    pass


class RecordTooLargeError(BrokerError):
    pass


class BrokerCorruptError(BrokerError):
    """The data directory contains something the broker did not create."""


def validate_topic_name(name: str) -> None:
    """Raise `InvalidRequestError` if `name` cannot be used as a topic name."""
    if TOPIC_NAME.fullmatch(name) is None:
        raise InvalidRequestError(
            f"invalid topic name {name!r}: use 1-249 characters a-z, 0-9, '.', '_', '-' "
            "and do not start with '.'"
        )
    if name.endswith("."):
        raise InvalidRequestError(f"invalid topic name {name!r}: must not end with '.'")
    if name.split(".", 1)[0] in WINDOWS_RESERVED_NAMES:
        raise InvalidRequestError(f"invalid topic name {name!r}: reserved on Windows")


def validate_group(group: str) -> None:
    """Raise `InvalidRequestError` if `group` cannot be used as a consumer group name."""
    if not 0 < len(group) <= MAX_GROUP_LENGTH:
        raise InvalidRequestError(f"group name must have 1-{MAX_GROUP_LENGTH} characters")


class _Partition:
    def __init__(self, log: Log) -> None:
        self.log = log
        self.lock = threading.Lock()


class Broker:
    def __init__(
        self,
        data_dir: Path,
        log_config: LogConfig | None = None,
        offsets_compact_bytes: int = DEFAULT_COMPACT_BYTES,
    ) -> None:
        self.data_dir = data_dir
        self.log_config = log_config or LogConfig()
        self._topics: dict[str, list[_Partition]] = {}
        self._offsets: OffsetStore | None = None
        self._topics_lock = threading.Lock()
        self._closed = False
        data_dir.mkdir(parents=True, exist_ok=True)
        self._lock = DirectoryLock(data_dir)
        self._lock.acquire()
        try:
            self._load()
            self._offsets = OffsetStore(data_dir / OFFSETS_DIR, offsets_compact_bytes)
        except BaseException:
            # Keep the original error; a second one from cleanup would only hide it.
            with contextlib.suppress(Exception):
                self._close_logs()
            with contextlib.suppress(Exception):
                self._lock.release()
            raise

    @property
    def closed(self) -> bool:
        return self._closed

    def topics(self) -> dict[str, int]:
        """Return every topic with its number of partitions, sorted by name."""
        with self._topics_lock:
            self._check_open()
            return {name: len(partitions) for name, partitions in sorted(self._topics.items())}

    def create_topic(self, name: str, partitions: int) -> None:
        validate_topic_name(name)
        if not 1 <= partitions <= MAX_PARTITIONS:
            raise InvalidRequestError(
                f"partitions must be in [1, {MAX_PARTITIONS}], got {partitions}"
            )
        with self._topics_lock:
            self._check_open()
            if name in self._topics:
                raise TopicExistsError(f"topic {name!r} already exists")
            temporary = self.data_dir / f"{name}{CREATING_SUFFIX}"
            if temporary.exists():
                shutil.rmtree(temporary)
            temporary.mkdir()
            for number in range(partitions):
                (temporary / str(number)).mkdir()
            fsync_directory(temporary)
            final = temporary.rename(self.data_dir / name)
            fsync_directory(self.data_dir)
            self._topics[name] = self._open_partitions(final)
        logger.info("created topic %s with %d partitions", name, partitions)

    def produce(self, topic: str, partition: int, records: Sequence[ProduceRecord]) -> int:
        """Append records to a partition and return the offset of the first one.

        With the default log policy the records are on disk when this returns.
        """
        if not records:
            raise InvalidRequestError("produce request has no records")
        for record in records:
            size = len(record.key or b"") + len(record.value)
            if size > MAX_RECORD_DATA_SIZE:
                raise RecordTooLargeError(
                    f"record key and value of {size} bytes exceed {MAX_RECORD_DATA_SIZE}"
                )
        target = self._partition(topic, partition)
        with target.lock:
            return target.log.append_batch([(record.value, record.key) for record in records])

    def fetch(
        self, topic: str, partition: int, offset: int, max_bytes: int
    ) -> tuple[list[Record], int]:
        """Read records from `offset`. Return them and the partition's next offset.

        `max_bytes` is capped so that the records always fit in one response frame.
        Raises `OffsetOutOfRangeError` if `offset` is outside the log.
        """
        if max_bytes <= 0:
            raise InvalidRequestError(f"max_bytes must be positive, got {max_bytes}")
        target = self._partition(topic, partition)
        with target.lock:
            # A record takes fewer bytes in a Fetch response than in the log, so a limit
            # on log bytes is also a limit on response bytes.
            records = target.log.read(offset, min(max_bytes, MAX_FETCH_RECORDS_SIZE))
            return records, target.log.next_offset

    def commit_offset(self, group: str, topic: str, partition: int, offset: int) -> None:
        """Store the offset a consumer group will read next; on disk when this returns.

        The offset may be anything from 0 to the partition's next offset (a consumer
        that has read everything commits the next offset).
        """
        validate_group(group)
        target = self._partition(topic, partition)
        with target.lock:
            end = target.log.next_offset
        if not 0 <= offset <= end:
            raise OffsetOutOfRangeError(f"offset {offset} is outside [0, {end}]")
        self._offset_store().commit(group, topic, partition, offset)

    def fetch_offset(self, group: str, topic: str, partition: int) -> int | None:
        """Return the committed offset, or None if the group has not committed one."""
        validate_group(group)
        self._partition(topic, partition)
        return self._offset_store().get(group, topic, partition)

    def close(self) -> None:
        with self._topics_lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._close_logs()
            finally:
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
        for entry in sorted(self.data_dir.iterdir()):
            if not entry.is_dir() or entry.name.startswith(INTERNAL_PREFIX):
                continue
            if entry.name.endswith(CREATING_SUFFIX):
                logger.warning("removing unfinished topic creation %s", entry.name)
                shutil.rmtree(entry)
                continue
            try:
                validate_topic_name(entry.name)
            except InvalidRequestError as exc:
                raise BrokerCorruptError(f"unexpected directory {entry}") from exc
            self._topics[entry.name] = self._open_partitions(entry)

    def _open_partitions(self, topic_dir: Path) -> list[_Partition]:
        entries = list(topic_dir.iterdir())
        expected = {str(number) for number in range(len(entries))}
        if not entries or {entry.name for entry in entries} != expected:
            raise BrokerCorruptError(f"{topic_dir} must contain partition directories 0..N-1")
        if not all(entry.is_dir() for entry in entries):
            raise BrokerCorruptError(f"{topic_dir} contains a file instead of a partition")

        partitions: list[_Partition] = []
        try:
            for number in range(len(entries)):
                partitions.append(_Partition(Log(topic_dir / str(number), self.log_config)))
        except BaseException:
            for opened in partitions:
                with contextlib.suppress(Exception):
                    opened.log.close()
            raise
        return partitions

    def _partition(self, topic: str, partition: int) -> _Partition:
        with self._topics_lock:
            self._check_open()
            partitions = self._topics.get(topic)
        if partitions is None or not 0 <= partition < len(partitions):
            raise UnknownTopicOrPartitionError(f"unknown topic or partition {topic}/{partition}")
        return partitions[partition]

    def _offset_store(self) -> OffsetStore:
        with self._topics_lock:
            self._check_open()
        assert self._offsets is not None  # set in __init__ before the broker is usable
        return self._offsets

    def _close_logs(self) -> None:
        """Close every log and the offset store, even if some fail; raise the first error."""
        first_error: Exception | None = None
        if self._offsets is not None:
            try:
                self._offsets.close()
            except Exception as exc:
                logger.exception("failed to close the offset store")
                first_error = exc
        for partitions in self._topics.values():
            for partition in partitions:
                with partition.lock:
                    try:
                        partition.log.close()
                    except Exception as exc:
                        logger.exception("failed to close %s", partition.log.directory)
                        first_error = first_error or exc
        if first_error is not None:
            raise first_error

    def _check_open(self) -> None:
        if self._closed:
            raise BrokerError("broker is closed")
