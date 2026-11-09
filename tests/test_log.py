import asyncio
import os
from pathlib import Path

import pytest

import pyqueue.log as log_module
from pyqueue.index import index_path
from pyqueue.log import (
    MAX_SEGMENT_BYTES,
    Log,
    LogConfig,
    LogCorruptError,
    LogError,
    LogFailedError,
    OffsetOutOfRangeError,
    fsync_directory,
    run_periodic_sync,
)
from pyqueue.record import MAX_RECORD_SIZE, Record, RecordError, encoded_size
from pyqueue.segment import Segment, segment_path

# Every record below is 58 bytes: 28 bytes of header and a 30-byte value.
VALUE = b"x" * 30
RECORD_SIZE = 58
SMALL = LogConfig(segment_bytes=200, index_interval_bytes=100)  # 4 records per segment


def fill(log: Log, count: int) -> None:
    for i in range(count):
        log.append(VALUE, timestamp_ms=i)


def segment_files(directory: Path) -> list[str]:
    return sorted(p.name for p in directory.glob("*.log"))


class FsyncCounter:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.calls = 0
        self._real = os.fsync
        monkeypatch.setattr(os, "fsync", self)

    def __call__(self, fd: int) -> None:
        self.calls += 1
        self._real(fd)


def failing_fsync(fd: int) -> None:
    raise OSError("fsync failed")


# --- configuration ---------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"segment_bytes": 0},
        {"segment_bytes": MAX_SEGMENT_BYTES + 1},
        {"index_interval_bytes": 0},
        {"fsync_interval_ms": -1},
    ],
)
def test_invalid_config_is_rejected(kwargs: dict[str, int]) -> None:
    with pytest.raises(ValueError):
        LogConfig(**kwargs)


# --- basic operation -------------------------------------------------------------


def test_new_log_creates_first_segment(tmp_path: Path) -> None:
    directory = tmp_path / "partition-0"
    with Log(directory) as log:
        assert segment_files(directory) == ["00000000000000000000.log"]
        assert log.start_offset == 0
        assert log.next_offset == 0
        assert log.size_bytes == 0
        assert log.read(0) == []


def test_append_and_read(tmp_path: Path) -> None:
    with Log(tmp_path) as log:
        assert log.append(b"a", key=b"k", timestamp_ms=1) == 0
        assert log.append(b"b", timestamp_ms=2) == 1
        assert log.read(0) == [Record(0, 1, b"k", b"a"), Record(1, 2, None, b"b")]
        assert log.read(1) == [Record(1, 2, None, b"b")]
        expected = encoded_size(Record(0, 1, b"k", b"a")) + encoded_size(Record(1, 2, None, b"b"))
        assert log.size_bytes == expected


@pytest.mark.parametrize("offset", [-1, 3])
def test_read_outside_log_raises(tmp_path: Path, offset: int) -> None:
    with Log(tmp_path) as log:
        fill(log, 2)
        with pytest.raises(OffsetOutOfRangeError):
            log.read(offset)


def test_read_at_end_returns_nothing(tmp_path: Path) -> None:
    with Log(tmp_path) as log:
        fill(log, 2)
        assert log.read(2) == []


def test_read_respects_max_bytes(tmp_path: Path) -> None:
    with Log(tmp_path) as log:
        fill(log, 5)
        assert len(log.read(0, max_bytes=RECORD_SIZE * 2)) == 2
        assert len(log.read(0, max_bytes=RECORD_SIZE * 2 + 57)) == 2
        assert len(log.read(0, max_bytes=10_000)) == 5


def test_read_returns_one_record_even_if_larger_than_max_bytes(tmp_path: Path) -> None:
    with Log(tmp_path) as log:
        fill(log, 3)
        assert len(log.read(1, max_bytes=1)) == 1


def test_read_rejects_non_positive_max_bytes(tmp_path: Path) -> None:
    with Log(tmp_path) as log:
        fill(log, 1)
        with pytest.raises(ValueError):
            log.read(0, max_bytes=0)


def test_record_size_constant_matches_encoding() -> None:
    assert encoded_size(Record(0, 0, None, VALUE)) == RECORD_SIZE


# --- rotation --------------------------------------------------------------------


def test_rotation_by_size(tmp_path: Path) -> None:
    with Log(tmp_path, SMALL) as log:
        fill(log, 10)
        # 4 records reach 232 >= 200 bytes, so the 5th starts a new segment.
        assert segment_files(tmp_path) == [
            "00000000000000000000.log",
            "00000000000000000004.log",
            "00000000000000000008.log",
        ]
        assert log.segment_count == 3
        assert log.size_bytes == 10 * RECORD_SIZE


@pytest.mark.parametrize("offset", range(10))
def test_read_across_segments(tmp_path: Path, offset: int) -> None:
    with Log(tmp_path, SMALL) as log:
        fill(log, 10)
        assert [r.offset for r in log.read(offset)] == list(range(offset, 10))


def test_rotation_syncs_old_segment_even_with_interval_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    synced: list[int] = []
    real_sync = Segment.sync

    def recording_sync(self: Segment) -> None:
        synced.append(self.base_offset)
        real_sync(self)

    config = LogConfig(segment_bytes=200, fsync_interval_ms=1000)
    with Log(tmp_path, config) as log:
        monkeypatch.setattr(Segment, "sync", recording_sync)
        fill(log, 5)
        assert synced == [0]  # only the rotation synced; appends did not


def test_new_segment_syncs_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    synced_dirs: list[Path] = []
    monkeypatch.setattr(log_module, "fsync_directory", synced_dirs.append)
    with Log(tmp_path, SMALL) as log:
        fill(log, 5)
    assert synced_dirs == [tmp_path, tmp_path]  # first segment and one rotation


@pytest.mark.skipif(not log_module.DIRECTORY_FSYNC_SUPPORTED, reason="no directory fsync")
def test_fsync_directory_calls_fsync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    counter = FsyncCounter(monkeypatch)
    fsync_directory(tmp_path)
    assert counter.calls == 1


def test_fsync_directory_is_skipped_where_unsupported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    counter = FsyncCounter(monkeypatch)
    monkeypatch.setattr(log_module, "DIRECTORY_FSYNC_SUPPORTED", False)
    fsync_directory(tmp_path)
    assert counter.calls == 0


# --- reopening and recovery ------------------------------------------------------


def test_reopen_restores_state(tmp_path: Path) -> None:
    with Log(tmp_path, SMALL) as log:
        fill(log, 10)
    with Log(tmp_path, SMALL) as log:
        assert log.segment_count == 3
        assert (log.start_offset, log.next_offset) == (0, 10)
        assert log.append(VALUE) == 10
        assert segment_files(tmp_path)[-1] == "00000000000000000008.log"
        assert [r.offset for r in log.read(0, max_bytes=10_000)] == list(range(11))


def test_reopen_repairs_torn_tail_of_active_segment(tmp_path: Path) -> None:
    with Log(tmp_path, SMALL) as log:
        fill(log, 10)
    active = segment_path(tmp_path, 8)
    active.write_bytes(active.read_bytes()[:-5])  # crash in the middle of record 9
    with Log(tmp_path, SMALL) as log:
        assert log.next_offset == 9
        assert log.append(VALUE) == 9


def test_damage_in_old_segment_refuses_to_open(tmp_path: Path) -> None:
    with Log(tmp_path, SMALL) as log:
        fill(log, 10)
    old = segment_path(tmp_path, 4)
    damaged = bytearray(old.read_bytes())
    damaged[RECORD_SIZE + 20] ^= 0xFF  # inside record 5
    old.write_bytes(bytes(damaged))
    with pytest.raises(LogCorruptError):
        Log(tmp_path, SMALL)
    assert old.read_bytes() == bytes(damaged)  # nothing was truncated
    # Every segment opened before the failure was closed (Windows would refuse otherwise).
    for path in tmp_path.iterdir():
        path.unlink()


def test_gap_between_segments_refuses_to_open(tmp_path: Path) -> None:
    with Log(tmp_path, SMALL) as log:
        fill(log, 10)
    segment_path(tmp_path, 4).unlink()
    index_path(tmp_path, 4).unlink()
    with pytest.raises(LogCorruptError, match="starts at offset 8"):
        Log(tmp_path, SMALL)
    for path in tmp_path.iterdir():
        path.unlink()


def test_unrelated_files_are_ignored(tmp_path: Path) -> None:
    (tmp_path / "notes.txt").write_text("hello")
    (tmp_path / "123.log").write_text("not a segment name")
    with Log(tmp_path) as log:
        fill(log, 1)
        assert segment_files(tmp_path) == ["00000000000000000000.log", "123.log"]


def test_log_starting_after_zero(tmp_path: Path) -> None:
    # After retention removes old segments, the first segment no longer starts at 0.
    Segment(tmp_path, 100).close()
    with Log(tmp_path) as log:
        assert (log.start_offset, log.next_offset) == (100, 100)
        assert log.append(VALUE) == 100
        with pytest.raises(OffsetOutOfRangeError):
            log.read(99)


# --- fsync policy ----------------------------------------------------------------


def test_default_policy_syncs_every_append(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with Log(tmp_path) as log:
        counter = FsyncCounter(monkeypatch)
        fill(log, 3)
        assert counter.calls == 3


def test_interval_policy_syncs_only_on_demand(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with Log(tmp_path, LogConfig(fsync_interval_ms=1000)) as log:
        counter = FsyncCounter(monkeypatch)
        fill(log, 3)
        assert counter.calls == 0
        log.sync()
        assert counter.calls == 1
        log.sync()  # nothing new since the last sync
        assert counter.calls == 1


async def test_periodic_sync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with Log(tmp_path, LogConfig(fsync_interval_ms=10)) as log:
        counter = FsyncCounter(monkeypatch)
        task = asyncio.create_task(run_periodic_sync(log))
        log.append(VALUE)
        for _ in range(100):  # wait up to 1 s, CI machines can be slow
            await asyncio.sleep(0.01)
            if counter.calls:
                break
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert counter.calls == 1


async def test_periodic_sync_requires_interval_policy(tmp_path: Path) -> None:
    with Log(tmp_path) as log, pytest.raises(ValueError):
        await run_periodic_sync(log)


async def test_periodic_sync_stops_on_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with Log(tmp_path, LogConfig(fsync_interval_ms=10)) as log:
        log.append(VALUE)
        monkeypatch.setattr(os, "fsync", failing_fsync)
        with pytest.raises(OSError):
            await asyncio.wait_for(run_periodic_sync(log), timeout=1)
        monkeypatch.undo()


# --- failures --------------------------------------------------------------------


def test_failed_fsync_stops_writes_but_not_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with Log(tmp_path) as log:
        log.append(b"first")
        monkeypatch.setattr(os, "fsync", failing_fsync)
        with pytest.raises(OSError):
            log.append(b"second")
        monkeypatch.undo()
        with pytest.raises(LogFailedError):
            log.append(b"third")
        with pytest.raises(LogFailedError):
            log.sync()
        assert log.read(0)[0].value == b"first"


def test_failed_write_stops_writes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_append(self: Segment, *args: object, **kwargs: object) -> int:
        raise OSError("no space left on device")

    with Log(tmp_path) as log:
        monkeypatch.setattr(Segment, "append", failing_append)
        with pytest.raises(OSError):
            log.append(b"a")
        monkeypatch.undo()
        with pytest.raises(LogFailedError):
            log.append(b"b")


def test_failed_rotation_stops_writes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_fsync_directory(path: Path) -> None:
        raise OSError("directory fsync failed")

    with Log(tmp_path, SMALL) as log:
        fill(log, 4)
        monkeypatch.setattr(log_module, "fsync_directory", failing_fsync_directory)
        with pytest.raises(OSError):
            log.append(VALUE)
        assert log.segment_count == 1
        with pytest.raises(LogFailedError):
            log.append(VALUE)
    # The half-created segment was closed (Windows would refuse to delete it otherwise).
    segment_path(tmp_path, 4).unlink()


def test_reopen_after_failure_accepts_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = Log(tmp_path)
    log.append(b"first")
    monkeypatch.setattr(os, "fsync", failing_fsync)
    with pytest.raises(OSError):
        log.append(b"second")
    with pytest.raises(OSError):
        log.close()  # closing syncs, which still fails
    monkeypatch.undo()
    with Log(tmp_path) as reopened:
        assert reopened.append(b"third") == reopened.next_offset - 1


def test_invalid_record_does_not_break_the_log(tmp_path: Path) -> None:
    with Log(tmp_path) as log:
        with pytest.raises(RecordError):
            log.append(bytes(MAX_RECORD_SIZE))
        assert log.append(b"ok") == 0


def test_closed_log_rejects_operations(tmp_path: Path) -> None:
    log = Log(tmp_path)
    log.close()
    log.close()  # idempotent
    assert log.closed
    with pytest.raises(LogError):
        log.append(b"a")
    with pytest.raises(LogError):
        log.read(0)
    with pytest.raises(LogError):
        log.sync()


def test_close_closes_every_segment_even_if_one_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_close = Segment.close

    def close_failing_first(self: Segment) -> None:
        real_close(self)
        if self.base_offset == 0:
            raise OSError("close failed")

    log = Log(tmp_path, SMALL)
    fill(log, 10)
    segments = list(log._segments)
    monkeypatch.setattr(Segment, "close", close_failing_first)
    with pytest.raises(OSError):
        log.close()
    assert all(segment.closed for segment in segments)


def test_failed_open_keeps_original_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with Log(tmp_path, SMALL) as log:
        fill(log, 10)
    segment_path(tmp_path, 4).unlink()

    real_close = Segment.close

    def failing_close(self: Segment) -> None:
        real_close(self)
        raise OSError("close failed")

    monkeypatch.setattr(Segment, "close", failing_close)
    # The corruption error reaches the caller, not the later error from closing.
    with pytest.raises(LogCorruptError):
        Log(tmp_path, SMALL)


# --- retention -------------------------------------------------------------------


def retention_log(directory: Path, **kwargs: int) -> Log:
    """10 records of 58 bytes with timestamps 0..9 in segments starting at 0, 4 and 8."""
    log = Log(directory, LogConfig(segment_bytes=200, index_interval_bytes=100, **kwargs))
    fill(log, 10)
    return log


@pytest.mark.parametrize("kwargs", [{"retention_bytes": 0}, {"retention_ms": 0}])
def test_invalid_retention_config_is_rejected(kwargs: dict[str, int]) -> None:
    with pytest.raises(ValueError):
        LogConfig(**kwargs)


def test_no_retention_configured_deletes_nothing(tmp_path: Path) -> None:
    with retention_log(tmp_path) as log:
        assert log.apply_retention(now_ms=10**15) == 0
        assert log.segment_count == 3


def test_retention_by_size_keeps_at_least_the_limit(tmp_path: Path) -> None:
    # Segments hold 232, 232 and 116 bytes (580 in total).
    with retention_log(tmp_path, retention_bytes=300) as log:
        # Without the first segment 348 >= 300 bytes remain; without the second only 116.
        assert log.apply_retention() == 1
        assert log.start_offset == 4
        assert log.size_bytes == 348
        assert segment_files(tmp_path) == [
            "00000000000000000004.log",
            "00000000000000000008.log",
        ]
        assert not index_path(tmp_path, 0).exists()


def test_retention_by_size_never_deletes_active_segment(tmp_path: Path) -> None:
    with retention_log(tmp_path, retention_bytes=1) as log:
        assert log.apply_retention() == 2
        assert log.segment_count == 1
        assert log.apply_retention() == 0
        assert [r.offset for r in log.read(8)] == [8, 9]


def test_retention_by_time_uses_newest_record_of_segment(tmp_path: Path) -> None:
    # Newest timestamps: segment 0 -> 3, segment 4 -> 7.
    with retention_log(tmp_path, retention_ms=5) as log:
        assert log.apply_retention(now_ms=8) == 0  # 8 - 3 = 5 is not older than 5
        assert log.apply_retention(now_ms=9) == 1  # 9 - 3 = 6 > 5
        assert log.start_offset == 4
        assert log.apply_retention(now_ms=1000) == 1  # the active segment stays
        assert log.start_offset == 8


def test_retention_by_time_deletes_only_a_prefix(tmp_path: Path) -> None:
    log = Log(tmp_path, LogConfig(segment_bytes=200, retention_ms=100))
    for ts in [1000, 1000, 1000, 1000, 0, 0, 0, 0, 2000]:  # middle segment is the oldest
        log.append(VALUE, timestamp_ms=ts)
    with log:
        # Segment 4 has expired, but segment 0 has not, so nothing is deleted:
        # removing segment 4 alone would leave a gap in offsets.
        assert log.apply_retention(now_ms=1050) == 0
        assert log.segment_count == 3


def test_retention_with_both_limits(tmp_path: Path) -> None:
    with retention_log(tmp_path, retention_bytes=10_000, retention_ms=5) as log:
        assert log.apply_retention(now_ms=9) == 1  # the size is fine, but time expired


def test_read_after_retention(tmp_path: Path) -> None:
    with retention_log(tmp_path, retention_bytes=300) as log:
        log.apply_retention()
        with pytest.raises(OffsetOutOfRangeError):
            log.read(3)
        assert [r.offset for r in log.read(4)] == list(range(4, 10))


def test_reopen_after_retention(tmp_path: Path) -> None:
    with retention_log(tmp_path, retention_bytes=300) as log:
        log.apply_retention()
    with Log(tmp_path, SMALL) as log:
        assert (log.start_offset, log.next_offset) == (4, 10)


def test_retention_syncs_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with retention_log(tmp_path, retention_bytes=300) as log:
        synced_dirs: list[Path] = []
        monkeypatch.setattr(log_module, "fsync_directory", synced_dirs.append)
        log.apply_retention()
        assert synced_dirs == [tmp_path]


def test_failed_delete_stops_writes_and_reopen_is_consistent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_unlink = Path.unlink

    def failing_unlink(self: Path, missing_ok: bool = False) -> None:
        if self.suffix == ".log":
            raise PermissionError("file is in use")
        real_unlink(self, missing_ok=missing_ok)

    with retention_log(tmp_path, retention_bytes=300) as log:
        monkeypatch.setattr(Path, "unlink", failing_unlink)
        with pytest.raises(PermissionError):
            log.apply_retention()
        monkeypatch.undo()
        assert log.start_offset == 4  # memory never refers to the closed segment
        with pytest.raises(LogFailedError):
            log.append(VALUE)
    with Log(tmp_path, SMALL) as log:
        # The segment that could not be deleted is the first one again; no gap.
        assert (log.start_offset, log.next_offset) == (0, 10)


def test_failed_index_delete_is_only_a_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    real_unlink = Path.unlink

    def failing_unlink(self: Path, missing_ok: bool = False) -> None:
        if self.suffix == ".index":
            raise PermissionError("file is in use")
        real_unlink(self, missing_ok=missing_ok)

    with retention_log(tmp_path, retention_bytes=300) as log:
        monkeypatch.setattr(Path, "unlink", failing_unlink)
        assert log.apply_retention() == 1
        monkeypatch.undo()
        assert log.append(VALUE) == 10  # the log keeps working
    assert "could not delete 00000000000000000000.index" in caplog.text
    with Log(tmp_path, SMALL) as log:
        assert log.start_offset == 4  # the leftover index file is ignored


def test_retention_on_closed_log_raises(tmp_path: Path) -> None:
    log = retention_log(tmp_path, retention_bytes=300)
    log.close()
    with pytest.raises(LogError):
        log.apply_retention()


def test_append_batch(tmp_path: Path) -> None:
    with Log(tmp_path) as log:
        log.append(b"first")
        assert log.append_batch([(b"a", None), (b"b", b"key")], timestamp_ms=5) == 1
        assert log.read(1) == [Record(1, 5, None, b"a"), Record(2, 5, b"key", b"b")]
        assert log.next_offset == 3


def test_append_batch_syncs_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0
    real_sync = Segment.sync

    def counting_sync(self: Segment) -> None:
        nonlocal calls
        calls += 1
        real_sync(self)

    with Log(tmp_path) as log:
        monkeypatch.setattr(Segment, "sync", counting_sync)
        log.append_batch([(VALUE, None)] * 10)
        assert calls == 1


def test_append_batch_rolls_segments(tmp_path: Path) -> None:
    with Log(tmp_path, LogConfig(segment_bytes=200)) as log:
        assert log.append_batch([(VALUE, None)] * 10) == 0
        assert log.segment_count > 1
        assert [record.offset for record in log.read(0, 10_000)] == list(range(10))


def test_append_batch_checks_every_record_first(tmp_path: Path) -> None:
    with Log(tmp_path) as log:
        with pytest.raises(RecordError):
            log.append_batch([(b"ok", None), (bytes(MAX_RECORD_SIZE), None)])
        assert log.next_offset == 0
        assert log.size_bytes == 0
        assert log.append(b"still writable") == 0


def test_empty_batch_is_rejected(tmp_path: Path) -> None:
    with Log(tmp_path) as log, pytest.raises(ValueError, match="empty"):
        log.append_batch([])


def test_failed_write_in_batch_stops_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_append = Segment.append
    calls = 0

    def append_then_fail(
        self: Segment, value: bytes, key: bytes | None = None, timestamp_ms: int | None = None
    ) -> int:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("no space left on device")
        return real_append(self, value, key, timestamp_ms)

    with Log(tmp_path) as log:
        monkeypatch.setattr(Segment, "append", append_then_fail)
        with pytest.raises(OSError):
            log.append_batch([(b"a", None), (b"b", None), (b"c", None)])
        monkeypatch.undo()
        with pytest.raises(LogFailedError):
            log.append(b"d")
    with Log(tmp_path) as reopened:
        # The unacknowledged batch left a prefix behind; the log is still consistent.
        assert [record.value for record in reopened.read(0)] == [b"a"]
