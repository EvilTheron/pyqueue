import os
from pathlib import Path

import pytest

from pyqueue.record import Record, encode_record
from pyqueue.segment import Segment, segment_path


def fill(directory: Path, count: int, base_offset: int = 0) -> Segment:
    segment = Segment(directory, base_offset)
    for i in range(count):
        segment.append(f"value {i}".encode(), key=f"key {i}".encode(), timestamp_ms=i)
    return segment


def test_new_segment_is_empty(tmp_path: Path) -> None:
    with Segment(tmp_path, 0) as segment:
        assert segment.path.name == "00000000000000000000.log"
        assert segment.size == 0
        assert segment.next_offset == 0
        assert list(segment.records()) == []


def test_negative_base_offset_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        Segment(tmp_path, -1)


def test_append_assigns_offsets_from_base(tmp_path: Path) -> None:
    with Segment(tmp_path, 100) as segment:
        assert [segment.append(b"a"), segment.append(b"b"), segment.append(b"c")] == [100, 101, 102]
        assert segment.next_offset == 103


def test_append_fills_timestamp_when_missing(tmp_path: Path) -> None:
    with Segment(tmp_path, 0) as segment:
        segment.append(b"a")
        _, record = next(segment.records())
        assert record.timestamp_ms > 1_600_000_000_000


def test_records_and_read_at(tmp_path: Path) -> None:
    with fill(tmp_path, 3) as segment:
        pairs = list(segment.records())
        assert [r.offset for _, r in pairs] == [0, 1, 2]
        assert pairs[1][1] == Record(1, 1, b"key 1", b"value 1")
        position, record = pairs[2]
        assert segment.read_at(position)[0] == record
        assert list(segment.records(position)) == [pairs[2]]


@pytest.mark.parametrize("position", [-1, 10_000])
def test_read_at_outside_segment_raises(tmp_path: Path, position: int) -> None:
    with fill(tmp_path, 1) as segment, pytest.raises(ValueError):
        segment.read_at(position)


def test_reopen_keeps_records_and_continues_offsets(tmp_path: Path) -> None:
    fill(tmp_path, 3, base_offset=10).close()
    with Segment(tmp_path, 10) as segment:
        assert segment.truncated_bytes == 0
        assert segment.next_offset == 13
        assert segment.append(b"next") == 13
        assert len(list(segment.records())) == 4


def test_records_see_appends_made_during_iteration(tmp_path: Path) -> None:
    with fill(tmp_path, 1) as segment:
        seen = []
        for _, record in segment.records():
            seen.append(record.offset)
            if record.offset == 0:
                segment.append(b"late")
        assert seen == [0, 1]


def corrupt_and_reopen(
    tmp_path: Path, mutate: bytes | None = None, flip: int | None = None
) -> Segment:
    """Write 3 records, damage the file, reopen. Return the reopened segment."""
    segment = fill(tmp_path, 3)
    positions = [p for p, _ in segment.records()]
    segment.close()
    path = segment_path(tmp_path, 0)
    data = bytearray(path.read_bytes())
    if flip is not None:
        data[positions[flip] + 10] ^= 0xFF  # a byte inside that record's offset field
    if mutate is not None:
        data += mutate
    path.write_bytes(bytes(data))
    return Segment(tmp_path, 0)


@pytest.mark.parametrize(
    "tail",
    [
        b"\x00\x00",  # cut inside a length prefix
        encode_record(Record(3, 0, None, b"half written"))[:-3],  # cut inside a record
        b"\xff\xff\xff\xff" + bytes(100),  # garbage length
    ],
)
def test_damaged_tail_is_truncated(tmp_path: Path, tail: bytes) -> None:
    with corrupt_and_reopen(tmp_path, mutate=tail) as segment:
        assert segment.truncated_bytes == len(tail)
        assert segment.next_offset == 3
        assert os.path.getsize(segment.path) == segment.size
        assert segment.append(b"after recovery") == 3
        assert [r.offset for _, r in segment.records()] == [0, 1, 2, 3]


def test_checksum_failure_drops_record_and_everything_after(tmp_path: Path) -> None:
    with corrupt_and_reopen(tmp_path, flip=1) as segment:
        assert [r.offset for _, r in segment.records()] == [0]
        assert segment.next_offset == 1


def test_record_with_unexpected_offset_is_truncated(tmp_path: Path) -> None:
    # A valid record (correct checksum) whose offset breaks the sequence.
    stray = encode_record(Record(7, 0, None, b"wrong offset"))
    with corrupt_and_reopen(tmp_path, mutate=stray) as segment:
        assert segment.truncated_bytes == len(stray)
        assert segment.next_offset == 3


def test_sync_calls_fsync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []
    real_fsync = os.fsync

    def counting_fsync(fd: int) -> None:
        calls.append(fd)
        real_fsync(fd)

    with Segment(tmp_path, 0) as segment:
        monkeypatch.setattr(os, "fsync", counting_fsync)
        segment.append(b"a")
        assert calls == []  # append alone does not force data to disk
        segment.sync()
        assert len(calls) == 1


def test_close_is_idempotent(tmp_path: Path) -> None:
    segment = fill(tmp_path, 1)
    segment.close()
    segment.close()


def test_failed_recovery_closes_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fill(tmp_path, 1).close()

    def broken_recover(self: Segment) -> None:
        raise OSError("disk error")

    monkeypatch.setattr(Segment, "_recover", broken_recover)
    with pytest.raises(OSError):
        Segment(tmp_path, 0)
    # On Windows an open handle would block deletion.
    segment_path(tmp_path, 0).unlink()
