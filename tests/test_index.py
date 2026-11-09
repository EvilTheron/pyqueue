import struct
from pathlib import Path

import pytest

from pyqueue.index import ENTRY, Index, index_path


def test_new_index_is_empty(tmp_path: Path) -> None:
    with Index(tmp_path, 0) as index:
        assert index.path.name == "00000000000000000000.index"
        assert len(index) == 0
        assert index.lookup(5) == 0


def test_lookup_returns_closest_entry_at_or_before(tmp_path: Path) -> None:
    with Index(tmp_path, 100) as index:
        index.append(110, 4000)
        index.append(120, 8000)
        assert index.lookup(105) == 0  # before the first entry: scan from the start
        assert index.lookup(110) == 4000
        assert index.lookup(119) == 4000
        assert index.lookup(120) == 8000
        assert index.lookup(10_000) == 8000


def test_entries_are_stored_relative_to_base(tmp_path: Path) -> None:
    with Index(tmp_path, 100) as index:
        index.append(110, 4000)
    assert index_path(tmp_path, 100).read_bytes() == struct.pack(">II", 10, 4000)


def test_reopen_loads_entries(tmp_path: Path) -> None:
    with Index(tmp_path, 100) as index:
        index.append(110, 4000)
        index.append(120, 8000)
    with Index(tmp_path, 100) as index:
        assert index.entries() == [(110, 4000), (120, 8000)]


@pytest.mark.parametrize(("offset", "position"), [(110, 5000), (115, 4000), (105, 3000)])
def test_append_rejects_entry_not_after_last(tmp_path: Path, offset: int, position: int) -> None:
    with Index(tmp_path, 100) as index:
        index.append(110, 4000)
        with pytest.raises(ValueError):
            index.append(offset, position)


@pytest.mark.parametrize(("offset", "position"), [(99, 0), (100 + 2**32, 0), (100, 2**32)])
def test_append_rejects_out_of_range_entry(tmp_path: Path, offset: int, position: int) -> None:
    with Index(tmp_path, 100) as index, pytest.raises(ValueError):
        index.append(offset, position)


def test_partial_trailing_entry_is_dropped(tmp_path: Path) -> None:
    index_path(tmp_path, 0).write_bytes(ENTRY.pack(1, 100) + b"\x00\x00\x00")
    with Index(tmp_path, 0) as index:
        assert index.entries() == [(1, 100)]
    assert index_path(tmp_path, 0).stat().st_size == ENTRY.size


def test_entries_after_a_non_increasing_one_are_dropped(tmp_path: Path) -> None:
    data = ENTRY.pack(1, 100) + ENTRY.pack(2, 50) + ENTRY.pack(3, 300)
    index_path(tmp_path, 0).write_bytes(data)
    with Index(tmp_path, 0) as index:
        assert index.entries() == [(1, 100)]


def test_clear(tmp_path: Path) -> None:
    with Index(tmp_path, 0) as index:
        index.append(1, 100)
        index.clear()
        assert len(index) == 0
        index.append(1, 100)
    assert index_path(tmp_path, 0).stat().st_size == ENTRY.size


def test_close_is_idempotent(tmp_path: Path) -> None:
    index = Index(tmp_path, 0)
    index.close()
    index.close()


def test_failed_load_closes_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def broken_load(self: Index) -> None:
        raise OSError("disk error")

    monkeypatch.setattr(Index, "_load", broken_load)
    with pytest.raises(OSError):
        Index(tmp_path, 0)
    index_path(tmp_path, 0).unlink()  # an open handle would block this on Windows


def test_close_releases_file_even_if_fsync_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    index = Index(tmp_path, 0)

    def failing_fsync(fd: int) -> None:
        raise OSError("fsync failed")

    monkeypatch.setattr("os.fsync", failing_fsync)
    with pytest.raises(OSError):
        index.close()
    assert index.closed
    monkeypatch.undo()
    index_path(tmp_path, 0).unlink()  # an open handle would block this on Windows
