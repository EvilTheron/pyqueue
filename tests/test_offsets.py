import shutil
from pathlib import Path

import pytest

from pyqueue.log import Log, LogError, LogFailedError
from pyqueue.offsets import (
    COMPACTING_SUFFIX,
    OLD_SUFFIX,
    OffsetStore,
    OffsetStoreCorruptError,
    decode_key,
    encode_key,
)

COMPACT_BYTES = 2000


def store_dir(tmp_path: Path) -> Path:
    return tmp_path / "~offsets"


def compacting_dir(tmp_path: Path) -> Path:
    return tmp_path / f"~offsets{COMPACTING_SUFFIX}"


def old_dir(tmp_path: Path) -> Path:
    return tmp_path / f"~offsets{OLD_SUFFIX}"


def churn(store: OffsetStore, commits: int, keys: int = 3) -> dict[tuple[str, str, int], int]:
    """Commit many offsets for a few keys; return the expected latest values."""
    expected = {}
    for number in range(commits):
        key = ("group", "orders", number % keys)
        store.commit(*key, number)
        expected[key] = number
    return expected


def assert_offsets(store: OffsetStore, expected: dict[tuple[str, str, int], int]) -> None:
    for key, offset in expected.items():
        assert store.get(*key) == offset


@pytest.mark.parametrize(
    "key", [("g", "t", 0), ("", "", -1), ("группа", "orders", 2**31 - 1), ("g" * 300, "t", 5)]
)
def test_key_round_trip(key: tuple[str, str, int]) -> None:
    assert decode_key(encode_key(*key)) == key


@pytest.mark.parametrize(
    "data", [b"", b"\x00\x05ab", encode_key("g", "t", 1) + b"x", encode_key("g", "t", 1)[:-1]]
)
def test_invalid_key_is_rejected(data: bytes) -> None:
    with pytest.raises(OffsetStoreCorruptError):
        decode_key(data)


def test_commit_get_and_reopen(tmp_path: Path) -> None:
    with_store = OffsetStore(store_dir(tmp_path))
    assert with_store.get("billing", "orders", 0) is None
    with_store.commit("billing", "orders", 0, 5)
    with_store.commit("billing", "orders", 0, 7)
    with_store.commit("billing", "orders", 1, 1)
    with_store.close()
    with_store.close()  # idempotent

    reopened = OffsetStore(store_dir(tmp_path))
    assert reopened.get("billing", "orders", 0) == 7
    assert reopened.get("billing", "orders", 1) == 1
    assert reopened.get("audit", "orders", 0) is None
    reopened.close()


def test_negative_offset_is_rejected(tmp_path: Path) -> None:
    store = OffsetStore(store_dir(tmp_path))
    with pytest.raises(ValueError):
        store.commit("g", "t", 0, -1)
    store.close()


def test_invalid_compact_bytes(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        OffsetStore(store_dir(tmp_path), compact_bytes=0)


def test_compaction_keeps_latest_values_and_shrinks_the_log(tmp_path: Path) -> None:
    store = OffsetStore(store_dir(tmp_path), COMPACT_BYTES)
    expected = churn(store, 500)
    assert store.size_bytes < 2 * COMPACT_BYTES  # without compaction: about 35 KB
    assert_offsets(store, expected)
    assert not compacting_dir(tmp_path).exists()
    assert not old_dir(tmp_path).exists()
    store.close()

    reopened = OffsetStore(store_dir(tmp_path), COMPACT_BYTES)
    assert_offsets(reopened, expected)
    reopened.close()


def test_large_live_set_is_not_compacted_on_every_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = OffsetStore(store_dir(tmp_path), COMPACT_BYTES)
    compactions = 0
    real_compact = OffsetStore._compact

    def counting_compact(self: OffsetStore) -> None:
        nonlocal compactions
        compactions += 1
        real_compact(self)

    monkeypatch.setattr(OffsetStore, "_compact", counting_compact)
    churn(store, 1000, keys=200)  # live data is several times larger than COMPACT_BYTES
    assert 0 < compactions < 20
    store.close()


def test_failure_while_writing_snapshot_keeps_current_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = OffsetStore(store_dir(tmp_path), COMPACT_BYTES)

    real_append_batch = Log.append_batch

    def failing_snapshot(
        self: Log, batch: list[tuple[bytes, bytes | None]], timestamp_ms: int | None = None
    ) -> int:
        if self.directory.name.endswith(COMPACTING_SUFFIX):
            raise OSError("disk full")
        return real_append_batch(self, batch, timestamp_ms)

    monkeypatch.setattr(Log, "append_batch", failing_snapshot)
    expected = churn(store, 100)  # compaction is attempted and fails, commits go on
    monkeypatch.undo()
    assert_offsets(store, expected)
    assert not compacting_dir(tmp_path).exists()
    store.close()
    reopened = OffsetStore(store_dir(tmp_path), COMPACT_BYTES)
    assert_offsets(reopened, expected)
    reopened.close()


def test_failure_between_renames_is_finished_at_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = OffsetStore(store_dir(tmp_path), COMPACT_BYTES)
    real_rename = Path.rename

    def failing_second_rename(self: Path, target: Path) -> Path:
        if self.name.endswith(COMPACTING_SUFFIX):
            raise OSError("crash between step 2 and step 3")
        return real_rename(self, target)

    monkeypatch.setattr(Path, "rename", failing_second_rename)
    expected: dict[tuple[str, str, int], int] = {}
    with pytest.raises(LogFailedError):
        for number in range(1000):
            key = ("group", "orders", number % 3)
            store.commit(*key, number)
            expected[key] = number  # acknowledged
    monkeypatch.undo()
    assert not store_dir(tmp_path).exists()
    assert old_dir(tmp_path).exists() and compacting_dir(tmp_path).exists()
    assert_offsets(store, expected)  # reads still work
    store.close()

    reopened = OffsetStore(store_dir(tmp_path), COMPACT_BYTES)
    assert_offsets(reopened, expected)  # every acknowledged commit survived
    assert not old_dir(tmp_path).exists()
    assert not compacting_dir(tmp_path).exists()
    reopened.close()


def test_failure_deleting_old_log_is_harmless(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = OffsetStore(store_dir(tmp_path), COMPACT_BYTES)
    real_rmtree = shutil.rmtree

    def rmtree_except_old(path: Path, *args: object, **kwargs: object) -> None:
        if Path(path).name.endswith(OLD_SUFFIX):
            raise OSError("file in use")
        real_rmtree(path)

    monkeypatch.setattr(shutil, "rmtree", rmtree_except_old)
    # The first compaction leaves the old log; later ones cannot delete it and give up.
    expected = churn(store, 300)
    monkeypatch.undo()
    assert old_dir(tmp_path).exists()
    assert_offsets(store, expected)
    store.close()

    reopened = OffsetStore(store_dir(tmp_path), COMPACT_BYTES)
    assert_offsets(reopened, expected)
    assert not old_dir(tmp_path).exists()
    reopened.close()


def test_unfinished_snapshot_is_removed_at_startup(tmp_path: Path) -> None:
    store = OffsetStore(store_dir(tmp_path))
    store.commit("g", "orders", 0, 3)
    store.close()
    (compacting_dir(tmp_path) / "garbage").mkdir(parents=True)

    reopened = OffsetStore(store_dir(tmp_path))
    assert reopened.get("g", "orders", 0) == 3
    assert not compacting_dir(tmp_path).exists()
    reopened.close()


def test_compaction_removes_a_stale_snapshot(tmp_path: Path) -> None:
    store = OffsetStore(store_dir(tmp_path), COMPACT_BYTES)
    (compacting_dir(tmp_path) / "garbage").mkdir(parents=True)
    expected = churn(store, 200)
    assert store.size_bytes < 2 * COMPACT_BYTES
    assert not compacting_dir(tmp_path).exists()
    assert_offsets(store, expected)
    store.close()


def test_old_log_alone_refuses_to_start(tmp_path: Path) -> None:
    old_dir(tmp_path).mkdir()
    with pytest.raises(OffsetStoreCorruptError):
        OffsetStore(store_dir(tmp_path))
    assert old_dir(tmp_path).exists()  # nothing was deleted


@pytest.mark.parametrize(("key", "value"), [(None, bytes(8)), (b"\x00", bytes(8)), (b"k", b"x")])
def test_foreign_record_refuses_to_start(tmp_path: Path, key: bytes | None, value: bytes) -> None:
    with Log(store_dir(tmp_path)) as log:
        log.append(bytes(8), encode_key("g", "t", 0))
        log.append(value, key)
    with pytest.raises(OffsetStoreCorruptError):
        OffsetStore(store_dir(tmp_path))
    OffsetStore(tmp_path / "other").close()
    with Log(store_dir(tmp_path)):  # the failed store closed its log
        pass


def test_closed_store_rejects_operations(tmp_path: Path) -> None:
    store = OffsetStore(store_dir(tmp_path))
    store.close()
    with pytest.raises(LogError, match="closed"):
        store.get("g", "t", 0)
    with pytest.raises(LogError, match="closed"):
        store.commit("g", "t", 0, 1)
