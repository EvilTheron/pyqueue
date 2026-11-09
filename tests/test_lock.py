import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

import pyqueue.lock as lock_module
from pyqueue.lock import LOCK_FILE, DirectoryLock, DirectoryLockedError
from pyqueue.log import Log, LogConfig, LogCorruptError
from pyqueue.segment import segment_path

# A separate process that takes the lock, reports it and waits to be killed.
HOLDER = """
import sys, time
from pathlib import Path
from pyqueue.lock import DirectoryLock
DirectoryLock(Path(sys.argv[1])).acquire()
print("locked", flush=True)
time.sleep(60)
"""


@pytest.fixture
def holder(tmp_path: Path) -> Iterator[subprocess.Popen[str]]:
    process = subprocess.Popen(
        [sys.executable, "-c", HOLDER, str(tmp_path)], stdout=subprocess.PIPE, text=True
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline() == "locked\n"
        yield process
    finally:
        process.kill()
        process.wait()
        if process.stdout is not None:
            process.stdout.close()


def test_acquire_and_release(tmp_path: Path) -> None:
    lock = DirectoryLock(tmp_path)
    lock.acquire()
    assert lock.locked
    assert (tmp_path / LOCK_FILE).exists()
    lock.release()
    assert not lock.locked
    lock.acquire()  # can be taken again after release
    lock.release()


def test_second_lock_in_same_process_fails(tmp_path: Path) -> None:
    with DirectoryLock(tmp_path), pytest.raises(DirectoryLockedError):
        DirectoryLock(tmp_path).acquire()


def test_lock_is_free_after_context_exit(tmp_path: Path) -> None:
    with DirectoryLock(tmp_path):
        pass
    with DirectoryLock(tmp_path) as lock:
        assert lock.locked


def test_acquire_twice_on_same_object_is_a_bug(tmp_path: Path) -> None:
    with DirectoryLock(tmp_path) as lock, pytest.raises(RuntimeError):
        lock.acquire()


def test_release_without_acquire_is_harmless(tmp_path: Path) -> None:
    DirectoryLock(tmp_path).release()


def test_unexpected_error_closes_file_and_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken_lock(fd: int) -> None:
        raise RuntimeError("unexpected")

    monkeypatch.setattr(lock_module, "_try_lock", broken_lock)
    lock = DirectoryLock(tmp_path)
    with pytest.raises(RuntimeError):
        lock.acquire()
    assert not lock.locked
    monkeypatch.undo()
    (tmp_path / LOCK_FILE).unlink()  # the descriptor was closed (Windows would refuse otherwise)


def test_missing_directory_raises_os_error(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        DirectoryLock(tmp_path / "missing").acquire()


def test_lock_held_by_another_process(tmp_path: Path, holder: subprocess.Popen[str]) -> None:
    with pytest.raises(DirectoryLockedError):
        DirectoryLock(tmp_path).acquire()


def test_lock_is_released_when_holder_is_killed(
    tmp_path: Path, holder: subprocess.Popen[str]
) -> None:
    holder.kill()
    holder.wait()
    # No stale lock: the operating system released it together with the process.
    with DirectoryLock(tmp_path) as lock:
        assert lock.locked


# --- integration with Log ----------------------------------------------------------


def test_second_log_on_same_directory_fails(tmp_path: Path) -> None:
    with Log(tmp_path), pytest.raises(DirectoryLockedError):
        Log(tmp_path)


def test_log_can_be_reopened_after_close(tmp_path: Path) -> None:
    Log(tmp_path).close()
    with Log(tmp_path) as log:
        assert log.next_offset == 0


def test_log_held_by_another_process(tmp_path: Path, holder: subprocess.Popen[str]) -> None:
    with pytest.raises(DirectoryLockedError):
        Log(tmp_path)
    assert list(tmp_path.glob("*.log")) == []  # nothing was created or touched


def test_failed_open_releases_lock(tmp_path: Path) -> None:
    config = LogConfig(segment_bytes=100)
    with Log(tmp_path, config) as log:
        for _ in range(5):
            log.append(b"x" * 60)  # 88 bytes each: segments start at offsets 0, 2 and 4
    segment_path(tmp_path, 2).unlink()  # a gap between segments
    with pytest.raises(LogCorruptError):
        Log(tmp_path, config)
    with DirectoryLock(tmp_path) as lock:
        assert lock.locked


def test_lock_file_is_not_a_segment(tmp_path: Path) -> None:
    with Log(tmp_path) as log:
        log.append(b"a")
    with Log(tmp_path) as log:
        assert log.segment_count == 1
