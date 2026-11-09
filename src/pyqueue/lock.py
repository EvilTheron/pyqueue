"""Exclusive lock on a data directory, so that two processes never write the same log.

The lock is taken on a `.lock` file inside the directory with the operating system's
file locking:

* Linux and macOS: `fcntl.flock`. The lock is advisory: it stops other processes that
  also take it (every PyQueue process does), not arbitrary programs.
* Windows: `msvcrt.locking` on the first byte of the file. The lock is mandatory for
  that byte only; the rest of the directory is not protected by the OS either.

In both cases the operating system releases the lock when the process exits, including
when it is killed, so a crash never leaves a stale lock behind. The `.lock` file itself
stays in the directory; its presence means nothing.

Locks are per open file, not per process: opening the same directory twice in one
process also fails, which is what we want for two `Log` objects. File locks may not
work on network file systems (NFS, SMB); keep data directories on a local disk.
"""

import os
import sys
from pathlib import Path
from types import TracebackType
from typing import Self

LOCK_FILE = ".lock"


class DirectoryLockedError(Exception):
    """Another process (or another object in this process) holds the directory lock."""


if sys.platform == "win32":
    import msvcrt

    def _try_lock(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        # Non-blocking lock of one byte; locking past the end of the file is allowed.
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)

    def _unlock(fd: int) -> None:
        # Windows releases locks of a closed handle only eventually, so unlock explicitly.
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)

    # msvcrt.locking reports a held lock as a plain OSError (EACCES).
    _LOCK_HELD_ERRORS: tuple[type[OSError], ...] = (OSError,)

else:
    import fcntl

    def _try_lock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)

    _LOCK_HELD_ERRORS: tuple[type[OSError], ...] = (BlockingIOError,)


class DirectoryLock:
    def __init__(self, directory: Path) -> None:
        self.path = directory / LOCK_FILE
        self._fd: int | None = None

    @property
    def locked(self) -> bool:
        return self._fd is not None

    def acquire(self) -> None:
        """Take the lock or raise `DirectoryLockedError` at once (never waits)."""
        if self._fd is not None:
            raise RuntimeError(f"{self.path} is already locked by this object")
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            _try_lock(fd)
        except _LOCK_HELD_ERRORS as exc:
            os.close(fd)
            raise DirectoryLockedError(
                f"{self.path.parent} is in use by another process or Log object"
            ) from exc
        except BaseException:
            os.close(fd)
            raise
        self._fd = fd

    def release(self) -> None:
        """Release the lock. Safe to call when it is not held."""
        if self._fd is None:
            return
        fd, self._fd = self._fd, None
        try:
            _unlock(fd)
        finally:
            os.close(fd)

    def __enter__(self) -> Self:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()
