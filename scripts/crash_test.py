"""Crash test: kill a writing process abruptly and check that no acknowledged record is lost.

Each round starts a writer subprocess that appends records to a log and prints the offset
of every record after `append()` returns, that is, after the record is acknowledged. The
parent kills the writer at a random moment (SIGKILL on Linux, TerminateProcess on Windows),
then reopens the log and checks that:

* the log opens (the torn tail, if any, is repaired);
* every acknowledged offset is present;
* offsets are consecutive and every record holds the value written for its offset.

The next round reopens the same log and keeps writing, which also checks that a
recovered log accepts new records. Small segments make crashes hit rotations too.

Scope: this simulates a process crash. It does not simulate a power loss: after a process
crash the operating system still holds data that was not synced yet, so the test would
pass with any fsync policy. Surviving a power loss is what fsync is for, and it cannot be
checked by killing a process.

Usage:
    uv run python scripts/crash_test.py [--rounds 20] [--dir DIR]
"""

import argparse
import random
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from pyqueue.log import Log, LogConfig

CONFIG = LogConfig(segment_bytes=64 * 1024)


def value_for(offset: int) -> bytes:
    return f"record {offset};".encode() * 16


def write(directory: Path) -> None:
    """Writer process: append forever, print each acknowledged offset."""
    with Log(directory, CONFIG) as log:
        while True:
            offset = log.append(value_for(log.next_offset))
            print(offset, flush=True)


def run_round(directory: Path) -> tuple[int | None, int]:
    """Start a writer, kill it at a random moment, return (last acknowledged offset, kept)."""
    writer = subprocess.Popen(
        [sys.executable, __file__, "write", str(directory)],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert writer.stdout is not None
    last_acked: int | None = None
    deadline = time.monotonic() + random.uniform(0.3, 1.5)
    while time.monotonic() < deadline:
        line = writer.stdout.readline()
        if not line:
            break
        last_acked = int(line)
    # Check before killing: the exit code of a killed process differs between operating
    # systems (negative on Linux, 1 on Windows), so it cannot tell a kill from a crash.
    if writer.poll() is not None:
        sys.exit(f"writer exited on its own with code {writer.returncode}")
    writer.kill()
    writer.wait()
    # Lines printed before the kill were acknowledged too; a cut-off last line was not.
    for line in writer.stdout:
        if line.endswith("\n"):
            last_acked = int(line)

    with Log(directory, CONFIG) as log:
        if last_acked is not None and log.next_offset <= last_acked:
            sys.exit(f"LOST: offset {last_acked} was acknowledged, log ends at {log.next_offset}")
        offset = log.start_offset
        while offset < log.next_offset:
            for record in log.read(offset, max_bytes=1024 * 1024):
                if record.offset != offset or record.value != value_for(offset):
                    sys.exit(f"CORRUPT: unexpected record at offset {offset}")
                offset += 1
        return last_acked, log.next_offset


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("mode", nargs="?", default="run", choices=["run", "write"])
    parser.add_argument("directory", nargs="?", type=Path)
    parser.add_argument("--rounds", type=int, default=20)
    args = parser.parse_args()

    if args.mode == "write":
        write(args.directory)
        return

    with tempfile.TemporaryDirectory() as tmp:
        directory = args.directory or Path(tmp) / "log"
        for round_number in range(1, args.rounds + 1):
            last_acked, kept = run_round(directory)
            acked = "none" if last_acked is None else str(last_acked)
            print(f"round {round_number:2}: last acknowledged {acked:>6}, log holds {kept} records")
        segments = len(list(directory.glob("*.log")))
        print(f"OK: no acknowledged record lost in {args.rounds} crashes ({segments} segments)")


if __name__ == "__main__":
    main()
