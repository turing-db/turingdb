#!/usr/bin/env python3
"""Run a command while holding an exclusive, host-global lock.

The regress suite assumes exclusive use of the machine: every test binds the fixed
port 6666 and runs a host-wide `pkill -9 turingdb`. Several self-hosted runner
instances share one physical host, so concurrent regress jobs would kill each
other's servers and collide on the port. This serializes them with a kernel flock
on a shared lock file. The lock is held only while this process lives and the OS
releases it automatically on exit -- including a hard SIGKILL -- so there is no
stale-lock bookkeeping to get wrong.

Usage: with_host_lock.py LOCK_FILE COMMAND [ARG ...]
"""

import fcntl
import os
import subprocess
import sys
import time

POLL_SECONDS = 10


def main():
    if len(sys.argv) < 3:
        sys.exit(f"usage: {sys.argv[0]} LOCK_FILE COMMAND [ARG ...]")

    lock_file = sys.argv[1]
    command = sys.argv[2:]

    fd = os.open(lock_file, os.O_CREAT | os.O_RDWR, 0o666)
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                print(f"Waiting for host lock {lock_file} ...", flush=True)
                time.sleep(POLL_SECONDS)

        os.ftruncate(fd, 0)
        os.write(fd, f"{os.environ.get('GITHUB_RUN_ID', '?')}\n".encode())

        return subprocess.run(command).returncode
    finally:
        os.close(fd)


if __name__ == "__main__":
    sys.exit(main())
