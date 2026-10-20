#!/usr/bin/env python3
"""Restore and save a directory in the cache the self-hosted runners of a host share.

Every runner container on a host mounts the same cache directory, so a key one job
saves is restored by the others. An entry is published by renaming a complete staging
directory into place, so a reader sees all of it or none of it. A restore refreshes the
entry's mtime, and the entries matching --clean-key that no job has restored for a week
are deleted.

Usage:
  ci_cache.py restore --base DIR --key KEY --path PATH [--clean-key PREFIX]
  ci_cache.py save --base DIR --key KEY --path PATH
"""

import argparse
import contextlib
import errno
import fcntl
import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path

UNUSED_SECONDS = 7 * 24 * 3600
ABANDONED_SECONDS = 24 * 3600


@contextlib.contextmanager
def locked(base, operation):
    fd = os.open(base / ".lock", os.O_CREAT | os.O_RDWR, 0o666)
    try:
        fcntl.flock(fd, operation)
        yield
    finally:
        os.close(fd)


def unique_path(directory, name):
    directory.mkdir(exist_ok=True)
    return directory / f"{name}.{uuid.uuid4().hex}"


def remove(path):
    if path.is_symlink() or path.is_file():
        path.unlink()
    elif path.is_dir():
        shutil.rmtree(path)


def link(path, target):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.symlink_to(target, target_is_directory=True)


def set_output(name, value):
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a") as file:
            file.write(f"{name}={value}\n")


def clean(base, prefix):
    now = time.time()
    trash = base / ".trash"
    doomed = []

    for directory in (base / ".staging", trash):
        if directory.is_dir():
            for leftover in directory.iterdir():
                if now - leftover.stat().st_mtime > ABANDONED_SECONDS:
                    doomed.append(leftover)

    with locked(base, fcntl.LOCK_EX):
        for entry in base.iterdir():
            matches = entry.name.startswith(prefix) and entry.is_dir()
            if matches and now - entry.stat().st_mtime > UNUSED_SECONDS:
                target = unique_path(trash, entry.name)
                entry.rename(target)
                doomed.append(target)

    for path in doomed:
        print(f"Deleting {path.name}")
        shutil.rmtree(path, ignore_errors=True)


def restore(base, key, path, clean_key):
    base.mkdir(parents=True, exist_ok=True)
    entry = base / key
    remove(path)

    with locked(base, fcntl.LOCK_SH):
        hit = entry.is_dir()
        if hit:
            os.utime(entry)

    if hit:
        link(path, entry / path.name)
        print(f"Cache restored with key {key}")
    else:
        print(f"Cache not found for {key}")

    set_output("cache-hit", "true" if hit else "false")
    set_output("key", key)

    if clean_key:
        clean(base, clean_key)


def save(base, key, path):
    entry = base / key
    staging = unique_path(base / ".staging", key)
    staging.mkdir()
    subprocess.run(["mv", str(path), str(staging / path.name)], check=True)

    try:
        staging.rename(entry)
        print(f"Cache saved with key {key}")
    except OSError as error:
        if error.errno not in (errno.EEXIST, errno.ENOTEMPTY):
            raise
        print(f"Cache already saved with key {key} by another job")
        shutil.rmtree(staging)

    link(path, entry / path.name)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["restore", "save"])
    parser.add_argument("--base", required=True, type=Path)
    parser.add_argument("--key", required=True)
    parser.add_argument("--path", required=True, type=Path)
    parser.add_argument("--clean-key")
    args = parser.parse_args()

    if "/" in args.key or args.key.startswith("."):
        parser.error(f"--key must be a plain name not starting with '.': {args.key}")

    if args.command == "restore":
        restore(args.base, args.key, args.path, args.clean_key)
    else:
        save(args.base, args.key, args.path)


if __name__ == "__main__":
    main()
