#!/usr/bin/env python3
"""Print the project version, formatted for one packaging ecosystem.

The wheel and the npm package are cut from the same v* tags and must agree on what
version a commit is, but they cannot spell it the same way: PEP 440 writes a
prerelease as 1.37.1.dev202609171440 and semver as 1.37.1-dev.202609171440. Deriving
it once here and formatting per ecosystem keeps the one rule in one place -- when this
lived in both ci_build.yml and build_npm.sh the copies drifted, and the older one
crashed on any tag whose patch was not a bare number (v1.37.2rc1).

The tags are two-component (v1.37), so the patch is padded, and a tag may carry a
suffix (v1.30.0-log-debug), so a non-numeric tail is dropped. Off a tag the patch is
bumped and a dev suffix added, so a nightly sorts above the release it was cut from
and below the next one.

Usage: project_version.py --format {pep440,semver} [--dev-suffix SUFFIX]
"""

import argparse
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent


def leading_number(text):
    return int(re.match(r"\d*", text).group() or 0)


def git(*args):
    result = subprocess.run(("git", "-C", str(ROOT_DIR)) + args, capture_output=True, text=True)
    return result.returncode, result.stdout.strip()


def describe_release():
    status, tag = git("describe", "--tags", "--match", "v*", "--first-parent", "--abbrev=0")
    if status != 0 or not tag:
        raise SystemExit("project_version: no v* tag is reachable from HEAD")

    fields = tag.lstrip("v").split(".")
    major = leading_number(fields[0])
    minor = leading_number(fields[1]) if len(fields) > 1 else 0
    patch = leading_number(fields[2]) if len(fields) > 2 else 0

    exact, _ = git("describe", "--exact-match", "--tags", "--match", "v*", "HEAD")

    return major, minor, patch, exact == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--format", choices=("pep440", "semver"), required=True)
    parser.add_argument("--dev-suffix", default="",
                        help="dev build identifier; defaults to the current UTC minute")
    args = parser.parse_args()

    major, minor, patch, on_tag = describe_release()

    if on_tag:
        print(f"{major}.{minor}.{patch}")
        return

    suffix = args.dev_suffix or datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
    separator = ".dev" if args.format == "pep440" else "-dev."

    print(f"{major}.{minor}.{patch + 1}{separator}{suffix}")


if __name__ == "__main__":
    sys.exit(main())
