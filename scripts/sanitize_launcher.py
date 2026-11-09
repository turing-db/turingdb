#!/usr/bin/env python3
"""Compiler launcher of the GCC sanitizer build.

GCC's sanitizer instrumentation raises false warnings, so the sanitizer build demotes
them with -Wno-error=<warning>. This launcher runs the compile, passes its output
through, and fails it on any diagnostic of a warning the command demotes that no entry
of the exclusion file matches. An entry reads

  <warning> <header>:<line> <object>

<header> matches a suffix of the diagnostic's path. <object> matches the whole object
the warning names, * standing for any run of characters.

Usage:
  sanitize_launcher.py EXCLUSIONS COMMAND...
"""

import os
import re
import subprocess
import sys

DIAGNOSTIC = re.compile(
    r"^(?P<path>[^:]+):(?P<line>\d+):\d+: (?:warning|error): (?P<message>.*) \[-W(?:error=)?(?P<warning>[\w-]+)\]$")
QUOTED_OBJECT = re.compile(r"^[‘'](?P<object>.*)[’'] ")
COLOR = re.compile(r"\x1b\[[0-9;]*[mK]")


def compile_pattern(pattern):
    return re.compile(".*".join(re.escape(part) for part in pattern.split("*")), re.DOTALL)


def read_exclusions(path):
    exclusions = []
    with open(path, encoding="utf-8") as file:
        for number, text in enumerate(file, start=1):
            text = text.strip()
            if not text or text.startswith("#"):
                continue

            fields = text.split(maxsplit=2)
            if len(fields) != 3 or ":" not in fields[1]:
                sys.exit(f"{path}:{number}: expected '<warning> <header>:<line> <object>'")

            warning, location, pattern = fields
            header, line = location.rsplit(":", 1)
            exclusions.append((warning, header, line, compile_pattern(pattern)))

    return exclusions


def is_excluded(exclusions, warning, path, line, quoted_object):
    for excluded_warning, header, excluded_line, pattern in exclusions:
        same_location = excluded_line == line and (path == header or path.endswith("/" + header))
        if excluded_warning == warning and same_location and pattern.fullmatch(quoted_object):
            return True

    return False


def find_unexcluded(exclusions, demoted, stderr):
    unexcluded = []
    for text in stderr.splitlines():
        diagnostic = DIAGNOSTIC.match(COLOR.sub("", text))
        if not diagnostic or diagnostic["warning"] not in demoted:
            continue

        quoted = QUOTED_OBJECT.match(diagnostic["message"])
        quoted_object = quoted["object"] if quoted else diagnostic["message"]
        if not is_excluded(exclusions, diagnostic["warning"], diagnostic["path"], diagnostic["line"], quoted_object):
            unexcluded.append(f"{diagnostic['path']}:{diagnostic['line']}: [-W{diagnostic['warning']}] {quoted_object}")

    return unexcluded


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)

    exclusions_path = sys.argv[1]
    command = sys.argv[2:]
    exclusions = read_exclusions(exclusions_path)
    demoted = {argument.removeprefix("-Wno-error=") for argument in command if argument.startswith("-Wno-error=")}

    result = subprocess.run(command, stderr=subprocess.PIPE)
    stderr = result.stderr.decode("utf-8", errors="replace")
    sys.stderr.write(stderr)
    if result.returncode != 0:
        return result.returncode

    unexcluded = find_unexcluded(exclusions, demoted, stderr)
    if not unexcluded:
        return 0

    for diagnostic in unexcluded:
        sys.stderr.write(f"error: no entry of {exclusions_path} matches {diagnostic}\n")

    if "-o" in command:
        output = command[command.index("-o") + 1]
        if os.path.exists(output):
            os.remove(output)

    return 1


if __name__ == "__main__":
    sys.exit(main())
