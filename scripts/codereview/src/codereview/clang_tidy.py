"""clang-tidy integration for higher quality const analysis."""

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path

from .models import Violation


class ClangTidyConstChecker:
    """Runs clang-tidy's misc-const-correctness check for higher quality const analysis."""

    CLANG_TIDY_VERSIONS = ["clang-tidy-18", "clang-tidy-17", "clang-tidy-16", "clang-tidy"]

    def __init__(self, compile_commands_dir: str = "build"):
        self.compile_commands_dir = compile_commands_dir
        self.compile_commands: dict[str, dict] = {}
        self.clang_tidy_path: str | None = None
        self._load_compile_commands()
        self._find_clang_tidy()

    def _load_compile_commands(self):
        """Load the compile_commands.json database."""
        cc_path = Path(self.compile_commands_dir) / "compile_commands.json"
        if not cc_path.exists():
            return

        try:
            with open(cc_path) as f:
                cmds = json.load(f)
            for c in cmds:
                self.compile_commands[c["file"]] = c
        except (IOError, json.JSONDecodeError) as e:
            print(f"Warning: Could not load compile_commands.json: {e}", file=sys.stderr)

    def _find_clang_tidy(self):
        """Find a suitable clang-tidy version with misc-const-correctness support."""
        for version in self.CLANG_TIDY_VERSIONS:
            path = shutil.which(version)
            if path:
                # Check if this version has misc-const-correctness
                try:
                    result = subprocess.run(
                        [path, "--list-checks", "-checks=*"],
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    if "misc-const-correctness" in result.stdout:
                        self.clang_tidy_path = path
                        return
                except (subprocess.TimeoutExpired, subprocess.SubprocessError):
                    continue

    def is_available(self) -> bool:
        """Check if clang-tidy with const-correctness check is available."""
        return self.clang_tidy_path is not None and len(self.compile_commands) > 0

    def _get_compile_flags(self, filepath: str) -> list[str]:
        """Extract compile flags from the compile_commands.json for a file."""
        abs_path = str(Path(filepath).resolve())
        if abs_path not in self.compile_commands:
            return []

        cmd = self.compile_commands[abs_path]["command"]
        flags = []
        parts = cmd.split()
        i = 0
        while i < len(parts):
            p = parts[i]
            if p.startswith("-I"):
                flags.append(p)
            elif p == "-I" and i + 1 < len(parts):
                flags.append("-I" + parts[i + 1])
                i += 1
            elif p.startswith("-D"):
                flags.append(p)
            elif p == "-D" and i + 1 < len(parts):
                flags.append("-D" + parts[i + 1])
                i += 1
            elif p.startswith("-std="):
                flags.append(p)
            i += 1

        # Ensure we have a C++ standard
        if not any(f.startswith("-std=") for f in flags):
            flags.append("-std=c++23")

        return flags

    def check_file(self, filepath: str) -> list[Violation]:
        """Run clang-tidy const-correctness check on a single file."""
        if not self.is_available():
            return []

        abs_path = str(Path(filepath).resolve())
        if abs_path not in self.compile_commands:
            return []

        flags = self._get_compile_flags(filepath)
        if not flags:
            return []

        try:
            result = subprocess.run(
                [
                    self.clang_tidy_path,
                    "--checks=-*,misc-const-correctness",
                    abs_path,
                    "--",
                ]
                + flags,
                capture_output=True,
                text=True,
                timeout=120,
            )
        except (subprocess.TimeoutExpired, subprocess.SubprocessError) as e:
            print(f"Warning: clang-tidy failed on {filepath}: {e}", file=sys.stderr)
            return []

        return self._parse_output(result.stdout, filepath)

    def _parse_output(self, output: str, original_filepath: str) -> list[Violation]:
        """Parse clang-tidy output and return violations."""
        violations = []
        # Pattern: /path/file.cpp:123:5: warning: ... [misc-const-correctness]
        pattern = re.compile(
            r"^(.+?):(\d+):\d+: warning: (.+?) \[misc-const-correctness\]$"
        )

        for line in output.split("\n"):
            match = pattern.match(line)
            if match:
                file_path = match.group(1)
                line_num = int(match.group(2))
                message = match.group(3)

                # Only include violations from the requested file (not headers)
                abs_original = str(Path(original_filepath).resolve())
                if file_path == abs_original:
                    violations.append(
                        Violation(
                            filepath=original_filepath,
                            line=line_num,
                            severity="warning",
                            rule="Const",
                            message=message,
                        )
                    )

        return violations

    def check_files(self, filepaths: list[str]) -> list[Violation]:
        """Run clang-tidy const-correctness check on multiple files."""
        all_violations = []
        for filepath in filepaths:
            all_violations.extend(self.check_file(filepath))
        return all_violations
