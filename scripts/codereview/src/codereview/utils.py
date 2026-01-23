"""Utility functions for the code review system."""

import subprocess
import sys
from pathlib import Path

from .constants import CPP_EXTENSIONS, SKIP_DIRS


def get_changed_files(base_ref: str) -> list[str]:
    """Get list of changed C++ files compared to base ref."""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "--diff-filter=ACMR", base_ref],
            capture_output=True,
            text=True,
            check=True,
        )
        files = result.stdout.strip().split("\n")
        # Filter to C++ files only
        return [f for f in files if f and Path(f).suffix in CPP_EXTENSIONS]
    except subprocess.CalledProcessError as e:
        print(f"Error getting changed files: {e}", file=sys.stderr)
        sys.exit(1)


def get_all_cpp_files(root: Path = Path(".")) -> list[str]:
    """Get all C++ files in the codebase, excluding skipped directories."""
    files = []
    for ext in CPP_EXTENSIONS:
        for filepath in root.rglob(f"*{ext}"):
            # Check if in skip directory
            skip = False
            for part in filepath.parts:
                if part in SKIP_DIRS:
                    skip = True
                    break
            if not skip:
                files.append(str(filepath))
    return sorted(files)
