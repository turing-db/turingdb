#!/usr/bin/env python3
"""
TuringDB Code Style Reviewer

DEPRECATED: This script is a shim for backward compatibility.
Use 'uv run --project scripts/codereview codereview' instead.

A static code style checker for C++ files based on CODING_STYLE.md.
Designed to run in GitHub Actions without requiring AI model calls.

Usage:
    python code_review.py file1.cpp file2.h           # Check specific files
    python code_review.py --diff origin/main          # Check changed files vs base
    python code_review.py --format github             # GitHub Actions annotations
"""

import subprocess
import sys
from pathlib import Path


def main():
    # Get the directory containing this script
    script_dir = Path(__file__).parent

    # Build the uv command
    cmd = [
        "uv",
        "run",
        "--project",
        str(script_dir / "codereview"),
        "codereview",
    ] + sys.argv[1:]

    # Run the new codereview package
    result = subprocess.run(cmd, cwd=script_dir.parent)
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
