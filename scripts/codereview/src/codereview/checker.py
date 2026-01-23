"""Main style checker orchestrator."""

import sys
from pathlib import Path

from .models import Violation
from .constants import SKIP_DIRS, SKIP_FILES, CPP_EXTENSIONS
from .checks import get_enabled_checks
from .checks.base import CheckContext


class StyleChecker:
    """Checks a single C++ file for style violations."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.lines: list[str] = []
        self._is_header = Path(filepath).suffix in {".h", ".hpp"}

    def load(self) -> bool:
        """Load file contents. Returns False if file should be skipped."""
        path = Path(self.filepath)

        # Check if in skip directory
        for part in path.parts:
            if part in SKIP_DIRS:
                return False

        # Check if file should be skipped
        if path.name in SKIP_FILES:
            return False

        # Skip unit test files (*Test.cpp, *Test.h)
        if path.stem.endswith("Test"):
            return False

        # Check extension
        if path.suffix not in CPP_EXTENSIONS:
            return False

        try:
            with open(self.filepath, "r", encoding="utf-8", errors="replace") as f:
                self.lines = f.read().splitlines()
            return True
        except (IOError, OSError) as e:
            print(f"Warning: Could not read {self.filepath}: {e}", file=sys.stderr)
            return False

    def check_all(
        self,
        skip_checks: set[str] | None = None,
        only_checks: set[str] | None = None,
    ) -> list[Violation]:
        """Run all enabled checks and return violations.

        Args:
            skip_checks: Set of check names to skip
            only_checks: If provided, only run these checks
        """
        context = CheckContext(
            filepath=self.filepath,
            lines=self.lines,
            is_header=self._is_header,
        )

        violations = []
        check_classes = get_enabled_checks(skip_checks, only_checks)

        for check_cls in check_classes:
            check = check_cls()
            violations.extend(check.check(context))

        return violations
