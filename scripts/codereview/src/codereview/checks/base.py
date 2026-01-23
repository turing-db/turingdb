"""Base class for all style checks."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from ..models import Violation


@dataclass
class CheckContext:
    """Shared context passed to all checks."""

    filepath: str
    lines: list[str]
    is_header: bool

    @property
    def filename(self) -> str:
        """Get the filename without path."""
        return Path(self.filepath).name

    @property
    def stem(self) -> str:
        """Get the filename without extension."""
        return Path(self.filepath).stem

    @property
    def path(self) -> Path:
        """Get the path as a Path object."""
        return Path(self.filepath)


class BaseCheck(ABC):
    """Abstract base class for all style checks.

    Subclasses must implement:
    - name: str - The check name used in violation reports
    - run(context) - The main check logic

    Optionally override:
    - enabled_by_default: bool - Whether check runs by default (default: True)
    - severity: str - Default severity level ("error" or "warning")
    """

    name: str = "UnnamedCheck"
    enabled_by_default: bool = True
    severity: str = "error"  # default severity

    def __init__(self):
        self._violations: list[Violation] = []
        self._context: CheckContext | None = None

    def add_violation(
        self, line: int, message: str, severity: str | None = None, rule: str | None = None
    ):
        """Add a violation at the specified line.

        Args:
            line: Line number (1-indexed)
            message: Description of the violation
            severity: Override the default severity ("error" or "warning")
            rule: Override the default rule name
        """
        self._violations.append(
            Violation(
                filepath=self._context.filepath,
                line=line,
                severity=severity or self.severity,
                rule=rule or self.name,
                message=message,
            )
        )

    def check(self, context: CheckContext) -> list[Violation]:
        """Run the check and return violations.

        This method sets up the context and calls the subclass's run() method.
        """
        self._violations = []
        self._context = context
        self.run(context)
        return self._violations

    @abstractmethod
    def run(self, context: CheckContext) -> None:
        """Execute the check logic.

        Use self.add_violation() to report issues.
        Access file content via context.lines.
        """
        pass
