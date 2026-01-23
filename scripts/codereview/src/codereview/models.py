"""Data models for the code review system."""

from dataclasses import dataclass, asdict


@dataclass
class Violation:
    """Represents a style violation found in code."""

    filepath: str
    line: int
    severity: str  # "error" or "warning"
    rule: str
    message: str

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return asdict(self)
