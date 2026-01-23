"""Output formatters for the code review system."""

import json

from .models import Violation


def format_violation(v: Violation, fmt: str) -> str:
    """Format a violation for output."""
    if fmt == "github":
        # GitHub Actions workflow command (creates annotations)
        return f"::{v.severity} file={v.filepath},line={v.line}::[{v.rule}] {v.message}"
    elif fmt == "json":
        # JSON format for programmatic processing
        return json.dumps(v.to_dict())
    else:
        # Plain text
        return f"{v.filepath}:{v.line}: {v.severity}: [{v.rule}] {v.message}"


def output_json(violations: list[Violation]) -> str:
    """Output all violations as a JSON array."""
    return json.dumps([v.to_dict() for v in violations], indent=2)
