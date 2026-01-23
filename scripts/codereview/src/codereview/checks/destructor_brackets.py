"""Check that destructors have opening bracket on the same line."""

import re

from . import register_check
from .base import BaseCheck, CheckContext


@register_check
class DestructorBracketsCheck(BaseCheck):
    """Check that destructors have opening bracket on the same line."""

    name = "Destructors"
    severity = "error"

    def run(self, context: CheckContext) -> None:
        # Pattern: ~ClassName() with optional whitespace
        destructor_start = re.compile(r"^(\w+)::~\1\s*\(\s*\)\s*$")

        for i, line in enumerate(context.lines, start=1):
            stripped = line.strip()
            match = destructor_start.match(stripped)

            if match:
                # Check if next non-empty line starts with {
                for j in range(i, min(i + 3, len(context.lines))):
                    next_line = context.lines[j].strip()
                    if next_line == "":
                        continue
                    if next_line.startswith("{"):
                        self.add_violation(
                            i,
                            "Destructor opening bracket '{' must be on the same line",
                        )
                    break
