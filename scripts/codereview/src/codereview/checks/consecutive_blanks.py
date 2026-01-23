"""Check for consecutive blank lines."""

from . import register_check
from .base import BaseCheck, CheckContext


@register_check
class ConsecutiveBlanksCheck(BaseCheck):
    """Check for 2+ consecutive blank lines."""

    name = "Formatting"
    severity = "error"

    def run(self, context: CheckContext) -> None:
        blank_count = 0
        blank_start = 0
        # Track recent lines to detect macro blocks
        recent_nonblank_lines: list[str] = []

        for i, line in enumerate(context.lines, start=1):
            if line.strip() == "":
                if blank_count == 0:
                    blank_start = i
                blank_count += 1
            else:
                if blank_count >= 2:
                    # Skip if we're after a macro block
                    # A macro block has lines ending with \ (continuation)
                    # Check if any of the recent lines were part of a macro
                    is_after_macro = any(
                        ln.rstrip().endswith("\\") for ln in recent_nonblank_lines
                    )
                    if not is_after_macro:
                        self.add_violation(
                            blank_start,
                            f"Found {blank_count} consecutive blank lines (max 1 allowed)",
                        )
                blank_count = 0
                # Keep track of recent non-blank lines (last 10)
                recent_nonblank_lines.append(line)
                if len(recent_nonblank_lines) > 10:
                    recent_nonblank_lines.pop(0)

        # Check at end of file
        if blank_count >= 2:
            self.add_violation(
                blank_start,
                f"Found {blank_count} consecutive blank lines at end of file",
            )
