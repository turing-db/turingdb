"""Check for code that is too dense and needs blank lines."""

from . import register_check
from .base import BaseCheck, CheckContext
from ..constants import DENSITY_THRESHOLD


@register_check
class CodeDensityCheck(BaseCheck):
    """Check for code that is too dense and needs blank lines for breathing."""

    name = "Code Breathing"
    enabled_by_default = False
    severity = "warning"

    def run(self, context: CheckContext) -> None:
        # Track consecutive non-blank lines inside function bodies
        consecutive = 0
        start_line = 0
        brace_depth = 0
        in_function = False

        for i, line in enumerate(context.lines, start=1):
            stripped = line.strip()

            # Track brace depth to detect function bodies
            open_braces = stripped.count("{")
            close_braces = stripped.count("}")

            if open_braces > 0 and brace_depth == 0:
                in_function = True

            brace_depth += open_braces - close_braces

            if brace_depth <= 0:
                # Function ended - check density before resetting
                if consecutive >= DENSITY_THRESHOLD:
                    self.add_violation(
                        start_line,
                        f"Dense code block ({consecutive} lines without blank line) - "
                        "consider adding blank lines to separate logical sections",
                    )
                in_function = False
                consecutive = 0
                continue

            # Only check inside function bodies
            if not in_function:
                continue

            # Skip pure comment lines and preprocessor from density count
            if (
                stripped.startswith("//")
                or stripped.startswith("/*")
                or stripped.startswith("#")
            ):
                continue

            if stripped == "":
                if consecutive >= DENSITY_THRESHOLD:
                    self.add_violation(
                        start_line,
                        f"Dense code block ({consecutive} lines without blank line) - "
                        "consider adding blank lines to separate logical sections",
                    )
                consecutive = 0
            else:
                if consecutive == 0:
                    start_line = i
                consecutive += 1

        # Check at end
        if consecutive >= DENSITY_THRESHOLD:
            self.add_violation(
                start_line,
                f"Dense code block ({consecutive} lines without blank line) - "
                "consider adding blank lines to separate logical sections",
            )
