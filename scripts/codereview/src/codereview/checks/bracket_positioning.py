"""Check bracket positioning for control flow and functions."""

import re

from . import register_check
from .base import BaseCheck, CheckContext


@register_check
class BracketPositioningCheck(BaseCheck):
    """Check bracket positioning for control flow and functions."""

    name = "Brackets"
    severity = "error"

    def run(self, context: CheckContext) -> None:
        # Control flow keywords that need { on same line
        control_flow = re.compile(
            r"^\s*(if|for|while|else|else\s+if|switch|do)\s*"
            r"(\([^)]*\))?\s*$"  # Optional condition, no bracket
        )

        # Function/method definition without opening bracket
        # Match: return_type ClassName::methodName(...) optional_const
        func_def = re.compile(
            r"^\s*(?:virtual\s+)?(?:static\s+)?(?:const\s+)?"
            r"(?:\w+(?:<[^>]+>)?(?:\*|&)?\s+)+"  # Return type
            r"(?:\w+::)?\w+\s*"  # Optional class prefix and method name
            r"\([^)]*\)\s*"  # Parameters
            r"(?:const\s*)?(?:override\s*)?(?:final\s*)?"
            r"$"  # No bracket at end
        )

        for i, line in enumerate(context.lines, start=1):
            stripped = line.strip()

            # Skip empty lines, comments, preprocessor
            if not stripped or stripped.startswith("//") or stripped.startswith("#"):
                continue

            # Skip lines that already have { or are just }
            if "{" in stripped or stripped == "}":
                continue

            # Check control flow
            if control_flow.match(stripped):
                # Verify next non-empty line starts with {
                for j in range(i, min(i + 3, len(context.lines))):
                    next_line = context.lines[j].strip()
                    if next_line == "":
                        continue
                    if next_line.startswith("{"):
                        self.add_violation(
                            i,
                            "Opening bracket '{' must be on the same line for control flow",
                        )
                    break

            # Check function definitions (but not declarations in headers)
            # Only flag if this looks like a definition (has ClassName::)
            if func_def.match(stripped) and "::" in stripped:
                # This is a method definition (has ClassName::)
                for j in range(i, min(i + 3, len(context.lines))):
                    next_line = context.lines[j].strip()
                    if next_line == "":
                        continue
                    if next_line.startswith("{"):
                        self.add_violation(
                            i,
                            "Function opening bracket '{' must be on the same line",
                        )
                    break
