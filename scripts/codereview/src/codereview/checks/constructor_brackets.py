"""Check that constructors have opening bracket on the next line."""

import re

from . import register_check
from .base import BaseCheck, CheckContext


@register_check
class ConstructorBracketsCheck(BaseCheck):
    """Check that constructors have opening bracket on the next line.

    Per CODING_STYLE.md: Constructors are the only place where the opening
    bracket must be on the next line (applies to all constructors).
    """

    name = "Constructors"
    severity = "error"

    def run(self, context: CheckContext) -> None:
        # Regex to match start of constructor: ClassName::ClassName(
        constructor_start = re.compile(r"^(\w+)::(\1)\s*\(")

        i = 0
        lines = context.lines
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            i += 1

            # Check if this line starts a constructor
            match = constructor_start.match(stripped)
            if not match:
                continue

            constructor_line = i

            # Track parentheses to find the end of parameters
            paren_depth = stripped.count("(") - stripped.count(")")

            # Find the line where parameters end (paren_depth reaches 0)
            # Also collect all the lines to check for = default/delete
            constructor_text = stripped
            while paren_depth > 0 and i < len(lines):
                next_line = lines[i].strip()
                constructor_text += " " + next_line
                paren_depth += next_line.count("(") - next_line.count(")")
                i += 1

            # Skip defaulted or deleted constructors (no body to check)
            if "= default" in constructor_text or "= delete" in constructor_text:
                continue

            # Now find the constructor body opener - should be a line that is just "{"
            found_body_opener = False
            for j in range(i - 1, min(i + 10, len(lines))):
                check_line = lines[j].strip()

                # Skip empty lines
                if check_line == "":
                    continue

                # The constructor body should start with a line that is just "{"
                if check_line == "{":
                    found_body_opener = True
                    break

                # If we find a line ending with { that's not a standalone {
                # it's a violation (constructor body opener should be on its own line)
                if check_line.endswith("{"):
                    # If line has } it's brace initialization, not body opener
                    if "}" in check_line:
                        continue

                    # If line has content before { (not just whitespace), it's a violation
                    before_brace = check_line[:-1].rstrip()
                    if before_brace:
                        self.add_violation(
                            j + 1,
                            "Constructor opening bracket '{' must be on its own line",
                        )
                    found_body_opener = True
                    break

            if found_body_opener:
                i = j + 1
