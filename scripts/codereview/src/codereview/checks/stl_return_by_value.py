"""Check that functions don't return STL containers by value."""

import re

from . import register_check
from .base import BaseCheck, CheckContext
from ..constants import STL_CONTAINERS


@register_check
class STLReturnByValueCheck(BaseCheck):
    """Check that functions don't return STL containers by value.

    Rule: Do not return STL containers or strings by value.
    Use output parameters (reference or pointer) instead.
    """

    name = "Return Type (STL)"
    severity = "error"

    # Debug/logging methods that commonly return strings
    DEBUG_METHODS = {
        "describe",
        "toString",
        "to_string",
        "str",
        "dump",
        "format",
        "repr",
        "debug",
        "print",
        "show",
    }

    def run(self, context: CheckContext) -> None:
        # Pattern to match function declarations/definitions returning STL types
        # Matches: std::vector<...> funcName(  or  vector<...> funcName(
        # Also matches with const, static, virtual, inline prefixes
        func_pattern = re.compile(
            r"^\s*"
            r"(?:(?:static|virtual|inline|const|constexpr)\s+)*"  # optional prefixes
            r"(?:const\s+)?"  # optional const before type
            r"(?:std::)?"  # optional std::
            r"(\w+)"  # container name (captured)
            r"\s*<"  # template bracket
            r"[^>]+"  # template arguments
            r">\s*"
            r"(?:&{1,2})?"  # optional reference (& or &&) - these are OK
            r"\s*"
            r"(?:\w+::)*"  # optional class prefix like MyClass::
            r"(\w+)"  # function name (captured)
            r"\s*\("  # opening paren
        )

        # Also check for string returns (no template)
        string_pattern = re.compile(
            r"^\s*"
            r"(?:(?:static|virtual|inline|const|constexpr)\s+)*"
            r"(?:const\s+)?"
            r"(?:std::)?"
            r"(string|wstring)"  # string types
            r"\s+"
            r"(?:\w+::)*"
            r"(\w+)"  # function name
            r"\s*\("
        )

        # Track brace depth to distinguish function declarations from local variables
        # Function declarations appear at depth 0 (global) or 1 (class body)
        # Local variables appear at depth >= 2 (inside method bodies in headers)
        # or depth >= 1 (inside function bodies in .cpp files)
        brace_depth = 0
        in_class = False
        class_depth = 0

        for i, line in enumerate(context.lines, start=1):
            stripped = line.strip()

            # Track class/struct definitions
            if re.match(r"^(class|struct)\s+\w+", stripped) and ";" not in stripped:
                in_class = True
                if "{" in stripped:
                    class_depth = brace_depth + 1
                    brace_depth += stripped.count("{") - stripped.count("}")
                else:
                    class_depth = brace_depth + 1
                continue

            # Update brace depth
            old_depth = brace_depth
            brace_depth += stripped.count("{") - stripped.count("}")

            # Track when we exit a class
            if in_class and brace_depth < class_depth:
                in_class = False

            # Skip empty lines, comments, preprocessor
            if not stripped or stripped.startswith("//") or stripped.startswith("#"):
                continue

            # Skip lines that are clearly not function declarations
            if stripped.startswith("return ") or stripped.startswith("if "):
                continue

            # Only check at declaration scope (not inside function bodies)
            # In headers: depth 0 (global) or depth == class_depth (class body)
            # In .cpp files: depth 0 (global)
            if context.is_header:
                if brace_depth > 0 and (not in_class or brace_depth > class_depth):
                    continue  # Inside a method body
            else:
                if brace_depth > 0:
                    continue  # Inside a function body

            # Check for STL container return
            match = func_pattern.match(stripped)
            if match:
                container = match.group(1)
                func_name = match.group(2)

                # Skip debug/logging methods
                if func_name in self.DEBUG_METHODS:
                    continue

                # Check if it's an STL container
                if container in STL_CONTAINERS:
                    # Check if it's returning by reference (OK) or by value (bad)
                    # Look for & after the > but before the function name
                    after_template = stripped[stripped.find(">") + 1 :]
                    before_func = after_template[: after_template.find(func_name)]

                    if "&" not in before_func:
                        self.add_violation(
                            i,
                            f"Function '{func_name}' returns std::{container} by value. "
                            "Use an output parameter instead.",
                            rule="Return Type",
                        )

            # Check for string return
            match = string_pattern.match(stripped)
            if match:
                string_type = match.group(1)
                func_name = match.group(2)

                # Skip debug/logging methods
                if func_name in self.DEBUG_METHODS:
                    continue

                # Check it's not returning by reference
                type_end = stripped.find(string_type) + len(string_type)
                after_type = stripped[type_end : stripped.find(func_name)]

                if "&" not in after_type:
                    self.add_violation(
                        i,
                        f"Function '{func_name}' returns std::{string_type} by value. "
                        "Use an output parameter instead.",
                        rule="Return Type",
                    )
