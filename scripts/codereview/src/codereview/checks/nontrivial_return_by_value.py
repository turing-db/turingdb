"""Check that non-trivial classes are not returned by value."""

import re

from . import register_check
from .base import BaseCheck, CheckContext
from ..constants import STL_CONTAINERS, RESULT_TYPE_PATTERNS


@register_check
class NontrivialReturnByValueCheck(BaseCheck):
    """Check that non-trivial classes are not returned by value.

    Non-trivial classes are those containing STL data structure members.
    Result types (classes designed to be returned) are excluded.
    """

    name = "Return Type (Non-trivial)"
    severity = "error"

    def run(self, context: CheckContext) -> None:
        # Only check header files where class definitions live
        if not context.is_header:
            return

        # First pass: find classes with STL members
        nontrivial_classes: dict[str, int] = {}  # class name -> line defined
        current_class = None
        class_start_line = 0
        brace_depth = 0
        class_brace_depth = 0

        for i, line in enumerate(context.lines, start=1):
            stripped = line.strip()

            # Skip comments and preprocessor
            if not stripped or stripped.startswith("//") or stripped.startswith("#"):
                continue

            # Detect class/struct definition
            class_match = re.match(r"^(class|struct)\s+(\w+)", stripped)
            if class_match and ";" not in stripped:  # Not forward declaration
                class_name = class_match.group(2)

                # Skip result types
                is_result_type = any(
                    pattern in class_name for pattern in RESULT_TYPE_PATTERNS
                )
                if is_result_type:
                    continue

                current_class = class_name
                class_start_line = i
                if "{" in stripped:
                    class_brace_depth = brace_depth + 1
                    brace_depth += stripped.count("{") - stripped.count("}")
                else:
                    class_brace_depth = brace_depth + 1
                continue

            # Update brace depth
            brace_depth += stripped.count("{") - stripped.count("}")

            # Check if we exited the class
            if current_class and brace_depth < class_brace_depth:
                current_class = None
                continue

            # If inside a class, check for STL members
            if current_class and brace_depth == class_brace_depth:
                # Check if this line declares an STL container member
                for container in STL_CONTAINERS:
                    # Match std::vector<...> or vector<...> member declarations
                    pattern = rf"(?:std::)?{container}\s*<"
                    if re.search(pattern, stripped):
                        # This class has an STL member - it's non-trivial
                        if current_class not in nontrivial_classes:
                            nontrivial_classes[current_class] = class_start_line
                        break

        # Second pass: check for functions returning non-trivial classes by value
        for i, line in enumerate(context.lines, start=1):
            stripped = line.strip()

            # Skip comments and preprocessor
            if not stripped or stripped.startswith("//") or stripped.startswith("#"):
                continue

            # Look for function declarations returning a known non-trivial class
            for class_name in nontrivial_classes:
                # Match: ClassName funcName( but not ClassName& or ClassName*
                # Also handle const ClassName
                pattern = rf"^(?:(?:static|virtual|inline|const|constexpr)\s+)*(?:const\s+)?({class_name})\s+(\w+)\s*\("

                match = re.match(pattern, stripped)
                if match:
                    returned_type = match.group(1)
                    func_name = match.group(2)

                    # Skip constructors and destructors
                    if func_name == class_name or func_name.startswith("~"):
                        continue

                    # Skip iterator functions (begin, end, etc.) - standard pattern
                    if func_name in ("begin", "end", "cbegin", "cend", "rbegin", "rend"):
                        continue

                    # Check it's not returning by reference or pointer
                    type_end = stripped.find(returned_type) + len(returned_type)
                    between = stripped[type_end : stripped.find(func_name)]

                    if "&" not in between and "*" not in between:
                        self.add_violation(
                            i,
                            f"Function '{func_name}' returns non-trivial class "
                            f"'{class_name}' by value. Use a pointer or output parameter.",
                            rule="Return Type",
                        )
