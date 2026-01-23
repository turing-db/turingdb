"""Check that pointer class members are initialized with {nullptr}."""

import re

from . import register_check
from .base import BaseCheck, CheckContext


@register_check
class PointerMemberInitCheck(BaseCheck):
    """Check that pointer class members are initialized with {nullptr}.

    Rule: All pointer members should be default-initialized to nullptr
    using brace initialization: MyType* _member {nullptr};
    """

    name = "Initialization"
    severity = "error"

    # Smart pointer types that don't need nullptr initialization
    SMART_POINTERS = {"unique_ptr", "shared_ptr", "weak_ptr", "auto_ptr"}

    def run(self, context: CheckContext) -> None:
        # Only check header files (class definitions are in headers)
        if not context.is_header:
            return

        # Track if we're inside a class/struct body (not method body)
        in_class_body = False
        class_brace_depth = 0
        brace_depth = 0

        # Pattern to match raw pointer member declarations
        # Matches: Type* name; or Type* name = ...; or Type * name;
        # Captures: (1) type name, (2) member name, (3) ending char
        pointer_member = re.compile(
            r"^\s*"
            r"(?:const\s+)?"  # optional const
            r"(\w+(?:::\w+)?)"  # type name (possibly with namespace, but not templates)
            r"\s*\*+\s*"  # pointer asterisk(s)
            r"(_?\w+)"  # member name (possibly with underscore prefix)
            r"\s*"
            r"(;|=|{)"  # ends with ; or = or {
        )

        for i, line in enumerate(context.lines, start=1):
            stripped = line.strip()

            # Skip empty lines, comments, preprocessor
            if not stripped or stripped.startswith("//") or stripped.startswith("#"):
                continue

            # Track class/struct definitions
            class_match = re.match(r"^(class|struct)\s+(\w+)", stripped)
            if class_match and ";" not in stripped:  # Not a forward declaration
                if "{" in stripped:
                    in_class_body = True
                    class_brace_depth = brace_depth + 1
                    brace_depth += stripped.count("{") - stripped.count("}")
                else:
                    # Opening brace might be on next line
                    in_class_body = True
                    class_brace_depth = brace_depth + 1
                continue

            # Update brace depth
            old_depth = brace_depth
            brace_depth += stripped.count("{") - stripped.count("}")

            # Track when we exit the class
            if in_class_body and brace_depth < class_brace_depth:
                in_class_body = False
                continue

            # Only check at class body level (depth == class_brace_depth)
            # This excludes method bodies which are deeper
            if not in_class_body or brace_depth != class_brace_depth:
                continue

            # Skip access specifiers
            if stripped in ("public:", "private:", "protected:"):
                continue

            # Skip lines with parentheses (method declarations, function pointers, default params)
            if "(" in stripped or stripped.endswith(")"):
                continue

            # Skip lines with return, if, for, etc (shouldn't be here but safety check)
            if re.match(r"^(return|if|for|while|switch)\b", stripped):
                continue

            # Skip smart pointers (unique_ptr, shared_ptr, etc.)
            if any(sp in stripped for sp in self.SMART_POINTERS):
                continue

            # Skip template instantiations that look like pointers but aren't
            if "<" in stripped and ">" in stripped:
                continue

            # Check for pointer member declarations
            match = pointer_member.match(stripped)
            if match:
                type_name = match.group(1)
                member_name = match.group(2)
                ending = match.group(3)

                # Check the initialization style
                if ending == ";":
                    # No initialization at all - error
                    self.add_violation(
                        i,
                        f"Pointer member '{member_name}' must be initialized with {{nullptr}}",
                    )
                elif ending == "=":
                    # Using = style instead of {} - error
                    self.add_violation(
                        i,
                        f"Pointer member '{member_name}' should use {{nullptr}} "
                        "initialization, not = style",
                    )
                elif ending == "{":
                    # Using {} style - check if it's {nullptr}
                    if "{nullptr}" not in stripped and "{ nullptr }" not in stripped:
                        # Could be initialized to something else, or wrong format
                        if re.search(r"\{\s*\}", stripped):
                            # Empty braces {} - should be {nullptr}
                            self.add_violation(
                                i,
                                f"Pointer member '{member_name}' should be initialized "
                                "with {{nullptr}}, not {{}}",
                            )
