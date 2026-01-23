"""Check for missing const in common cases."""

import re

from . import register_check
from .base import BaseCheck, CheckContext
from ..constants import STL_CONTAINERS


@register_check
class MissingConstCheck(BaseCheck):
    """Check for missing const in common cases.

    Detects:
    1. Non-const reference parameters for STL containers (likely should be const)
    2. Local variables that are never modified after initialization
    """

    name = "Const"
    severity = "warning"

    def run(self, context: CheckContext) -> None:
        self._check_nonconst_reference_params(context)
        if not context.is_header:
            self._check_nonconst_local_variables(context)

    def _check_nonconst_reference_params(self, context: CheckContext) -> None:
        """Check for non-const reference parameters that should likely be const.

        Focuses on STL container references. Scans function body to check if
        the parameter is actually modified before flagging.
        """
        # Only check .cpp files where we can see function bodies
        if context.is_header:
            return

        # Patterns that indicate modification of a container parameter
        modify_method_pattern = re.compile(
            r"\b(\w+)\s*\.\s*(?:push_back|push_front|pop_back|pop_front|"
            r"emplace|emplace_back|emplace_front|insert|erase|clear|resize|"
            r"reserve|assign|swap|shrink_to_fit)\s*\("
        )
        subscript_assign_pattern = re.compile(r"\b(\w+)\s*\[[^\]]*\]\s*=[^=]")
        direct_assign_pattern = re.compile(r"\b(\w+)\s*=[^=]")

        i = 0
        lines = context.lines
        while i < len(lines):
            line = lines[i]
            stripped = line.strip()
            i += 1

            # Skip comments and preprocessor
            if not stripped or stripped.startswith("//") or stripped.startswith("#"):
                continue

            # Must have parentheses (function signature)
            if "(" not in stripped:
                continue

            # Skip constructors/destructors by checking for ::ClassName( pattern
            if re.search(r"(\w+)::~?\1\s*\(", stripped):
                continue

            # Collect non-const reference params from this function declaration
            nonconst_params: dict[str, tuple[int, str]] = {}  # name -> (line, container)

            for container in STL_CONTAINERS:
                pattern = (
                    rf"(?:std::)?{container}\s*<[^>]+>\s*&\s*"
                    rf"(?!&)"
                    rf"(\w+)"
                )

                for match in re.finditer(pattern, stripped):
                    param_name = match.group(1)

                    # Skip if it looks like an output parameter by name
                    if param_name.lower() in (
                        "result",
                        "output",
                        "out",
                        "ret",
                        "outlist",
                        "results",
                        "outputs",
                        "items",
                        "entries",
                    ):
                        continue

                    # Check if const appears before this parameter
                    if re.search(
                        r"\bconst\s+(?:std::)?" + container, stripped[: match.end()]
                    ):
                        continue

                    nonconst_params[param_name] = (i, container)

            if not nonconst_params:
                continue

            # Find the function body and scan for modifications
            # Look for opening brace
            brace_depth = stripped.count("{") - stripped.count("}")
            func_start = i

            # Scan until we find the opening brace
            while brace_depth <= 0 and i < len(lines):
                line = lines[i]
                brace_depth += line.count("{") - line.count("}")
                i += 1
                if brace_depth > 0:
                    break

            if brace_depth <= 0:
                # No function body found (declaration only)
                continue

            # Scan the function body for modifications
            modified_params: set[str] = set()
            body_start = i

            while brace_depth > 0 and i < len(lines):
                body_line = lines[i]
                brace_depth += body_line.count("{") - body_line.count("}")

                # Check for modifications
                for param_name in nonconst_params:
                    if param_name in modified_params:
                        continue

                    # Check modify methods
                    method_match = modify_method_pattern.search(body_line)
                    if method_match and method_match.group(1) == param_name:
                        modified_params.add(param_name)
                        continue

                    # Check subscript assignment
                    subscript_match = subscript_assign_pattern.search(body_line)
                    if subscript_match and subscript_match.group(1) == param_name:
                        modified_params.add(param_name)
                        continue

                    # Check if passed to non-const reference function
                    # This is a heuristic - if param is passed to a function, assume it might be modified
                    if re.search(rf"\(\s*{param_name}\s*\)", body_line):
                        modified_params.add(param_name)
                        continue
                    if re.search(rf",\s*{param_name}\s*[,)]", body_line):
                        modified_params.add(param_name)
                        continue

                i += 1

            # Report unmodified parameters
            for param_name, (decl_line, container) in nonconst_params.items():
                if param_name not in modified_params:
                    self.add_violation(
                        decl_line,
                        f"Parameter '{param_name}' is a non-const reference to "
                        f"std::{container}. Consider using const& if not modified.",
                    )

    def _check_nonconst_local_variables(self, context: CheckContext) -> None:
        """Check for local variables that could be const.

        Detects variables that are initialized but never assigned to afterwards.
        """
        # Track function bodies
        brace_depth = 0
        in_function = False
        function_start = 0

        # Variables declared in current function: {name: (line, is_modified)}
        local_vars: dict[str, tuple[int, bool]] = {}

        # Pattern for local variable declarations
        # Matches: Type name = value; or Type name(value); or Type name{value};
        var_decl = re.compile(
            r"^\s*"
            r"(?!return|if|else|for|while|switch|case|break|continue|goto|throw|void|virtual|static|inline)"
            r"(?:const\s+)?"
            r"(\w+(?:::\w+)?(?:<[^>]+>)?)"  # type
            r"(?:\s*[*&])?\s+"  # optional pointer/ref
            r"(\w+)"  # variable name
            r"\s*[=({]"  # initialization
        )

        # Pattern for assignments
        assign_pattern = re.compile(r"(\w+)\s*(?:\[.*\])?\s*=[^=]")

        # Pattern for non-const method calls or passing as non-const ref
        modify_patterns = [
            re.compile(
                r"(\w+)\s*\.\s*(?:push|pop|insert|erase|clear|resize|assign|emplace|swap|reset)"
            ),
            re.compile(r"(\w+)\s*\[\s*.*\s*\]\s*="),  # array/map assignment
            re.compile(r"(\w+)\s*\+\+"),  # increment
            re.compile(r"\+\+\s*(\w+)"),  # prefix increment
            re.compile(r"(\w+)\s*--"),  # decrement
            re.compile(r"--\s*(\w+)"),  # prefix decrement
            re.compile(r"(\w+)\s*(?:\+|-|\*|\/|%|&|\||\^)="),  # compound assignment
        ]

        # Pattern for std::move() which prevents const
        move_pattern = re.compile(r"\bstd::move\s*\(\s*(\w+)\s*\)")

        # Pattern for return statements - returned variables can't be const for move-only types
        return_pattern = re.compile(r"\breturn\s+(\w+)\s*;")

        # Pattern for non-const method calls (object.method() or object->method())
        # This catches methods that likely modify state
        nonconst_method_pattern = re.compile(
            r"(\w+)\s*(?:\.|\->)\s*(?:set|add|remove|delete|update|write|commit|submit|"
            r"start|stop|open|close|init|load|save|send|receive|connect|disconnect|"
            r"append|prepend|put|post|execute|run|process|handle|build|create|destroy|"
            r"allocate|free|release|acquire|lock|unlock|notify|signal|wait|join|detach)\w*\s*\("
        )

        # Pattern for passing variable by pointer (address-of)
        # Functions that take &var can modify it
        address_of_pattern = re.compile(r"[,(]\s*&(\w+)\s*[,)]")

        # Pattern for passing variable to a function (could be by non-const reference)
        # Skip analysis for these variables as they might be output parameters
        func_arg_pattern = re.compile(r"\w+\s*\([^)]*\b(\w+)\b[^)]*\)")

        lines = context.lines
        for i, line in enumerate(lines, start=1):
            stripped = line.strip()

            # Skip comments
            if stripped.startswith("//"):
                continue

            # Track brace depth
            open_braces = stripped.count("{")
            close_braces = stripped.count("}")

            # Detect function start
            if not in_function and open_braces > 0:
                # Check if this looks like a function body start
                if brace_depth == 0 and "(" in stripped or i > 1:
                    prev_lines = "".join(lines[max(0, i - 3) : i])
                    if "(" in prev_lines and ")" in prev_lines:
                        in_function = True
                        function_start = i
                        local_vars = {}

            brace_depth += open_braces - close_braces

            # Function ended
            if in_function and brace_depth == 0:
                # Report unmodified variables
                for var_name, (decl_line, is_modified) in local_vars.items():
                    if not is_modified:
                        # Check the declaration line doesn't already have const
                        decl = lines[decl_line - 1]
                        if not re.search(rf"\bconst\b.*\b{var_name}\b", decl):
                            self.add_violation(
                                decl_line,
                                f"Local variable '{var_name}' is never modified. "
                                "Consider declaring it as const.",
                            )
                in_function = False
                local_vars = {}
                continue

            if not in_function:
                continue

            # Skip type aliases (using = ...)
            if stripped.startswith("using "):
                continue

            # Look for variable declarations
            match = var_decl.match(stripped)
            if match:
                type_name = match.group(1)
                var_name = match.group(2)

                # Skip loop variables, iterators, and common mutable patterns
                if var_name in ("i", "j", "k", "n", "it", "iter", "idx", "index"):
                    continue

                # Skip class member-like names (start with underscore)
                if var_name.startswith("_"):
                    continue

                # Skip RAII lock variables (lock_guard, unique_lock, scoped_lock, etc.)
                if var_name in ("lock", "lk", "guard") or "lock" in var_name.lower():
                    continue
                if (
                    "lock_guard" in stripped
                    or "unique_lock" in stripped
                    or "scoped_lock" in stripped
                ):
                    continue

                # Skip RAII objects (Profile, Timer, Scope, Guard, etc.)
                raii_types = ("Profile", "Timer", "Scope", "Guard", "Clock", "Stopwatch")
                if type_name in raii_types or any(t in type_name for t in raii_types):
                    continue

                # Skip smart pointers (unique_ptr, shared_ptr) - commonly moved/returned
                if "unique_ptr" in stripped or "make_unique" in stripped:
                    continue
                if "shared_ptr" in stripped or "make_shared" in stripped:
                    continue

                # Skip if already const
                if "const " in stripped and stripped.index("const ") < stripped.index(
                    var_name
                ):
                    continue

                # Skip pointer/reference declarations (complex to analyze)
                if "*" in stripped[: stripped.index(var_name)] or "&" in stripped[
                    : stripped.index(var_name)
                ]:
                    continue

                # Skip function calls/declarations: check if ( is followed by ) on same line
                # This filters out lines like: Type funcName(args);
                var_pos = stripped.index(var_name)
                after_var = stripped[var_pos + len(var_name) :]
                if after_var.lstrip().startswith("(") and ")" in after_var:
                    # Looks like a function call, not a variable declaration
                    continue

                local_vars[var_name] = (i, False)

            # Check for modifications
            for var_name in list(local_vars.keys()):
                if local_vars[var_name][1]:  # Already marked as modified
                    continue

                # Check assignment
                if re.search(rf"\b{var_name}\s*(?:\[.*\])?\s*=[^=]", stripped):
                    # Skip if this is the declaration line
                    if local_vars[var_name][0] != i:
                        local_vars[var_name] = (local_vars[var_name][0], True)
                    continue

                # Check std::move() - variable cannot be const if it's moved
                move_match = move_pattern.search(stripped)
                if move_match and move_match.group(1) == var_name:
                    local_vars[var_name] = (local_vars[var_name][0], True)
                    continue

                # Check non-const method calls (set*, add*, etc.)
                method_match = nonconst_method_pattern.search(stripped)
                if method_match and method_match.group(1) == var_name:
                    local_vars[var_name] = (local_vars[var_name][0], True)
                    continue

                # Check if passed by pointer (address-of &var)
                # Use finditer to find ALL matches, not just the first one
                for addr_match in address_of_pattern.finditer(stripped):
                    if addr_match.group(1) == var_name:
                        local_vars[var_name] = (local_vars[var_name][0], True)
                        break

                # Check if passed to a function as argument (could be non-const reference)
                # Skip variables that are passed to functions with non-trivial names
                # This is conservative - better to miss a const than suggest wrong const
                for func_match in func_arg_pattern.finditer(stripped):
                    if func_match.group(1) == var_name:
                        # Skip if it's just the declaration line or a simple getter call
                        if local_vars[var_name][0] != i:
                            local_vars[var_name] = (local_vars[var_name][0], True)
                            break

                # Check modifying operations
                for pattern in modify_patterns:
                    if pattern.search(stripped):
                        match = pattern.search(stripped)
                        if match and match.group(1) == var_name:
                            local_vars[var_name] = (local_vars[var_name][0], True)
                            break
