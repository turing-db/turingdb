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
        # For maps, even operator[] access without assignment is non-const
        # because it may insert a default value
        subscript_access_pattern = re.compile(r"\b(\w+)\s*\[[^\]]+\]")

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

            # IMPORTANT: Skip lines that are NOT function declarations/definitions
            # These are local reference variable declarations like:
            #   std::vector<T>& var = expr.getVec();
            # They have '=' BEFORE the first '(' which indicates assignment, not a signature
            paren_pos = stripped.find("(")
            equals_pos = stripped.find("=")
            if equals_pos != -1 and equals_pos < paren_pos:
                continue

            # Skip if this looks like a control flow statement or variable initialization
            # Control flow: if(...), while(...), for(...), switch(...)
            # Lambda: [...](...) { }
            if re.match(r"^\s*(?:if|while|for|switch|catch)\s*\(", stripped):
                continue

            # Skip if this is just a variable declaration with brace initialization
            # e.g., Type var {init};
            if re.match(r"^\s*\w+(?:<[^>]+>)?\s+\w+\s*\{", stripped):
                continue

            # Collect non-const reference params from this function declaration
            nonconst_params: dict[str, tuple[int, str]] = {}  # name -> (line, container)

            for container in STL_CONTAINERS:
                # Match container type followed by & and parameter name
                # The parameter name must be followed by:
                #   - comma (more params), or
                #   - closing paren (last param), or
                #   - = (default value)
                # This prevents matching function return types like:
                #   std::vector<T>& funcName(params...)
                # Use word boundary \b to prevent 'map' matching 'unordered_map'
                # Template pattern handles nested templates like vector<pair<int, string>>
                # by matching non-<> chars OR complete nested <...> groups
                nested_template = r"<(?:[^<>]|<[^<>]*>)*>"
                pattern = (
                    rf"(?:std::)?\b{container}\b\s*{nested_template}\s*&\s*"
                    rf"(?!&)"  # Not && (rvalue reference)
                    rf"(\w+)"  # Parameter name
                    rf"(?=\s*[,)=])"  # Must be followed by comma, closing paren, or =
                )

                for match in re.finditer(pattern, stripped):
                    param_name = match.group(1)

                    # Extra check: make sure this param appears inside parentheses
                    # to avoid matching return types (return type comes BEFORE the paren)
                    paren_start = stripped.find("(")
                    if paren_start == -1 or match.start() < paren_start:
                        continue

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

                    # Check subscript access on map types (non-const even without assignment)
                    # For maps, operator[] can insert default values, making it non-const
                    container_type = nonconst_params[param_name][1]
                    map_containers = {"map", "unordered_map", "multimap", "unordered_multimap"}
                    if container_type in map_containers:
                        subscript_match = subscript_access_pattern.search(body_line)
                        if subscript_match and subscript_match.group(1) == param_name:
                            modified_params.add(param_name)
                            continue

                    # Check if passed to non-const reference function
                    # This is a heuristic - if param is passed to a function, assume it might be modified
                    # Pattern 1: sole argument - func(param)
                    if re.search(rf"\(\s*{param_name}\s*\)", body_line):
                        modified_params.add(param_name)
                        continue
                    # Pattern 2: first argument - func(param, ...)
                    if re.search(rf"\(\s*{param_name}\s*,", body_line):
                        modified_params.add(param_name)
                        continue
                    # Pattern 3: middle or last argument - func(..., param, ...) or func(..., param)
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
            r"(?!return|if|else|for|while|switch|case|break|continue|goto|throw|void|virtual|static|inline|"
            r"namespace|class|struct|enum|union)"
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
        # Handles: std::move(var), std::move(var.method()), std::move(var->method())
        move_pattern = re.compile(r"\bstd::move\s*\(\s*(\w+)\s*\)")
        # Also match std::move(var.something) or std::move(var->something)
        move_member_pattern = re.compile(r"\bstd::move\s*\(\s*(\w+)\s*(?:\.|\->)")

        # Pattern for non-const method calls (object.method() or object->method())
        # This catches methods that likely modify state
        nonconst_method_pattern = re.compile(
            r"(\w+)\s*(?:\.|\->)\s*(?:set|add|remove|delete|update|write|commit|submit|"
            r"start|stop|open|close|init|load|save|send|receive|connect|disconnect|"
            r"append|prepend|put|post|execute|run|process|handle|build|create|destroy|"
            r"allocate|free|release|acquire|lock|unlock|notify|signal|wait|join|detach|"
            r"dump|flush|emit|generate|finish|finalize|compute|calculate|resolve|apply|"
            r"transform|mutate|modify|change|reset|invalidate|parse|read|fetch|pull|push|"
            r"enqueue|dequeue|schedule|dispatch|register|unregister|enable|disable|"
            r"attach|mount|configure|setup|prepare|complete|abort|cancel|terminate|"
            r"incr|decr|increment|decrement|advance|rewind|seek|skip|next|prev|move|copy)\w*\s*\("
        )

        # Pattern for chained method calls ending in non-const method: var.method().release()
        # This detects var.something().release() or var.something()->release()
        chained_nonconst_methods = ("release", "reset", "swap", "clear")
        chained_nonconst_pattern = re.compile(
            r"\b(\w+)\s*(?:\.|\->).*?\.(?:" + "|".join(chained_nonconst_methods) + r")\s*\("
        )

        # Pattern for passing variable by pointer (address-of)
        # Functions that take &var can modify it
        # Handles: func(&var), func(a, &var), (Type*)&var, reinterpret_cast<T>(&var)
        address_of_pattern = re.compile(r"[,(]\s*&(\w+)\s*[,)]")
        # Additional patterns for cast before address-of:
        # C-style cast: (Type*)&var or (struct foo*)&var
        c_style_cast_address_of = re.compile(r"\([^)]+\)\s*&(\w+)")
        # C++ style cast: reinterpret_cast<T>(&var)
        cpp_style_cast_address_of = re.compile(
            r"(?:reinterpret|static|const|dynamic)_cast\s*<[^>]+>\s*\(\s*&(\w+)\s*\)"
        )
        # Pattern for &var at start of line (multi-line function call continuation)
        line_start_address_of_pattern = re.compile(r"^\s*&(\w+)\s*[,)]")

        # Pattern for const accessor methods passed as function arguments
        # These methods have const/non-const overloads with different return types,
        # so the variable may need to be non-const for the result to be usable
        # e.g., file->write(hello.data(), ...) - if hello were const, .data() returns
        # const char* which can't convert to void*
        const_accessor_arg_pattern = re.compile(
            r"[,(]\s*(\w+)\s*\.\s*(?:data|c_str|get|value|ptr|ref)\s*\(\s*\)\s*[,)]"
        )

        # Pattern for reference binding: auto& ref = var.method()
        # When a reference is bound to an object's internal state and later moved,
        # the source object is indirectly modified
        ref_binding_pattern = re.compile(
            r"auto\s*&\s*(\w+)\s*=\s*(\w+)\s*\.\s*\w+\s*\("
        )

        # Known const accessor method prefixes - calling these doesn't modify the object
        # Using abstract interpretation principle: if we see a method that's NOT in this
        # list, conservatively assume it might be non-const
        const_method_prefixes = (
            "get", "is", "has", "can", "should", "will", "was", "does", "did",
            "size", "length", "count", "empty", "valid", "contains", "find",
            "at", "front", "back", "begin", "end", "cbegin", "cend",
            "rbegin", "rend", "crbegin", "crend", "data", "c_str",
            "to", "as", "what", "describe", "str", "string", "name", "type",
            "value", "key", "first", "second", "ptr", "ref", "view", "span",
            "check", "test", "compare", "equal", "match", "exists", "defined",
            "operator",  # comparison operators are typically const
        )

        # Pattern to detect any method call on an object: obj.method() or obj->method()
        any_method_pattern = re.compile(r"\b(\w+)\s*(?:\.|\->)\s*(\w+)\s*\(")

        # Known functions that take arguments by value (don't modify their args)
        # These are typically printf-family, assertion macros, and logging functions
        const_safe_functions = {
            "printf", "sprintf", "snprintf", "fprintf", "vprintf", "vsprintf",
            "vsnprintf", "vfprintf", "puts", "fputs", "putchar", "fputc",
            "assert", "static_assert", "bioassert",
            "sizeof", "alignof", "decltype", "typeid",
            "std::cout", "std::cerr", "std::clog",
            "fmt::print", "fmt::format", "fmt::println",
            "spdlog::info", "spdlog::debug", "spdlog::warn", "spdlog::error",
            "spdlog::trace", "spdlog::critical",
            "LOG_INFO", "LOG_DEBUG", "LOG_WARN", "LOG_ERROR", "LOG_TRACE",
        }

        lines = context.lines
        in_multiline_comment = False
        for i, line in enumerate(lines, start=1):
            stripped = line.strip()

            # Handle multi-line comments
            if in_multiline_comment:
                if "*/" in stripped:
                    in_multiline_comment = False
                continue

            # Skip single-line comments
            if stripped.startswith("//"):
                continue

            # Check for start of multi-line comment
            if "/*" in stripped:
                if "*/" not in stripped:  # Comment continues to next line
                    in_multiline_comment = True
                continue

            # Track brace depth
            open_braces = stripped.count("{")
            close_braces = stripped.count("}")

            # Detect function start
            if not in_function and open_braces > 0:
                # Check if this looks like a function body start
                # Look for function signature with () either on this line or previous lines
                if brace_depth == 0:
                    # Check current line and up to 3 previous lines for function signature
                    # Note: i is 1-indexed, lines is 0-indexed, so lines[i-1] is current line
                    prev_lines = "".join(lines[max(0, i - 4) : i - 1])
                    all_context = prev_lines + stripped
                    if "(" in all_context and ")" in all_context:
                        in_function = True
                        function_start = i
                        local_vars = {}
                        reference_bindings: dict[str, str] = {}  # ref_name -> source_var

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
                reference_bindings = {}
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
                should_add = True

                # Skip loop variables, iterators, and common mutable patterns
                if var_name in ("i", "j", "k", "n", "it", "iter", "idx", "index"):
                    should_add = False

                # Skip class member-like names (start with underscore)
                if should_add and var_name.startswith("_"):
                    should_add = False

                # Skip RAII lock variables (lock_guard, unique_lock, scoped_lock, etc.)
                if should_add and (
                    var_name in ("lock", "lk", "guard") or "lock" in var_name.lower()
                ):
                    should_add = False
                if should_add and (
                    "lock_guard" in stripped
                    or "unique_lock" in stripped
                    or "scoped_lock" in stripped
                ):
                    should_add = False

                # Skip RAII objects (Profile, Timer, Scope, Guard, etc.)
                raii_types = ("Profile", "Timer", "Scope", "Guard", "Clock", "Stopwatch")
                if should_add and (
                    type_name in raii_types or any(t in type_name for t in raii_types)
                ):
                    should_add = False

                # Skip smart pointers (unique_ptr, shared_ptr) - commonly moved/returned
                if should_add and (
                    "unique_ptr" in stripped or "make_unique" in stripped
                ):
                    should_add = False
                if should_add and (
                    "shared_ptr" in stripped or "make_shared" in stripped
                ):
                    should_add = False

                # Skip if already const
                if should_add and "const " in stripped:
                    if stripped.index("const ") < stripped.index(var_name):
                        should_add = False

                # Skip pointer/reference declarations (complex to analyze)
                if should_add:
                    var_idx = stripped.index(var_name)
                    if "*" in stripped[:var_idx] or "&" in stripped[:var_idx]:
                        should_add = False

                # Skip function calls/declarations: check if ( is followed by ) on same line
                # This filters out lines like: Type funcName(args);
                if should_add:
                    var_pos = stripped.index(var_name)
                    after_var = stripped[var_pos + len(var_name) :]
                    if after_var.lstrip().startswith("(") and ")" in after_var:
                        # Looks like a function call, not a variable declaration
                        should_add = False

                if should_add:
                    local_vars[var_name] = (i, False)

            # Detect reference bindings: auto& ref = var.method()
            # When a reference is bound to internal state and later moved,
            # the source variable is indirectly modified
            ref_match = ref_binding_pattern.search(stripped)
            if ref_match:
                ref_name = ref_match.group(1)
                source_var = ref_match.group(2)
                if source_var in local_vars:
                    reference_bindings[ref_name] = source_var

            # Check for modifications
            for var_name in list(local_vars.keys()):
                if local_vars[var_name][1]:  # Already marked as modified
                    continue

                # Check assignment (direct assignment or array assignment)
                if re.search(rf"\b{var_name}\s*(?:\[.*\])?\s*=[^=]", stripped):
                    # Skip if this is the declaration line
                    if local_vars[var_name][0] != i:
                        local_vars[var_name] = (local_vars[var_name][0], True)
                    continue

                # Check assignment through pointer member access: var->member = ...
                if re.search(rf"\b{var_name}\s*->\s*\w+\s*=[^=]", stripped):
                    local_vars[var_name] = (local_vars[var_name][0], True)
                    continue

                # Check std::move() - variable cannot be const if it's moved
                # Handles both std::move(var) and std::move(var.method())
                # Also handles move via reference binding (auto& ref = var.value(); move(ref))
                move_match = move_pattern.search(stripped)
                if move_match:
                    moved_name = move_match.group(1)
                    # Direct move of this variable
                    if moved_name == var_name:
                        local_vars[var_name] = (local_vars[var_name][0], True)
                        continue
                    # Move via reference binding - mark source as modified
                    if moved_name in reference_bindings:
                        source = reference_bindings[moved_name]
                        if source == var_name:
                            local_vars[var_name] = (local_vars[var_name][0], True)
                            continue
                move_member_match = move_member_pattern.search(stripped)
                if move_member_match and move_member_match.group(1) == var_name:
                    local_vars[var_name] = (local_vars[var_name][0], True)
                    continue

                # Check non-const method calls (set*, add*, etc.)
                method_match = nonconst_method_pattern.search(stripped)
                if method_match and method_match.group(1) == var_name:
                    local_vars[var_name] = (local_vars[var_name][0], True)
                    continue

                # Check chained non-const method calls: var.something().release()
                chained_match = chained_nonconst_pattern.search(stripped)
                if chained_match and chained_match.group(1) == var_name:
                    local_vars[var_name] = (local_vars[var_name][0], True)
                    continue

                # Check if const accessor method result is passed to a function
                # e.g., file->write(hello.data(), hello.size())
                # If hello were const, .data() returns const char* which can't convert to void*
                for accessor_match in const_accessor_arg_pattern.finditer(stripped):
                    if accessor_match.group(1) == var_name:
                        local_vars[var_name] = (local_vars[var_name][0], True)
                        break

                # Check if passed by pointer (address-of &var)
                # Use finditer to find ALL matches, not just the first one
                for addr_match in address_of_pattern.finditer(stripped):
                    if addr_match.group(1) == var_name:
                        local_vars[var_name] = (local_vars[var_name][0], True)
                        break

                # Check for cast before address-of: (Type*)&var or cast<T>(&var)
                for cast_match in c_style_cast_address_of.finditer(stripped):
                    if cast_match.group(1) == var_name:
                        local_vars[var_name] = (local_vars[var_name][0], True)
                        break
                for cast_match in cpp_style_cast_address_of.finditer(stripped):
                    if cast_match.group(1) == var_name:
                        local_vars[var_name] = (local_vars[var_name][0], True)
                        break

                # Check for &var at start of line (multi-line function call)
                line_start_match = line_start_address_of_pattern.match(stripped)
                if line_start_match and line_start_match.group(1) == var_name:
                    local_vars[var_name] = (local_vars[var_name][0], True)

                # Check if passed to a function as argument (could be non-const reference)
                # This is conservative - better to miss a const than suggest wrong const
                if local_vars[var_name][0] != i:  # Skip declaration line
                    # First check if this is a call to a known const-safe function
                    # (functions that take arguments by value)
                    is_const_safe_call = False
                    for func_name in const_safe_functions:
                        if func_name in stripped:
                            is_const_safe_call = True
                            break

                    # Skip control flow statements - these are not function calls
                    # if (var), while (var), for (...; var; ...), switch (var)
                    is_control_flow = re.match(
                        r"^\s*(?:if|while|for|switch|catch|return)\s*\(", stripped
                    )

                    if not is_const_safe_call and not is_control_flow:
                        # Check if var appears as a function argument:
                        # 1. After ( or , and before , or ) - handles: func(var), func(a, var)
                        # 2. This handles nested parens: func(a.method(), var, b)
                        # Pattern matches: "(var," or "(var)" or ", var," or ", var)"
                        if re.search(rf"[,(]\s*{var_name}\s*[,)]", stripped):
                            local_vars[var_name] = (local_vars[var_name][0], True)
                            continue
                        # Also match: "(var " for cases like "(var ," with space
                        if re.search(rf"\(\s*{var_name}\s*,", stripped):
                            local_vars[var_name] = (local_vars[var_name][0], True)
                            continue
                        # Check for dereferenced variable passed as argument: func(*var)
                        if re.search(rf"[,(]\s*\*{var_name}\s*[,)]", stripped):
                            local_vars[var_name] = (local_vars[var_name][0], True)
                            continue

                # Abstract interpretation: Check method calls on this object
                # If a method is called that's NOT known to be const, assume modification
                for method_match in any_method_pattern.finditer(stripped):
                    if method_match.group(1) == var_name:
                        method_name = method_match.group(2).lower()
                        # Check if this is a known const method by prefix
                        is_const_method = any(
                            method_name.startswith(prefix)
                            for prefix in const_method_prefixes
                        )
                        if not is_const_method:
                            local_vars[var_name] = (local_vars[var_name][0], True)
                            break

                # Check modifying operations
                for pattern in modify_patterns:
                    # Use finditer to find ALL matches, not just the first one
                    for match in pattern.finditer(stripped):
                        if match.group(1) == var_name:
                            local_vars[var_name] = (local_vars[var_name][0], True)
                            break
                    else:
                        continue
                    break
