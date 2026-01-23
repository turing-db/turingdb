#!/usr/bin/env python3
"""
TuringDB Code Style Reviewer

A static code style checker for C++ files based on CODING_STYLE.md.
Designed to run in GitHub Actions without requiring AI model calls.

Usage:
    python code_review.py file1.cpp file2.h           # Check specific files
    python code_review.py --diff origin/main          # Check changed files vs base
    python code_review.py --format github             # GitHub Actions annotations
"""

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass, asdict
from pathlib import Path


@dataclass
class Violation:
    filepath: str
    line: int
    severity: str  # "error" or "warning"
    rule: str
    message: str


class StyleChecker:
    """Checks a single C++ file for style violations."""

    # Directories to skip
    SKIP_DIRS = {"external", "third_party", "build", ".git", "googletest", "test"}

    # File patterns to skip
    SKIP_FILES = {"test_violations.h", "test_violations.cpp"}

    # File extensions to check
    CPP_EXTENSIONS = {".cpp", ".h", ".hpp", ".cc", ".cxx"}

    # Threshold for code density warning
    DENSITY_THRESHOLD = 15

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.lines: list[str] = []
        self.violations: list[Violation] = []
        self._is_header = Path(filepath).suffix in {".h", ".hpp"}

    def load(self) -> bool:
        """Load file contents. Returns False if file should be skipped."""
        path = Path(self.filepath)

        # Check if in skip directory
        for part in path.parts:
            if part in self.SKIP_DIRS:
                return False

        # Check if file should be skipped
        if path.name in self.SKIP_FILES:
            return False

        # Skip unit test files (*Test.cpp, *Test.h)
        if path.stem.endswith("Test"):
            return False

        # Check extension
        if path.suffix not in self.CPP_EXTENSIONS:
            return False

        try:
            with open(self.filepath, "r", encoding="utf-8", errors="replace") as f:
                self.lines = f.read().splitlines()
            return True
        except (IOError, OSError) as e:
            print(f"Warning: Could not read {self.filepath}: {e}", file=sys.stderr)
            return False

    def add_violation(self, line: int, severity: str, rule: str, message: str):
        self.violations.append(Violation(
            filepath=self.filepath,
            line=line,
            severity=severity,
            rule=rule,
            message=message,
        ))

    # Known external library prefixes (angle bracket includes)
    EXTERNAL_LIBS = {
        "spdlog", "nlohmann", "crow", "antlr4", "gtest", "gmock",
        "faiss", "aws", "boost", "fmt", "rapidjson", "tbb",
        "linenoise", "tabulate", "argparse", "termcolor", "range",
    }

    # Known standard library headers
    STD_HEADERS = {
        # C headers
        "assert.h", "ctype.h", "errno.h", "float.h", "limits.h",
        "locale.h", "math.h", "setjmp.h", "signal.h", "stdarg.h",
        "stddef.h", "stdio.h", "stdlib.h", "string.h", "time.h",
        "wchar.h", "wctype.h", "complex.h", "fenv.h", "inttypes.h",
        "stdbool.h", "stdint.h", "tgmath.h", "uchar.h",
        # C++ wrapper headers (C headers with c prefix) - NOTE: these are discouraged,
        # prefer the .h versions (e.g., <stdint.h> instead of <cstdint>)
        "cassert", "cctype", "cerrno", "cfloat", "climits", "clocale",
        "cmath", "csetjmp", "csignal", "cstdarg", "cstddef", "cstdio",
        "cstdlib", "cstring", "ctime", "cwchar", "cwctype", "ccomplex",
        "cfenv", "cinttypes", "cstdbool", "cstdint", "ctgmath", "cuchar",
        # POSIX/System headers
        "unistd.h", "fcntl.h", "pthread.h", "dirent.h", "dlfcn.h",
        "sys/types.h", "sys/stat.h", "sys/wait.h", "sys/mman.h",
        "sys/socket.h", "sys/epoll.h", "sys/event.h", "sys/signalfd.h",
        "sys/proc_info.h", "sys/time.h", "sys/resource.h", "sys/ioctl.h",
        "sys/uio.h", "sys/un.h", "sys/select.h", "sys/poll.h",
        "netinet/in.h", "netinet/tcp.h", "netinet/ip.h", "arpa/inet.h",
        "net/if.h", "netdb.h",
        # macOS specific
        "libproc.h", "mach/mach.h", "mach/mach_time.h",
        # Compiler intrinsics (x86, ARM, etc.)
        "immintrin.h", "emmintrin.h", "xmmintrin.h", "pmmintrin.h",
        "smmintrin.h", "nmmintrin.h", "ammintrin.h", "avxintrin.h",
        "avx2intrin.h", "arm_neon.h",
        # C++ headers
        "algorithm", "any", "array", "atomic", "barrier", "bit",
        "bitset", "charconv", "chrono", "codecvt", "compare",
        "complex", "concepts", "condition_variable", "coroutine",
        "deque", "exception", "execution", "expected", "filesystem",
        "format", "forward_list", "fstream", "functional", "future",
        "initializer_list", "iomanip", "ios", "iosfwd", "iostream",
        "istream", "iterator", "latch", "limits", "list", "locale",
        "map", "memory", "memory_resource", "mutex", "new", "numbers",
        "numeric", "optional", "ostream", "queue", "random", "ranges",
        "ratio", "regex", "scoped_allocator", "semaphore", "set",
        "shared_mutex", "source_location", "span", "spanstream",
        "sstream", "stack", "stacktrace", "stdexcept", "stop_token",
        "streambuf", "string", "string_view", "strstream", "syncstream",
        "system_error", "thread", "tuple", "type_traits", "typeindex",
        "typeinfo", "unordered_map", "unordered_set", "utility",
        "valarray", "variant", "vector", "version",
    }

    # STL containers that should not be returned by value
    STL_CONTAINERS = {
        "vector", "map", "unordered_map", "set", "unordered_set",
        "list", "deque", "forward_list", "multimap",
        "unordered_multimap", "multiset", "unordered_multiset",
        "stack", "queue", "priority_queue", "string", "wstring",
        "basic_string", "string_view",
        # Note: span is excluded - it's a lightweight non-owning view
        # Note: array is excluded - it's fixed-size and trivially copyable
    }

    # Result type patterns - classes designed to be returned by value
    RESULT_TYPE_PATTERNS = {
        "Result", "Error", "Status", "Optional", "Expected",
        "Outcome", "Maybe", "Either", "Try", "Response",
    }

    # C++ wrapper headers mapped to their preferred C-style equivalents
    # Per CODING_STYLE.md: use <stdlib.h> style, not <cstdlib>
    CPP_TO_C_HEADERS = {
        "cassert": "assert.h",
        "ccomplex": "complex.h",
        "cctype": "ctype.h",
        "cerrno": "errno.h",
        "cfenv": "fenv.h",
        "cfloat": "float.h",
        "cinttypes": "inttypes.h",
        "climits": "limits.h",
        "clocale": "locale.h",
        "cmath": "math.h",
        "csetjmp": "setjmp.h",
        "csignal": "signal.h",
        "cstdarg": "stdarg.h",
        "cstdbool": "stdbool.h",
        "cstddef": "stddef.h",
        "cstdint": "stdint.h",
        "cstdio": "stdio.h",
        "cstdlib": "stdlib.h",
        "cstring": "string.h",
        "ctgmath": "tgmath.h",
        "ctime": "time.h",
        "cuchar": "uchar.h",
        "cwchar": "wchar.h",
        "cwctype": "wctype.h",
    }

    def check_all(self) -> list[Violation]:
        """Run all style checks and return violations."""
        self.check_consecutive_blanks()
        self.check_using_namespace()
        self.check_include_order()
        self.check_pointer_member_init()
        self.check_stl_return_by_value()
        self.check_nontrivial_return_by_value()
        self.check_missing_const()
        self.check_constructor_brackets()
        self.check_destructor_brackets()
        self.check_bracket_positioning()
        self.check_code_density()
        return self.violations

    def check_consecutive_blanks(self):
        """Check for 2+ consecutive blank lines."""
        blank_count = 0
        blank_start = 0
        # Track recent lines to detect macro blocks
        recent_nonblank_lines: list[str] = []

        for i, line in enumerate(self.lines, start=1):
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
                            "error",
                            "Formatting",
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
                "error",
                "Formatting",
                f"Found {blank_count} consecutive blank lines at end of file",
            )

    def check_using_namespace(self):
        """Check for forbidden 'using namespace' directives."""
        pattern = re.compile(r"^\s*using\s+namespace\s+(\w+)\s*;")

        for i, line in enumerate(self.lines, start=1):
            match = pattern.match(line)
            if match:
                namespace = match.group(1)
                if namespace == "std":
                    # Never allowed anywhere
                    self.add_violation(
                        i,
                        "error",
                        "Namespaces",
                        "'using namespace std' is never allowed",
                    )
                elif self._is_header:
                    # Any 'using namespace' forbidden in headers
                    self.add_violation(
                        i,
                        "error",
                        "Namespaces",
                        f"'using namespace {namespace}' not allowed in header files",
                    )

    def check_include_order(self):
        """Check that includes follow the proper order.

        Order should be:
        1. Current class header (for .cpp files) - followed by blank line
        2. Standard library headers (<...>)
        3. External library headers (<spdlog/...>, etc.)
        4. Project headers ("...")
        """
        # Parse all includes
        include_pattern = re.compile(r'^\s*#include\s+([<"])([^>"]+)[>"]')

        includes: list[tuple[int, str, str, str]] = []  # (line, type, path, full)
        first_include_line = 0
        in_include_block = False

        for i, line in enumerate(self.lines, start=1):
            stripped = line.strip()

            # Skip empty lines and track include block boundaries
            if stripped == "":
                if in_include_block and includes:
                    # Mark end of include group
                    includes.append((i, "blank", "", ""))
                continue

            match = include_pattern.match(stripped)
            if match:
                if not first_include_line:
                    first_include_line = i
                in_include_block = True

                bracket = match.group(1)  # < or "
                path = match.group(2)
                inc_type = self._classify_include(bracket, path)
                includes.append((i, inc_type, path, stripped))
            elif stripped.startswith("#"):
                # Other preprocessor, continue
                continue
            else:
                # Non-include, non-blank line - end of include block
                if in_include_block:
                    break

        if not includes:
            return

        # Check 1: For .cpp files that implement a class, first include should match filename
        # Skip main.cpp, tool files, and similar entry point files
        filename = Path(self.filepath).stem  # e.g., "Graph" from "Graph.cpp"
        tool_files = {"main", "TuringImport", "TuringDBTool", "S3TestCli", "plan2"}
        if not self._is_header and self._has_class_implementation() and filename not in tool_files:
            first_inc = includes[0]
            if first_inc[1] != "blank":
                first_path = Path(first_inc[2]).stem
                if first_path.lower() != filename.lower():
                    # Only flag if the expected header file actually exists
                    expected_header = Path(self.filepath).parent / f"{filename}.h"
                    if expected_header.exists():
                        self.add_violation(
                            first_inc[0],
                            "error",
                            "Includes",
                            f"First include should be \"{filename}.h\" (current class header)",
                        )
                else:
                    # Check for blank line after first include
                    if len(includes) > 1 and includes[1][1] != "blank":
                        self.add_violation(
                            first_inc[0],
                            "error",
                            "Includes",
                            "Current class header should be followed by a blank line",
                        )

        # Check 2: Flag C++ wrapper headers (cstdint, cstdlib, etc.)
        for line_num, inc_type, path, full in includes:
            if inc_type == "blank":
                continue
            if path in self.CPP_TO_C_HEADERS:
                preferred = self.CPP_TO_C_HEADERS[path]
                self.add_violation(
                    line_num,
                    "error",
                    "Includes",
                    f"Use <{preferred}> instead of <{path}> (prefer C-style headers)",
                )

        # Check 3: Verify include order (standard -> external -> project)
        # Filter out blank markers for order checking
        real_includes = [(i, t, p, f) for i, t, p, f in includes if t != "blank"]

        # Skip the first include in .cpp files if it's the class header
        start_idx = 0
        if not self._is_header and real_includes and self._has_class_implementation():
            filename = Path(self.filepath).stem
            first_path = Path(real_includes[0][2]).stem
            if first_path.lower() == filename.lower():
                start_idx = 1

        # In header files, skip parent class headers that appear first
        # (e.g., #include "Iterator.h" at the top of GetEdgesIterator.h)
        parent_headers = set()
        if self._is_header:
            parent_headers = self._get_parent_class_headers()

        # Track the "phase" of includes we're in
        # 0 = standard, 1 = external, 2 = project
        phase = 0
        phase_names = ["standard library", "external library", "project"]

        for line_num, inc_type, path, full in real_includes[start_idx:]:
            if inc_type == "standard":
                if phase > 0:
                    self.add_violation(
                        line_num,
                        "error",
                        "Includes",
                        f"Standard library include <{path}> should come before "
                        f"{phase_names[phase]} includes",
                    )
            elif inc_type == "external":
                if phase > 1:
                    self.add_violation(
                        line_num,
                        "error",
                        "Includes",
                        f"External library include <{path}> should come before "
                        "project includes",
                    )
                phase = max(phase, 1)
            elif inc_type == "project":
                # Allow parent class headers to appear before standard includes
                include_stem = Path(path).stem.lower()
                if phase == 0 and include_stem in parent_headers:
                    # This is a parent class header at the top - allowed
                    continue
                phase = 2

    def _classify_include(self, bracket: str, path: str) -> str:
        """Classify an include as standard, external, or project."""
        if bracket == '"':
            return "project"

        # Angle bracket include - check if standard or external
        # Get the base header name
        parts = path.split("/")
        first_part = parts[0]

        # Check if it's a known external library
        if first_part in self.EXTERNAL_LIBS:
            return "external"

        # Check if it's a standard header
        if path in self.STD_HEADERS or first_part in self.STD_HEADERS:
            return "standard"

        # Check for common standard library/system header patterns
        system_prefixes = ("sys/", "netinet/", "arpa/", "net/", "mach/")
        if any(path.startswith(prefix) for prefix in system_prefixes):
            return "standard"

        # FlexLexer.h is a system header (part of flex)
        if path == "FlexLexer.h":
            return "standard"

        # range-v3 and similar template libraries in angle brackets
        # but treat as external since they're not in the standard library
        if path.startswith("range/"):
            return "external"

        # Default: treat unknown angle-bracket includes as external
        return "external"

    def _has_class_implementation(self) -> bool:
        """Check if this .cpp file implements a class (has ClassName:: methods)."""
        # Look for method definitions like ClassName::methodName or ClassName::ClassName
        # Return type can be: void, int, std::string, vector<T>, const Type&, etc.
        # Class name can include template args: TemplateClass<T>::method()
        method_pattern = re.compile(
            r"^\s*"
            r"(?:(?:const|static|inline|virtual)\s+)*"  # optional qualifiers
            r"(?:[\w:]+(?:<[^>]+>)?[*&\s]+)*"  # return type (handles std::string, etc.)
            r"(\w+)(?:<[^>]+>)?::~?\w+\s*\("  # ClassName<T>::methodName(
        )
        for line in self.lines:
            if method_pattern.match(line.strip()):
                return True
        return False

    def _get_parent_class_headers(self) -> set[str]:
        """Get the header names of parent classes from inheritance declarations.

        Looks for patterns like:
            class ChildClass : public ParentClass {
            class ChildClass : public Parent1, public Parent2 {
            struct ChildStruct : public ParentStruct {

        Returns a set of parent class names (lowercased, without .h extension).
        """
        parent_headers = set()
        # Match class/struct inheritance: class Name : access Parent
        inherit_pattern = re.compile(
            r"^\s*(?:class|struct)\s+\w+\s*"  # class/struct Name
            r"(?:final\s*)?"  # optional final
            r":\s*"  # colon
            r"(.+?)"  # capture inheritance list
            r"\s*\{?\s*$"  # optional opening brace
        )
        # Pattern to extract parent class names from inheritance list
        parent_pattern = re.compile(r"(?:public|protected|private)\s+(\w+)")

        for line in self.lines:
            match = inherit_pattern.match(line.strip())
            if match:
                inheritance_list = match.group(1)
                for parent_match in parent_pattern.finditer(inheritance_list):
                    parent_name = parent_match.group(1)
                    parent_headers.add(parent_name.lower())

        return parent_headers

    def check_pointer_member_init(self):
        """Check that pointer class members are initialized with {nullptr}.

        Rule: All pointer members should be default-initialized to nullptr
        using brace initialization: MyType* _member {nullptr};
        """
        # Only check header files (class definitions are in headers)
        if not self._is_header:
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

        # Smart pointer types that don't need nullptr initialization
        smart_pointers = {"unique_ptr", "shared_ptr", "weak_ptr", "auto_ptr"}

        for i, line in enumerate(self.lines, start=1):
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
            if any(sp in stripped for sp in smart_pointers):
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
                        "error",
                        "Initialization",
                        f"Pointer member '{member_name}' must be initialized with {{nullptr}}",
                    )
                elif ending == "=":
                    # Using = style instead of {} - error
                    self.add_violation(
                        i,
                        "error",
                        "Initialization",
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
                                "error",
                                "Initialization",
                                f"Pointer member '{member_name}' should be initialized "
                                "with {{nullptr}}, not {{}}",
                            )

    def check_stl_return_by_value(self):
        """Check that functions don't return STL containers by value.

        Rule: Do not return STL containers or strings by value.
        Use output parameters (reference or pointer) instead.
        """
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
            r"(?:&{1,2})?"  # optional reference (& or &&) - these are OK actually
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

        for i, line in enumerate(self.lines, start=1):
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
            if self._is_header:
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

                # Skip debug/logging methods that commonly return strings
                if func_name in ("describe", "toString", "to_string", "str", "dump",
                                 "format", "repr", "debug", "print", "show"):
                    continue

                # Check if it's an STL container
                if container in self.STL_CONTAINERS:
                    # Check if it's returning by reference (OK) or by value (bad)
                    # Look for & after the > but before the function name
                    after_template = stripped[stripped.find(">") + 1:]
                    before_func = after_template[:after_template.find(func_name)]

                    if "&" not in before_func:
                        self.add_violation(
                            i,
                            "error",
                            "Return Type",
                            f"Function '{func_name}' returns std::{container} by value. "
                            "Use an output parameter instead.",
                        )

            # Check for string return
            match = string_pattern.match(stripped)
            if match:
                string_type = match.group(1)
                func_name = match.group(2)

                # Skip debug/logging methods that commonly return strings
                if func_name in ("describe", "toString", "to_string", "str", "dump",
                                 "format", "repr", "debug", "print", "show"):
                    continue

                # Check it's not returning by reference
                type_end = stripped.find(string_type) + len(string_type)
                after_type = stripped[type_end:stripped.find(func_name)]

                if "&" not in after_type:
                    self.add_violation(
                        i,
                        "error",
                        "Return Type",
                        f"Function '{func_name}' returns std::{string_type} by value. "
                        "Use an output parameter instead.",
                    )

    def check_nontrivial_return_by_value(self):
        """Check that non-trivial classes are not returned by value.

        Non-trivial classes are those containing STL data structure members.
        Result types (classes designed to be returned) are excluded.
        """
        # Only check header files where class definitions live
        if not self._is_header:
            return

        # First pass: find classes with STL members
        nontrivial_classes: dict[str, int] = {}  # class name -> line defined
        current_class = None
        class_start_line = 0
        brace_depth = 0
        class_brace_depth = 0

        for i, line in enumerate(self.lines, start=1):
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
                    pattern in class_name for pattern in self.RESULT_TYPE_PATTERNS
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
                for container in self.STL_CONTAINERS:
                    # Match std::vector<...> or vector<...> member declarations
                    pattern = rf"(?:std::)?{container}\s*<"
                    if re.search(pattern, stripped):
                        # This class has an STL member - it's non-trivial
                        if current_class not in nontrivial_classes:
                            nontrivial_classes[current_class] = class_start_line
                        break

        # Second pass: check for functions returning non-trivial classes by value
        for i, line in enumerate(self.lines, start=1):
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
                    between = stripped[type_end:stripped.find(func_name)]

                    if "&" not in between and "*" not in between:
                        self.add_violation(
                            i,
                            "error",
                            "Return Type",
                            f"Function '{func_name}' returns non-trivial class "
                            f"'{class_name}' by value. Use a pointer or output parameter.",
                        )

    def check_missing_const(self):
        """Check for missing const in common cases.

        Detects:
        1. Non-const reference parameters for STL containers (likely should be const)
        2. Local variables that are never modified after initialization
        """
        # Only check implementation files for local variable analysis
        # Check both for reference parameters

        self._check_nonconst_reference_params()
        if not self._is_header:
            self._check_nonconst_local_variables()

    def _check_nonconst_reference_params(self):
        """Check for non-const reference parameters that should likely be const.

        Focuses on STL container references which are almost always input-only.
        """
        # Pattern to match function declarations/definitions with parameters
        # We look for STL container references without const
        for i, line in enumerate(self.lines, start=1):
            stripped = line.strip()

            # Skip comments and preprocessor
            if not stripped or stripped.startswith("//") or stripped.startswith("#"):
                continue

            # Must have parentheses (function signature)
            if "(" not in stripped:
                continue

            # Skip constructors/destructors by checking for ::ClassName( pattern
            if re.search(r"(\w+)::~?\1\s*\(", stripped):
                continue

            # Look for non-const STL container references in parameters
            for container in self.STL_CONTAINERS:
                # Match: std::vector<...>& name or vector<...>& name
                # But NOT: std::vector<...>* (pointer)
                pattern = (
                    rf"(?:std::)?{container}\s*<[^>]+>\s*&\s*"  # container<T>&
                    rf"(?!&)"  # not && (rvalue ref)
                    rf"(\w+)"  # parameter name
                )

                for match in re.finditer(pattern, stripped):
                    param_name = match.group(1)

                    # Skip if it looks like an output parameter
                    if param_name.lower() in ("result", "output", "out", "ret"):
                        continue

                    # Check if const appears before this match position
                    match_start = match.start()
                    before_match = stripped[:match_start]
                    # Look for "const" followed by optional whitespace at the end of before_match
                    # or "const " anywhere that applies to this parameter
                    if re.search(r"\bconst\s+(?:std::)?" + container, stripped[:match.end()]):
                        continue

                    self.add_violation(
                        i,
                        "warning",
                        "Const",
                        f"Parameter '{param_name}' is a non-const reference to "
                        f"std::{container}. Consider using const& if not modified.",
                    )

    def _check_nonconst_local_variables(self):
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
            re.compile(r"(\w+)\s*\.\s*(?:push|pop|insert|erase|clear|resize|assign|emplace|swap|reset)"),
            re.compile(r"(\w+)\s*\[\s*.*\s*\]\s*="),  # array/map assignment
            re.compile(r"(\w+)\s*\+\+"),  # increment
            re.compile(r"\+\+\s*(\w+)"),  # prefix increment
            re.compile(r"(\w+)\s*--"),  # decrement
            re.compile(r"--\s*(\w+)"),  # prefix decrement
            re.compile(r"(\w+)\s*(?:\+|-|\*|\/|%|&|\||\^)="),  # compound assignment
        ]

        for i, line in enumerate(self.lines, start=1):
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
                    prev_lines = "".join(self.lines[max(0, i-3):i])
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
                        decl = self.lines[decl_line - 1]
                        if not re.search(rf"\bconst\b.*\b{var_name}\b", decl):
                            self.add_violation(
                                decl_line,
                                "warning",
                                "Const",
                                f"Local variable '{var_name}' is never modified. "
                                "Consider declaring it as const.",
                            )
                in_function = False
                local_vars = {}
                continue

            if not in_function:
                continue

            # Look for variable declarations
            match = var_decl.match(stripped)
            if match:
                type_name = match.group(1)
                var_name = match.group(2)

                # Skip loop variables, iterators, and common mutable patterns
                if var_name in ("i", "j", "k", "n", "it", "iter", "idx", "index"):
                    continue

                # Skip RAII lock variables (lock_guard, unique_lock, scoped_lock, etc.)
                if var_name in ("lock", "lk", "guard") or "lock" in var_name.lower():
                    continue
                if "lock_guard" in stripped or "unique_lock" in stripped or "scoped_lock" in stripped:
                    continue

                # Skip if already const
                if "const " in stripped and stripped.index("const ") < stripped.index(var_name):
                    continue

                # Skip pointer/reference declarations (complex to analyze)
                if "*" in stripped[:stripped.index(var_name)] or "&" in stripped[:stripped.index(var_name)]:
                    continue

                # Skip function calls/declarations: check if ( is followed by ) on same line
                # This filters out lines like: Type funcName(args);
                var_pos = stripped.index(var_name)
                after_var = stripped[var_pos + len(var_name):]
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

                # Check modifying operations
                for pattern in modify_patterns:
                    if pattern.search(stripped):
                        match = pattern.search(stripped)
                        if match and match.group(1) == var_name:
                            local_vars[var_name] = (local_vars[var_name][0], True)
                            break

    def check_constructor_brackets(self):
        """Check that constructors have opening bracket on the next line.

        Per CODING_STYLE.md: Constructors are the only place where the opening
        bracket must be on the next line (applies to all constructors).
        """
        # Regex to match start of constructor: ClassName::ClassName(
        constructor_start = re.compile(r"^(\w+)::(\1)\s*\(")

        i = 0
        while i < len(self.lines):
            line = self.lines[i]
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
            while paren_depth > 0 and i < len(self.lines):
                next_line = self.lines[i].strip()
                constructor_text += " " + next_line
                paren_depth += next_line.count("(") - next_line.count(")")
                i += 1

            # Skip defaulted or deleted constructors (no body to check)
            if "= default" in constructor_text or "= delete" in constructor_text:
                continue

            # Now find the constructor body opener - should be a line that is just "{"
            found_body_opener = False
            for j in range(i - 1, min(i + 10, len(self.lines))):
                check_line = self.lines[j].strip()

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
                            "error",
                            "Constructors",
                            "Constructor opening bracket '{' must be on its own line",
                        )
                    found_body_opener = True
                    break

            if found_body_opener:
                i = j + 1

    def check_destructor_brackets(self):
        """Check that destructors have opening bracket on the same line."""
        # Pattern: ~ClassName() with optional whitespace
        destructor_start = re.compile(r"^(\w+)::~\1\s*\(\s*\)\s*$")

        for i, line in enumerate(self.lines, start=1):
            stripped = line.strip()
            match = destructor_start.match(stripped)

            if match:
                # Check if next non-empty line starts with {
                for j in range(i, min(i + 3, len(self.lines))):
                    next_line = self.lines[j].strip()
                    if next_line == "":
                        continue
                    if next_line.startswith("{"):
                        self.add_violation(
                            i,
                            "error",
                            "Destructors",
                            "Destructor opening bracket '{' must be on the same line",
                        )
                    break

    def check_bracket_positioning(self):
        """Check bracket positioning for control flow and functions."""
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

        for i, line in enumerate(self.lines, start=1):
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
                for j in range(i, min(i + 3, len(self.lines))):
                    next_line = self.lines[j].strip()
                    if next_line == "":
                        continue
                    if next_line.startswith("{"):
                        self.add_violation(
                            i,
                            "error",
                            "Brackets",
                            "Opening bracket '{' must be on the same line for control flow",
                        )
                    break

            # Check function definitions (but not declarations in headers)
            # Only flag if this looks like a definition (not in class body)
            if func_def.match(stripped) and "::" in stripped:
                # This is a method definition (has ClassName::)
                for j in range(i, min(i + 3, len(self.lines))):
                    next_line = self.lines[j].strip()
                    if next_line == "":
                        continue
                    if next_line.startswith("{"):
                        self.add_violation(
                            i,
                            "error",
                            "Brackets",
                            "Function opening bracket '{' must be on the same line",
                        )
                    break

    def check_code_density(self):
        """Check for code that is too dense and needs blank lines for breathing."""
        # Track consecutive non-blank lines inside function bodies
        consecutive = 0
        start_line = 0
        brace_depth = 0
        in_function = False

        for i, line in enumerate(self.lines, start=1):
            stripped = line.strip()

            # Track brace depth to detect function bodies
            open_braces = stripped.count("{")
            close_braces = stripped.count("}")

            if open_braces > 0 and brace_depth == 0:
                in_function = True

            brace_depth += open_braces - close_braces

            if brace_depth <= 0:
                # Function ended - check density before resetting
                if consecutive >= self.DENSITY_THRESHOLD:
                    self.add_violation(
                        start_line,
                        "warning",
                        "Code Breathing",
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
            if stripped.startswith("//") or stripped.startswith("/*") or stripped.startswith("#"):
                continue

            if stripped == "":
                if consecutive >= self.DENSITY_THRESHOLD:
                    self.add_violation(
                        start_line,
                        "warning",
                        "Code Breathing",
                        f"Dense code block ({consecutive} lines without blank line) - "
                        "consider adding blank lines to separate logical sections",
                    )
                consecutive = 0
            else:
                if consecutive == 0:
                    start_line = i
                consecutive += 1

        # Check at end
        if consecutive >= self.DENSITY_THRESHOLD:
            self.add_violation(
                start_line,
                "warning",
                "Code Breathing",
                f"Dense code block ({consecutive} lines without blank line) - "
                "consider adding blank lines to separate logical sections",
            )


def get_changed_files(base_ref: str) -> list[str]:
    """Get list of changed C++ files compared to base ref."""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "--diff-filter=ACMR", base_ref],
            capture_output=True,
            text=True,
            check=True,
        )
        files = result.stdout.strip().split("\n")
        # Filter to C++ files only
        cpp_ext = StyleChecker.CPP_EXTENSIONS
        return [f for f in files if f and Path(f).suffix in cpp_ext]
    except subprocess.CalledProcessError as e:
        print(f"Error getting changed files: {e}", file=sys.stderr)
        sys.exit(1)


def get_all_cpp_files(root: Path = Path(".")) -> list[str]:
    """Get all C++ files in the codebase, excluding skipped directories."""
    files = []
    for ext in StyleChecker.CPP_EXTENSIONS:
        for filepath in root.rglob(f"*{ext}"):
            # Check if in skip directory
            skip = False
            for part in filepath.parts:
                if part in StyleChecker.SKIP_DIRS:
                    skip = True
                    break
            if not skip:
                files.append(str(filepath))
    return sorted(files)


def format_violation(v: Violation, fmt: str) -> str:
    """Format a violation for output."""
    if fmt == "github":
        # GitHub Actions workflow command (creates annotations)
        return f"::{v.severity} file={v.filepath},line={v.line}::[{v.rule}] {v.message}"
    elif fmt == "json":
        # JSON format for programmatic processing
        return json.dumps(asdict(v))
    else:
        # Plain text
        return f"{v.filepath}:{v.line}: {v.severity}: [{v.rule}] {v.message}"


def output_json(violations: list[Violation]) -> str:
    """Output all violations as a JSON array."""
    return json.dumps([asdict(v) for v in violations], indent=2)


def main():
    parser = argparse.ArgumentParser(
        description="TuringDB C++ Code Style Checker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "files",
        nargs="*",
        help="C++ files to check",
    )
    parser.add_argument(
        "--diff",
        metavar="BASE_REF",
        help="Check files changed compared to BASE_REF (e.g., origin/main)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Check all C++ files in the codebase",
    )
    parser.add_argument(
        "--format",
        choices=["text", "github", "json"],
        default="text",
        help="Output format: text, github (annotations), json (default: text)",
    )

    args = parser.parse_args()

    # Determine files to check
    if args.all:
        files = get_all_cpp_files()
        if not files:
            print("No C++ files found.", file=sys.stderr)
            sys.exit(0)
        print(f"Checking {len(files)} files...", file=sys.stderr)
    elif args.diff:
        files = get_changed_files(args.diff)
        if not files:
            print("No C++ files changed.", file=sys.stderr)
            sys.exit(0)
    elif args.files:
        files = args.files
    else:
        parser.error("Either provide files, use --diff BASE_REF, or use --all")

    # Run checks
    all_violations: list[Violation] = []
    for filepath in files:
        checker = StyleChecker(filepath)
        if checker.load():
            violations = checker.check_all()
            all_violations.extend(violations)

    # Count errors and warnings
    errors = sum(1 for v in all_violations if v.severity == "error")
    warnings = sum(1 for v in all_violations if v.severity == "warning")

    # Output results
    if args.format == "json":
        # Output as single JSON array
        print(output_json(all_violations))
    else:
        for v in all_violations:
            print(format_violation(v, args.format))

        # Summary for text format
        if args.format == "text" and (errors or warnings):
            print(f"\nTotal: {errors} error(s), {warnings} warning(s)")

    # Exit with error code if there are errors
    sys.exit(1 if errors > 0 else 0)


if __name__ == "__main__":
    main()
