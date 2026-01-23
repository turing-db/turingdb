"""Check that includes follow the proper order."""

import re
from pathlib import Path

from . import register_check
from .base import BaseCheck, CheckContext
from ..constants import EXTERNAL_LIBS, STD_HEADERS, CPP_TO_C_HEADERS


@register_check
class IncludeOrderCheck(BaseCheck):
    """Check that includes follow the proper order.

    Order should be:
    1. Current class header (for .cpp files) - followed by blank line
    2. Standard library headers (<...>)
    3. External library headers (<spdlog/...>, etc.)
    4. Project headers ("...")
    """

    name = "Includes"
    severity = "error"

    # Tool files that don't need to include their own header first
    TOOL_FILES = {"main", "TuringImport", "TuringDBTool", "S3TestCli", "plan2"}

    def run(self, context: CheckContext) -> None:
        # Parse all includes
        include_pattern = re.compile(r'^\s*#include\s+([<"])([^>"]+)[>"]')

        includes: list[tuple[int, str, str, str]] = []  # (line, type, path, full)
        first_include_line = 0
        in_include_block = False

        for i, line in enumerate(context.lines, start=1):
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
        filename = context.stem  # e.g., "Graph" from "Graph.cpp"
        if (
            not context.is_header
            and self._has_class_implementation(context)
            and filename not in self.TOOL_FILES
        ):
            first_inc = includes[0]
            if first_inc[1] != "blank":
                first_path = Path(first_inc[2]).stem
                if first_path.lower() != filename.lower():
                    # Only flag if the expected header file actually exists
                    expected_header = context.path.parent / f"{filename}.h"
                    if expected_header.exists():
                        self.add_violation(
                            first_inc[0],
                            f'First include should be "{filename}.h" (current class header)',
                        )
                else:
                    # Check for blank line after first include
                    if len(includes) > 1 and includes[1][1] != "blank":
                        self.add_violation(
                            first_inc[0],
                            "Current class header should be followed by a blank line",
                        )

        # Check 2: Flag C++ wrapper headers (cstdint, cstdlib, etc.)
        for line_num, inc_type, path, full in includes:
            if inc_type == "blank":
                continue
            if path in CPP_TO_C_HEADERS:
                preferred = CPP_TO_C_HEADERS[path]
                self.add_violation(
                    line_num,
                    f"Use <{preferred}> instead of <{path}> (prefer C-style headers)",
                )

        # Check 3: Verify include order (standard -> external -> project)
        # Filter out blank markers for order checking
        real_includes = [(i, t, p, f) for i, t, p, f in includes if t != "blank"]

        # Skip the first include in .cpp files if it's the class header
        start_idx = 0
        if not context.is_header and real_includes and self._has_class_implementation(context):
            filename = context.stem
            first_path = Path(real_includes[0][2]).stem
            if first_path.lower() == filename.lower():
                start_idx = 1

        # In header files, skip parent class headers that appear first
        # (e.g., #include "Iterator.h" at the top of GetEdgesIterator.h)
        parent_headers = set()
        if context.is_header:
            parent_headers = self._get_parent_class_headers(context)

        # Track the "phase" of includes we're in
        # 0 = standard, 1 = external, 2 = project
        phase = 0
        phase_names = ["standard library", "external library", "project"]

        for line_num, inc_type, path, full in real_includes[start_idx:]:
            if inc_type == "standard":
                if phase > 0:
                    self.add_violation(
                        line_num,
                        f"Standard library include <{path}> should come before "
                        f"{phase_names[phase]} includes",
                    )
            elif inc_type == "external":
                if phase > 1:
                    self.add_violation(
                        line_num,
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
        if first_part in EXTERNAL_LIBS:
            return "external"

        # Check if it's a standard header
        if path in STD_HEADERS or first_part in STD_HEADERS:
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

    def _has_class_implementation(self, context: CheckContext) -> bool:
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
        for line in context.lines:
            if method_pattern.match(line.strip()):
                return True
        return False

    def _get_parent_class_headers(self, context: CheckContext) -> set[str]:
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

        for line in context.lines:
            match = inherit_pattern.match(line.strip())
            if match:
                inheritance_list = match.group(1)
                for parent_match in parent_pattern.finditer(inheritance_list):
                    parent_name = parent_match.group(1)
                    parent_headers.add(parent_name.lower())

        return parent_headers
