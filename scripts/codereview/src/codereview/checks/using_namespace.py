"""Check for forbidden 'using namespace' directives."""

import re

from . import register_check
from .base import BaseCheck, CheckContext


@register_check
class UsingNamespaceCheck(BaseCheck):
    """Check for forbidden 'using namespace' directives."""

    name = "Namespaces"
    severity = "error"

    def run(self, context: CheckContext) -> None:
        pattern = re.compile(r"^\s*using\s+namespace\s+(\w+)\s*;")

        for i, line in enumerate(context.lines, start=1):
            match = pattern.match(line)
            if match:
                namespace = match.group(1)
                if namespace == "std":
                    # Never allowed anywhere
                    self.add_violation(
                        i,
                        "'using namespace std' is never allowed",
                    )
                elif context.is_header:
                    # Any 'using namespace' forbidden in headers
                    self.add_violation(
                        i,
                        f"'using namespace {namespace}' not allowed in header files",
                    )
