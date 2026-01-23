"""Check registry and auto-discovery."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .base import BaseCheck

# Global registry of all checks
_registry: dict[str, type[BaseCheck]] = {}


def register_check(cls: type[BaseCheck]) -> type[BaseCheck]:
    """Decorator to register a check class.

    Usage:
        @register_check
        class MyCheck(BaseCheck):
            name = "MyCheck"
            ...
    """
    if not hasattr(cls, "name") or cls.name == "UnnamedCheck":
        raise ValueError(f"Check class {cls.__name__} must define a 'name' attribute")
    _registry[cls.name] = cls
    return cls


def get_all_checks() -> list[type[BaseCheck]]:
    """Return all registered check classes."""
    return list(_registry.values())


def get_check(name: str) -> type[BaseCheck] | None:
    """Get a specific check by name."""
    return _registry.get(name)


def get_enabled_checks(
    skip_checks: set[str] | None = None,
    only_checks: set[str] | None = None,
) -> list[type[BaseCheck]]:
    """Get checks filtered by enable/disable lists.

    Args:
        skip_checks: Set of check names to skip
        only_checks: If provided, only run these checks
    """
    skip_checks = skip_checks or set()
    checks = []

    for name, check_cls in _registry.items():
        if only_checks and name not in only_checks:
            continue
        if name in skip_checks:
            continue
        if check_cls.enabled_by_default or (only_checks and name in only_checks):
            checks.append(check_cls)

    return checks


# Import all check modules to trigger registration
# These imports must come after the registry functions are defined
from . import (  # noqa: E402, F401
    bracket_positioning,
    code_density,
    consecutive_blanks,
    constructor_brackets,
    destructor_brackets,
    include_order,
    missing_const,
    nontrivial_return_by_value,
    pointer_member_init,
    stl_return_by_value,
    using_namespace,
)

__all__ = [
    "register_check",
    "get_all_checks",
    "get_check",
    "get_enabled_checks",
]
