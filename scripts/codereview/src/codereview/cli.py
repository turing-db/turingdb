"""Command-line interface for the code review tool."""

import argparse
import sys

from .checker import StyleChecker
from .clang_tidy import ClangTidyConstChecker
from .utils import get_changed_files, get_all_cpp_files
from .output import format_violation, output_json
from .models import Violation


def main():
    """Main entry point for the code review CLI."""
    parser = argparse.ArgumentParser(
        description="TuringDB C++ Code Style Checker",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    codereview file1.cpp file2.h        # Check specific files
    codereview --diff origin/main       # Check changed files vs base
    codereview --all                    # Check all C++ files
    codereview --format github          # GitHub Actions annotations
""",
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
    parser.add_argument(
        "--skip-check",
        action="append",
        default=[],
        metavar="NAME",
        help="Skip a specific check by name (can be specified multiple times)",
    )
    parser.add_argument(
        "--only-check",
        action="append",
        default=[],
        metavar="NAME",
        help="Only run specific checks (can be specified multiple times)",
    )
    parser.add_argument(
        "--clang-tidy-const",
        action="store_true",
        help="Use clang-tidy misc-const-correctness instead of regex-based const check",
    )
    parser.add_argument(
        "--build-dir",
        default="build",
        help="Build directory with compile_commands.json (default: build)",
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

    # Configure skip/only checks
    skip_checks = set(args.skip_check)
    only_checks = set(args.only_check) if args.only_check else None

    # Initialize clang-tidy const checker if requested
    clang_tidy_checker = None
    use_clang_tidy_const = args.clang_tidy_const
    if use_clang_tidy_const:
        clang_tidy_checker = ClangTidyConstChecker(args.build_dir)
        if not clang_tidy_checker.is_available():
            print(
                "Warning: clang-tidy with misc-const-correctness not available. "
                "Falling back to regex-based const check.",
                file=sys.stderr,
            )
            use_clang_tidy_const = False
        else:
            print(
                f"Using clang-tidy ({clang_tidy_checker.clang_tidy_path}) for const checking",
                file=sys.stderr,
            )
            # Skip the regex-based const check
            skip_checks.add("Const")

    # Run checks
    all_violations: list[Violation] = []
    for filepath in files:
        checker = StyleChecker(filepath)
        if checker.load():
            violations = checker.check_all(skip_checks, only_checks)
            all_violations.extend(violations)

    # Run clang-tidy const check if enabled
    if use_clang_tidy_const and clang_tidy_checker:
        clang_tidy_violations = clang_tidy_checker.check_files(files)
        all_violations.extend(clang_tidy_violations)

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
