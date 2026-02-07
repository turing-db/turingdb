#!/usr/bin/env bash
#
# Verify that all built executables embed LLVM's libunwind and libc++abi
# symbols statically, and do not rely on Apple's system versions.
#
# With -Wl,-flat_namespace the statically linked symbols take precedence
# over any dynamically loaded copies.  This script checks that:
#   1. Key _Unwind_* symbols are defined (T) in the binary, not undefined (U).
#   2. Key __cxa_* / __gxx_personality symbols are defined in the binary.
#   3. The binary does not directly link against Apple's libunwind.dylib.
#
# Usage:  ./build/check_no_apple_unwind.sh [build-dir]
#         Defaults to <repo-root>/build.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${1:-${REPO_ROOT}/build}"

# Symbols that must be defined locally (from static libc++abi + libunwind)
UNWIND_SYMBOLS=(
    __Unwind_RaiseException
    __Unwind_GetIP
    __Unwind_SetIP
    __Unwind_SetGR
    __Unwind_GetLanguageSpecificData
    __Unwind_GetRegionStart
)

CXXABI_SYMBOLS=(
    ___cxa_throw
    ___cxa_begin_catch
    ___gxx_personality_v0
)

PASS=0
FAIL=0
CHECKED=0

# Collect all Mach-O executables, skipping:
#   - CMakeFiles/          (cmake internal tools)
#   - external/            (third-party builds)
#   - lib.macosx-*/        (stale Python wheel artifacts)
while IFS= read -r binary; do
    CHECKED=$((CHECKED + 1))
    errors=""

    # --- Check 1: _Unwind_* symbols are defined (T), not undefined (U) ---
    sym_table=$(nm -g "$binary" 2>/dev/null || true)

    for sym in "${UNWIND_SYMBOLS[@]}"; do
        sym_line=$(echo "$sym_table" | grep " ${sym}$" || true)
        if [ -z "$sym_line" ]; then
            errors+="  MISSING: ${sym} (not found at all)\n"
        elif echo "$sym_line" | grep -q " U "; then
            errors+="  UNDEFINED: ${sym} (dynamically resolved)\n"
        fi
    done

    # --- Check 2: __cxa_* / personality symbols are defined (T) ---
    for sym in "${CXXABI_SYMBOLS[@]}"; do
        sym_line=$(echo "$sym_table" | grep " ${sym}$" || true)
        if [ -z "$sym_line" ]; then
            errors+="  MISSING: ${sym} (not found at all)\n"
        elif echo "$sym_line" | grep -q " U "; then
            errors+="  UNDEFINED: ${sym} (dynamically resolved)\n"
        fi
    done

    # --- Check 3: no direct link to Apple's libunwind.dylib ---
    deps=$(otool -L "$binary" 2>/dev/null || true)
    if echo "$deps" | grep -q "libunwind\.dylib"; then
        errors+="  LINKED: directly links against Apple's libunwind.dylib\n"
    fi

    if [ -n "$errors" ]; then
        FAIL=$((FAIL + 1))
        echo "FAIL: $binary"
        echo -e "$errors"
    else
        PASS=$((PASS + 1))
    fi
done < <(
    find "$BUILD_DIR" -type f -perm +111 \
        ! -name "*.dylib" ! -name "*.sh" ! -name "*.py" \
        ! -name "*.cmake" ! -name "*.rb" ! -name "*.pl" \
        ! -path "*/CMakeFiles/*" \
        ! -path "*/external/*" \
        ! -path "*/lib.macosx-*" \
        -exec sh -c 'file "$1" | grep -q "Mach-O.*executable"' _ {} \; \
        -print 2>/dev/null
)

echo "---"
echo "Checked: $CHECKED   Passed: $PASS   Failed: $FAIL"

if [ "$FAIL" -gt 0 ]; then
    echo "ERROR: $FAIL binaries still reference Apple's unwind/cxxabi symbols."
    exit 1
fi

echo "OK: all binaries embed LLVM's libunwind and libc++abi statically."
exit 0
