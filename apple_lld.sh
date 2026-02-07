#!/usr/bin/env bash
#
# apple_ldd - list all dynamic libraries loaded by a Mach-O binary,
# recursively resolving transitive dependencies (like ldd on Linux).
#
# Usage:  ./build/apple_ldd <binary>
#
# Each line shows the resolved library path.  System libraries living
# in the dyld shared cache (macOS 11+) are annotated accordingly.
# Unresolvable @rpath references are shown as "not found".

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $(basename "$0") <binary>" >&2
    exit 1
fi

BINARY="$1"

if [ ! -f "$BINARY" ]; then
    echo "Error: '$BINARY' not found" >&2
    exit 1
fi

if ! file "$BINARY" | grep -q "Mach-O"; then
    echo "Error: '$BINARY' is not a Mach-O binary" >&2
    exit 1
fi

# Plain list for tracking visited libraries (bash 3.2 compatible)
SEEN=""

is_seen() {
    echo "$SEEN" | grep -qFx "$1"
}

mark_seen() {
    SEEN="${SEEN}${1}
"
}

# Check whether a path refers to a system library that lives in the
# dyld shared cache (macOS 11+ removed individual .dylib files from
# /usr/lib and /System/Library).
is_cached_system_lib() {
    local path="$1"
    case "$path" in
        /usr/lib/*|/System/Library/*)
            [ ! -f "$path" ] && return 0
            ;;
    esac
    return 1
}

# Resolve @rpath, @loader_path, @executable_path references.
# $1 = raw dylib install name
# $2 = path of the binary/dylib that references it
resolve_path() {
    local name="$1"
    local referrer="$2"
    local referrer_dir
    referrer_dir="$(dirname "$referrer")"

    case "$name" in
        @executable_path/*)
            local exe_dir
            exe_dir="$(cd "$(dirname "$BINARY")" && pwd)"
            echo "${exe_dir}/${name#@executable_path/}"
            ;;
        @loader_path/*)
            echo "${referrer_dir}/${name#@loader_path/}"
            ;;
        @rpath/*)
            local tail="${name#@rpath/}"
            # Collect rpaths from the referrer
            local rpaths
            rpaths=$(otool -l "$referrer" 2>/dev/null \
                | awk '/cmd LC_RPATH/{found=1} found && /path /{print $2; found=0}')
            local rp
            for rp in $rpaths; do
                case "$rp" in
                    @loader_path/*)
                        rp="${referrer_dir}/${rp#@loader_path/}"
                        ;;
                    @executable_path/*)
                        local exe_dir2
                        exe_dir2="$(cd "$(dirname "$BINARY")" && pwd)"
                        rp="${exe_dir2}/${rp#@executable_path/}"
                        ;;
                esac
                local candidate="${rp}/${tail}"
                if [ -f "$candidate" ]; then
                    echo "$candidate"
                    return
                fi
            done
            # Fallback: common system locations (may be in shared cache)
            local fb
            for fb in /usr/lib /usr/local/lib; do
                local sys_candidate="${fb}/${tail}"
                if [ -f "$sys_candidate" ] || is_cached_system_lib "$sys_candidate"; then
                    echo "$sys_candidate"
                    return
                fi
            done
            # Unresolved
            echo "$name"
            ;;
        *)
            echo "$name"
            ;;
    esac
}

# Recursively walk the dependency tree.
# $1 = library or binary path
# $2 = indent depth
walk() {
    local target="$1"
    local depth="$2"
    local indent=""
    local i

    for ((i = 0; i < depth; i++)); do
        indent+="  "
    done

    # Resolve to a real path for dedup
    local real
    real="$(realpath "$target" 2>/dev/null || echo "$target")"

    if is_seen "$real"; then
        return
    fi
    mark_seen "$real"

    # Handle system dyld shared cache libraries
    if is_cached_system_lib "$real"; then
        echo "${indent}${real} (dyld shared cache)"
        return
    fi

    if [ ! -f "$real" ]; then
        # Unresolvable @rpath or truly missing
        echo "${indent}${target} => not found"
        return
    fi

    if [ "$depth" -gt 0 ]; then
        echo "${indent}${real}"
    fi

    # Get direct dependencies (skip first line = the binary itself)
    local deps
    deps=$(otool -L "$real" 2>/dev/null | tail -n +2 \
        | sed 's/^[[:space:]]*//' | cut -d' ' -f1)

    local dep
    for dep in $deps; do
        local resolved
        resolved="$(resolve_path "$dep" "$real")"
        resolved="$(realpath "$resolved" 2>/dev/null || echo "$resolved")"

        if is_seen "$resolved"; then
            continue
        fi

        walk "$resolved" $((depth + 1))
    done
}

echo "$BINARY"
walk "$BINARY" 0
