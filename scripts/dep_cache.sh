#!/usr/bin/env bash
#
# Restore or publish the external/dependencies build cache on a self-hosted
# runner whose cache base is shared by several runner instances on one host.
#
# This replaces corca-ai/local-cache for the macOS minis, whose runner
# instances all share one on-disk base (/Users/m1/actions-local-cache). That
# action symlinks external/dependencies straight into the shared base on a hit
# and populates the base with a non-atomic mv on a miss, with no locking. A job
# could therefore symlink into -- and link against -- a half-published tree and
# fail with e.g. a missing libopenblas.a.
#
# Both modes must be invoked under scripts/with_host_lock.py so that restore and
# publish never overlap across instances. Publishing is also staged through a
# private temp dir and made visible with a single atomic rename, so a crashed
# publish can never leave a partial cache entry that a later restore mistakes
# for a complete one.
#
# Publishing additionally rewrites the absolute paths the dependency build bakes
# into its installed CMake/pkg-config files (e.g. faiss's BLAS_LIBRARIES, the
# LLVM/MLIR configs). Those paths point into the building instance's own checkout
# -- a transient path that other instances git-clean between jobs -- so a consumer
# on a different instance would link against e.g. a libopenblas.a that a concurrent
# checkout deletes mid-build. We rewrite them to the canonical, never-cleaned cache
# location so any instance can consume the entry.
#
# Usage: dep_cache.sh restore|publish BASE KEY
set -euo pipefail

mode="${1:?usage: dep_cache.sh restore|publish BASE KEY}"
base="${2:?missing BASE}"
key="${3:?missing KEY}"

entry="$base/$key/dependencies"

case "$mode" in
restore)
    mkdir -p "$base"

    if [ -d "$entry" ]; then
        rm -rf external/dependencies
        ln -s "$entry" external/dependencies
        echo "cache-hit=true" >> "$GITHUB_OUTPUT"
        echo "Dependency cache hit: $key"
    else
        echo "cache-hit=false" >> "$GITHUB_OUTPUT"
        echo "Dependency cache miss: $key"
    fi
    ;;

publish)
    # On a hit external/dependencies is already a symlink into the cache, so
    # there is nothing to publish. On a miss it is the real tree just built.
    if [ -L external/dependencies ]; then
        echo "Dependencies are symlinked from cache; nothing to publish."
        exit 0
    fi

    if [ -d "$entry" ]; then
        echo "Cache entry already present; another job populated it first."
        exit 0
    fi

    # The build baked this checkout's external/dependencies path into the
    # installed configs. Capture both its logical and physical forms (macOS
    # firmlinks /Users) before the move so we can rewrite every baked occurrence.
    built_logical="$(pwd)/external/dependencies"
    built_physical="$(cd external/dependencies && pwd -P)"

    mkdir -p "$base/$key"

    # Move the built tree next to its final location, then reveal it with a
    # single same-filesystem rename so readers only ever see a complete entry.
    staging="$base/$key/.dependencies.staging.$$"
    rm -rf "$staging"
    mv external/dependencies "$staging"
    mv "$staging" "$entry"

    # Rewrite the baked per-checkout paths to the canonical cache path in the
    # CMake/pkg-config files a consumer's find_package() reads. Binaries are
    # skipped (grep -I); static archives carry no such dependency. The scan is
    # scoped to config locations so it stays fast on the large dependency tree.
    scan_roots=()
    for dir in "$entry/lib" "$entry/share" "$entry/build/llvm-project/lib/cmake"; do
        if [ -d "$dir" ]; then
            scan_roots+=("$dir")
        fi
    done

    if [ "${#scan_roots[@]}" -gt 0 ]; then
        for old in "$built_logical" "$built_physical"; do
            if [ "$old" = "$entry" ]; then
                continue
            fi
            while IFS= read -r config; do
                LC_ALL=C sed -i '' "s|$old|$entry|g" "$config"
            done < <(grep -rIlF -- "$old" "${scan_roots[@]}" 2>/dev/null || true)
        done
    fi

    # Re-link so the steps after publish keep reading external/dependencies.
    ln -s "$entry" external/dependencies
    echo "Published dependency cache: $key"
    ;;

*)
    echo "usage: dep_cache.sh restore|publish BASE KEY" >&2
    exit 2
    ;;
esac
