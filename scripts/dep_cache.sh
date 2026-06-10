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

    mkdir -p "$base/$key"

    # Move the built tree next to its final location, then reveal it with a
    # single same-filesystem rename so readers only ever see a complete entry.
    staging="$base/$key/.dependencies.staging.$$"
    rm -rf "$staging"
    mv external/dependencies "$staging"
    mv "$staging" "$entry"

    # Re-link so the steps after publish keep reading external/dependencies.
    ln -s "$entry" external/dependencies
    echo "Published dependency cache: $key"
    ;;

*)
    echo "usage: dep_cache.sh restore|publish BASE KEY" >&2
    exit 2
    ;;
esac
