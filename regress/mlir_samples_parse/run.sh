#!/bin/bash
#
# Parse every MLIR sample under samples/mlir with the `mlir` sample tool and
# check the outcome. The file list is discovered at runtime, so dropping a new
# .mlir sample in is covered automatically - there is nothing to edit here.
#
# Convention: a sample named invalid_*.mlir is a negative fixture and must be
# rejected; every other sample must parse cleanly.

set -u

# The mlir sample tool is installed by turing_sample to $TURING_HOME/samples/mlir.
# The sample sources are not installed, so they are read from the source tree,
# which run_regress exports as $SOURCE_DIR. Both are overridable so the test can
# be run standalone (outside the run_regress harness) for debugging.
MLIR_TOOL="${MLIR_TOOL:-${TURING_HOME:-}/samples/mlir/mlir}"
SAMPLES_DIR="${MLIR_SAMPLES_DIR:-${SOURCE_DIR:-}/samples/mlir}"

if [[ ! -x "$MLIR_TOOL" ]]; then
    echo "error: mlir sample tool not found at '$MLIR_TOOL'" >&2
    exit 1
fi
if [[ ! -d "$SAMPLES_DIR" ]]; then
    echo "error: sample directory not found at '$SAMPLES_DIR'" >&2
    exit 1
fi

shopt -s nullglob
samples=("$SAMPLES_DIR"/*.mlir)
if [[ ${#samples[@]} -eq 0 ]]; then
    echo "error: no .mlir samples found in '$SAMPLES_DIR'" >&2
    exit 1
fi

declare -i checked=0
declare -a failures=()

for f in "${samples[@]}"; do
    name=$(basename "$f")
    checked+=1

    if "$MLIR_TOOL" -f "$f" >/dev/null 2>&1; then
        parsed=1
    else
        parsed=0
    fi

    if [[ "$name" == invalid_* ]]; then
        # Negative fixture: the tool must reject it.
        if [[ $parsed -eq 1 ]]; then
            echo "FAIL  $name (parsed, but an invalid_* fixture must be rejected)"
            failures+=("$name")
        else
            echo "ok    $name (rejected as expected)"
        fi
    else
        # Every other sample must parse.
        if [[ $parsed -eq 1 ]]; then
            echo "ok    $name"
        else
            echo "FAIL  $name (did not parse):"
            "$MLIR_TOOL" -f "$f" 2>&1 | sed 's/^/        /' | head -5
            failures+=("$name")
        fi
    fi
done

echo
echo "=== mlir_samples_parse: checked $checked sample(s), ${#failures[@]} failure(s) ==="
# Guard the expansion: under `set -u`, macOS's bash 3.2 treats "${failures[@]}"
# on an empty array as an unbound variable, so only expand it when non-empty.
if [[ ${#failures[@]} -gt 0 ]]; then
    for t in "${failures[@]}"; do echo "  - $t"; done
fi

# Non-zero exit on any failure.
[[ ${#failures[@]} -eq 0 ]]
