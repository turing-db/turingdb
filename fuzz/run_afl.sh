#!/bin/bash
# ==========================================================================
# TuringDB AFL++ Fuzzing
# ==========================================================================
# Builds instrumented harnesses with afl-g++ and runs AFL++ on them.
#
# Usage: ./fuzz/run_afl.sh [OPTIONS] [HARNESS...]
#
# Harnesses: --cypher  --csv  --gml  --http  (default: all)
#
# Options:
#   --time SECS      Time per harness (default: 300)
#   --nostop         Run until Ctrl+C (ignores --time)
#   --build-only     Build harnesses but don't run AFL
#   --skip-build     Skip building, use existing harnesses
# ==========================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FUZZ_DIR="$SCRIPT_DIR/fuzz"
BUILD_AFL="$SCRIPT_DIR/build_afl"
RESULTS_DIR="$SCRIPT_DIR/fuzz_results/afl_findings"

# Defaults
AFL_TIME=300
BUILD_ONLY=0
SKIP_BUILD=0
NOSTOP=0
RUN_CYPHER=0
RUN_CSV=0
RUN_GML=0
RUN_HTTP=0
ANY_SELECTED=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --cypher)     RUN_CYPHER=1; ANY_SELECTED=1; shift ;;
        --csv)        RUN_CSV=1; ANY_SELECTED=1; shift ;;
        --gml)        RUN_GML=1; ANY_SELECTED=1; shift ;;
        --http)       RUN_HTTP=1; ANY_SELECTED=1; shift ;;
        --time)       AFL_TIME="$2"; shift 2 ;;
        --nostop)     NOSTOP=1; shift ;;
        --build-only) BUILD_ONLY=1; shift ;;
        --skip-build) SKIP_BUILD=1; shift ;;
        -h|--help)
            echo "Usage: $0 [--cypher] [--csv] [--gml] [--http] [--time SECS] [--nostop] [--build-only] [--skip-build]"
            echo ""
            echo "  --cypher       Fuzz the Cypher parser"
            echo "  --csv          Fuzz the CSV parser"
            echo "  --gml          Fuzz the GML importer"
            echo "  --http         Fuzz the HTTP parser"
            echo "  --time SECS    Time per harness (default: 300)"
            echo "  --nostop       Run forever until Ctrl+C (ignores --time)"
            echo "  --build-only   Build but don't run"
            echo "  --skip-build   Use existing build"
            echo ""
            echo "If no harness is specified, all are run."
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Default: run all
if [[ $ANY_SELECTED -eq 0 ]]; then
    RUN_CYPHER=1; RUN_CSV=1; RUN_GML=1; RUN_HTTP=1
fi

# Build harness list
HARNESSES=()
CORPORA=()
if [[ $RUN_CYPHER -eq 1 ]]; then HARNESSES+=(fuzz_query_engine); CORPORA+=("$FUZZ_DIR/corpus/cypher"); fi
if [[ $RUN_CSV -eq 1 ]];    then HARNESSES+=(fuzz_csv_parser);    CORPORA+=("$FUZZ_DIR/corpus/csv"); fi
if [[ $RUN_GML -eq 1 ]];    then HARNESSES+=(fuzz_gml_importer);  CORPORA+=("$FUZZ_DIR/corpus/gml"); fi
if [[ $RUN_HTTP -eq 1 ]];   then HARNESSES+=(fuzz_http_parser);   CORPORA+=("$FUZZ_DIR/corpus/http"); fi

# =========================================================================
# Build
# =========================================================================
if [[ $SKIP_BUILD -eq 0 ]]; then
    AFL_GCC=$(command -v afl-gcc 2>/dev/null)
    AFL_GXX=$(command -v afl-g++ 2>/dev/null)

    if [[ -z "$AFL_GXX" ]]; then
        echo "ERROR: afl-g++ not found. Install afl++: sudo apt-get install -y afl++"
        exit 1
    fi

    mkdir -p "$BUILD_AFL"
    cd "$BUILD_AFL"

    # Step 1: Build everything with normal gcc.
    echo "Step 1/3: Building with gcc..."
    cmake "$SCRIPT_DIR" \
        -DAFL_CXX_FLAGS="-Wno-error=maybe-uninitialized" \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        2>&1 | tail -5
    make -j8 "${HARNESSES[@]}" 2>&1 | tail -10
    if [[ $? -ne 0 ]]; then
        echo "ERROR: gcc build failed."
        exit 1
    fi

    # Step 2: Recompile query/, net/, server/ and fuzz/ with afl-g++.
    # This instruments only the attack surface code for accurate coverage.
    echo "Step 2/3: Recompiling query/, net/, server/, io/, import/, fuzz/ with afl-g++..."
    AFL_GXX_PATH="$AFL_GXX" python3 -c "
import json, subprocess, sys, os

afl_gxx = os.environ['AFL_GXX_PATH']

with open('compile_commands.json') as f:
    cmds = json.load(f)

targets = ('/query/', '/net/', '/server/', '/fuzz/', '/io/', '/import/')
count = 0
failed = 0
skipped = 0

for entry in cmds:
    src = entry['file']
    if not any(t in src for t in targets):
        continue
    if not os.path.exists(src):
        skipped += 1
        continue
    cmd = entry['command']
    directory = entry['directory']
    parts = cmd.split(' ', 1)
    new_cmd = afl_gxx + ' ' + parts[1]
    rc = subprocess.run(new_cmd, shell=True, cwd=directory,
                        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    count += 1
    if rc.returncode != 0:
        if failed < 3:
            print(f'  FAIL: {basename}', file=sys.stderr)
            print(rc.stderr.decode()[-300:], file=sys.stderr)
        failed += 1

print(f'  Recompiled {count - failed}/{count} files ({skipped} skipped, {failed} failures)')
if failed > count // 4:
    print('ERROR: Too many failures', file=sys.stderr)
    sys.exit(1)
"
    if [[ $? -ne 0 ]]; then
        echo "ERROR: afl-g++ recompilation failed."
        exit 1
    fi

    # Step 3: Re-archive .a files and re-link harnesses with afl-g++.
    echo "Step 3/3: Re-archiving and re-linking with afl-g++..."
    # Delete .a files for instrumented libraries so make re-archives them.
    find . -path "*/query/*" -name "*.a" -delete 2>/dev/null
    find . -path "*/net/*" -name "*.a" -delete 2>/dev/null
    find . -path "*/server/*" -name "*.a" -delete 2>/dev/null
    find . -path "*/io/*" -name "*.a" -delete 2>/dev/null
    find . -path "*/import/*" -name "*.a" -delete 2>/dev/null
    # Let make re-archive (it uses `ar`, no compiler needed for that).
    make -j8 "${HARNESSES[@]}" 2>&1 | tail -10

    # Now re-link each harness with afl-g++ so the AFL runtime is included.
    # Read cmake's link.txt and replace the linker with afl-g++.
    for h in "${HARNESSES[@]}"; do
        LINK_FILE="fuzz/CMakeFiles/${h}.dir/link.txt"
        if [[ ! -f "$LINK_FILE" ]]; then
            echo "  WARN: $LINK_FILE not found, skipping $h"
            continue
        fi
        LINK_CMD=$(cat "$LINK_FILE")
        # Replace /usr/bin/c++ or /usr/bin/g++ with afl-g++
        LINK_CMD=$(echo "$LINK_CMD" | sed "s|/usr/bin/c++|$AFL_GXX|g; s|/usr/bin/g++|$AFL_GXX|g")
        echo "  Linking $h with afl-g++..."
        (cd fuzz && eval "$LINK_CMD") 2>&1
        if [[ $? -ne 0 ]]; then
            echo "  ERROR: Failed to link $h"
        fi
    done
    if [[ $? -ne 0 ]]; then
        echo "ERROR: Re-link failed."
        exit 1
    fi

    echo "Build OK. Only query/, net/, server/ are instrumented."
    cd "$SCRIPT_DIR"
fi

if [[ $BUILD_ONLY -eq 1 ]]; then
    echo "Build-only mode, not running AFL."
    exit 0
fi

# =========================================================================
# Run AFL++
# =========================================================================
mkdir -p "$RESULTS_DIR"
echo core | sudo tee /proc/sys/kernel/core_pattern >/dev/null 2>&1 || true

restore_terminal() {
    stty sane 2>/dev/null || true
    tput rmcup 2>/dev/null || true
    printf '\033[?25h\033[0m' 2>/dev/null || true
}

ROUND=0
STOP_REQUESTED=0
AFL_PID=""

cleanup_and_exit() {
    STOP_REQUESTED=1
    echo ""
    echo "Stopping..."
    if [[ -n "$AFL_PID" ]]; then
        kill "$AFL_PID" 2>/dev/null
        wait "$AFL_PID" 2>/dev/null
    fi
    restore_terminal
    echo "Done. Results in: $RESULTS_DIR"
    exit 0
}

trap cleanup_and_exit INT

run_one() {
    local HARNESS="$1"
    local CORPUS="$2"
    local BIN="$BUILD_AFL/fuzz/$HARNESS"
    local DURATION="$AFL_TIME"

    if [[ ! -x "$BIN" ]]; then
        echo "SKIP: $HARNESS (not found at $BIN)"
        return
    fi

    local FINDINGS="$RESULTS_DIR/$HARNESS"

    # Dictionary for token-aware fuzzing (major coverage improvement for parsers).
    local DICT_FLAG=""
    if [[ "$HARNESS" == "fuzz_query_engine" ]] && [[ -f "$FUZZ_DIR/cypher.dict" ]]; then
        DICT_FLAG="-x $FUZZ_DIR/cypher.dict"
    elif [[ "$HARNESS" == "fuzz_http_parser" ]] && [[ -f "$FUZZ_DIR/http.dict" ]]; then
        DICT_FLAG="-x $FUZZ_DIR/http.dict"
    fi

    # On first run, start fresh. On subsequent rounds, resume (AFL++ uses -i-).
    local INPUT_FLAG="-i"
    local INPUT_DIR="$CORPUS"
    if [[ -d "$FINDINGS/default/queue" ]] && [[ -n "$(ls -A "$FINDINGS/default/queue" 2>/dev/null)" ]]; then
        INPUT_FLAG="-i-"
        INPUT_DIR=""
    else
        rm -rf "$FINDINGS"
    fi
    mkdir -p "$FINDINGS"

    echo ""
    echo "================================================================"
    if [[ $NOSTOP -eq 1 ]]; then
        echo "  AFL++: $HARNESS (round $ROUND, ${DURATION}s, Ctrl+C to stop)"
    else
        echo "  AFL++: $HARNESS (${DURATION}s)"
    fi
    echo "================================================================"
    echo ""

    # In --nostop mode with a single harness, run without time limit.
    # Otherwise use timeout + -V for timed runs.
    local USE_TIMEOUT=1
    if [[ $NOSTOP -eq 1 ]] && [[ ${#HARNESSES[@]} -eq 1 ]]; then
        USE_TIMEOUT=0
    fi

    if [[ $USE_TIMEOUT -eq 1 ]]; then
        if [[ -n "$INPUT_DIR" ]]; then
            AFL_SKIP_CPUFREQ=1 timeout -k 10 "${DURATION}" afl-fuzz \
                "$INPUT_FLAG" "$INPUT_DIR" \
                -o "$FINDINGS" \
                -m none \
                -t 100 \
                -V "${DURATION}" \
                -- "$BIN" &
        else
            AFL_SKIP_CPUFREQ=1 timeout -k 10 "${DURATION}" afl-fuzz \
                "$INPUT_FLAG" \
                -o "$FINDINGS" \
                -m none \
                -t 100 \
                $DICT_FLAG \
                -- "$BIN" &
        fi
    else
        if [[ -n "$INPUT_DIR" ]]; then
            AFL_SKIP_CPUFREQ=1 afl-fuzz \
                "$INPUT_FLAG" "$INPUT_DIR" \
                -o "$FINDINGS" \
                -m none \
                -t 100 \
                $DICT_FLAG \
                -- "$BIN" &
        else
            AFL_SKIP_CPUFREQ=1 afl-fuzz \
                "$INPUT_FLAG" \
                -o "$FINDINGS" \
                -m none \
                -t 100 \
                $DICT_FLAG \
                -- "$BIN" &
        fi
    fi
    AFL_PID=$!
    wait "$AFL_PID" 2>/dev/null || true
    AFL_PID=""

    restore_terminal

    local CRASHES=$(find "$FINDINGS" -path "*/crashes/*" -not -name "README.txt" 2>/dev/null | wc -l)
    local HANGS=$(find "$FINDINGS" -path "*/hangs/*" -not -name "README.txt" 2>/dev/null | wc -l)
    echo ""
    echo "  Result: $CRASHES crashes, $HANGS hangs"
}

if [[ $NOSTOP -eq 1 ]]; then
    echo "Running in --nostop mode. Ctrl+C to stop."
    echo "Cycling through ${#HARNESSES[@]} harness(es), ${AFL_TIME}s each."

    while [[ $STOP_REQUESTED -eq 0 ]]; do
        ROUND=$((ROUND + 1))
        for i in "${!HARNESSES[@]}"; do
            if [[ $STOP_REQUESTED -eq 1 ]]; then
                break
            fi
            run_one "${HARNESSES[$i]}" "${CORPORA[$i]}"
        done
    done
else
    ROUND=1
    for i in "${!HARNESSES[@]}"; do
        run_one "${HARNESSES[$i]}" "${CORPORA[$i]}"
    done
fi

restore_terminal

echo ""
echo "Done. Results in: $RESULTS_DIR"
