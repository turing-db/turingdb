#!/bin/bash
# ==========================================================================
# TuringDB AFL++ Fuzzing
# ==========================================================================
# Builds AFL++ from source (external/AFLplusplus) and compiles instrumented
# harnesses with afl-cc (LLVM mode). Requires LLVM 21.
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
#   --rebuild-afl    Force rebuild of AFL++ itself
# ==========================================================================

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FUZZ_DIR="$SCRIPT_DIR/fuzz"
BUILD_AFL="$SCRIPT_DIR/build_afl"
RESULTS_DIR="$SCRIPT_DIR/fuzz_results/afl_findings"
AFL_SRC="$SCRIPT_DIR/external/AFLplusplus"

LLVM_MAJOR_VERSION=21

# Defaults
AFL_TIME=300
BUILD_ONLY=0
SKIP_BUILD=0
NOSTOP=0
REBUILD_AFL=0
RUN_CYPHER=0
RUN_CSV=0
RUN_GML=0
RUN_HTTP=0
ANY_SELECTED=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --cypher)      RUN_CYPHER=1; ANY_SELECTED=1; shift ;;
        --csv)         RUN_CSV=1; ANY_SELECTED=1; shift ;;
        --gml)         RUN_GML=1; ANY_SELECTED=1; shift ;;
        --http)        RUN_HTTP=1; ANY_SELECTED=1; shift ;;
        --time)        AFL_TIME="$2"; shift 2 ;;
        --nostop)      NOSTOP=1; shift ;;
        --build-only)  BUILD_ONLY=1; shift ;;
        --skip-build)  SKIP_BUILD=1; shift ;;
        --rebuild-afl) REBUILD_AFL=1; shift ;;
        -h|--help)
            echo "Usage: $0 [--cypher] [--csv] [--gml] [--http] [--time SECS] [--nostop] [--build-only] [--skip-build] [--rebuild-afl]"
            echo ""
            echo "  --cypher       Fuzz the Cypher parser"
            echo "  --csv          Fuzz the CSV parser"
            echo "  --gml          Fuzz the GML importer"
            echo "  --http         Fuzz the HTTP parser"
            echo "  --time SECS    Time per harness (default: 300)"
            echo "  --nostop       Run forever until Ctrl+C (ignores --time)"
            echo "  --build-only   Build but don't run"
            echo "  --skip-build   Use existing build"
            echo "  --rebuild-afl  Force rebuild of AFL++ from source"
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
# Require LLVM 21
# =========================================================================
LLVM_CONFIG_PATH=""
if [[ "$(uname)" == "Darwin" ]]; then
    MACOS_SETENV="$SCRIPT_DIR/external/dependencies/macos_setenv.sh"
    if [[ ! -f "$MACOS_SETENV" ]]; then
        echo "ERROR: $MACOS_SETENV not found. Run install_build_tools.sh first."
        exit 1
    fi
    source "$MACOS_SETENV"
    LLVM_CONFIG_PATH="$LLVM_PREFIX/bin/llvm-config"
else
    LLVM_CONFIG_PATH=$(command -v "llvm-config-${LLVM_MAJOR_VERSION}" 2>/dev/null || true)
fi

if [[ -z "$LLVM_CONFIG_PATH" ]] || [[ ! -x "$LLVM_CONFIG_PATH" ]]; then
    echo "ERROR: LLVM ${LLVM_MAJOR_VERSION} not found (need llvm-config-${LLVM_MAJOR_VERSION})."
    echo "  Run: ./install_build_tools.sh"
    exit 1
fi

echo "LLVM: $($LLVM_CONFIG_PATH --version) ($LLVM_CONFIG_PATH)"

# =========================================================================
# Build AFL++ from source (external/AFLplusplus)
# =========================================================================
AFL_CC_BIN="$AFL_SRC/afl-cc"
AFL_CXX_BIN="$AFL_SRC/afl-c++"
AFL_FUZZ_BIN="$AFL_SRC/afl-fuzz"

if [[ $REBUILD_AFL -eq 1 ]] || [[ ! -x "$AFL_CC_BIN" ]]; then
    if [[ ! -f "$AFL_SRC/GNUmakefile" ]]; then
        echo "ERROR: AFL++ source not found at $AFL_SRC"
        echo "  Run: git submodule update --init external/AFLplusplus"
        exit 1
    fi

    echo "Building AFL++ from source..."
    cd "$AFL_SRC"
    [[ $REBUILD_AFL -eq 1 ]] && make clean 2>/dev/null || true
    LLVM_CONFIG="$LLVM_CONFIG_PATH" make -j$(nproc) source-only 2>&1
    if [[ $? -ne 0 ]]; then
        echo "ERROR: AFL++ build failed."
        exit 1
    fi
    cd "$SCRIPT_DIR"

    if [[ ! -x "$AFL_CC_BIN" ]]; then
        echo "ERROR: afl-cc not found after build. LLVM mode may have failed."
        exit 1
    fi
    echo "AFL++ build OK: $($AFL_CC_BIN --version 2>&1 | head -1)"
else
    echo "AFL++: $($AFL_CC_BIN --version 2>&1 | head -1) (cached)"
fi

# =========================================================================
# Build libomp from source (afl-cc wraps clang, which needs libomp for OpenMP)
# =========================================================================
DEPS_DIR="$SCRIPT_DIR/external/dependencies"
DEPS_LIBOMP="$DEPS_DIR/lib/libomp.a"
if [[ $SKIP_BUILD -eq 0 ]] && [[ ! -f "$DEPS_LIBOMP" ]]; then
    LLVM_PREFIX=$("$LLVM_CONFIG_PATH" --prefix)
    LLVM_VERSION=$("$LLVM_PREFIX/bin/clang" --version | head -1 | sed 's/.*version \([0-9.]*\).*/\1/')

    BUILD_DIR="$DEPS_DIR/build"
    OPENMP_SRC_DIR="$BUILD_DIR/openmp-${LLVM_VERSION}.src"

    if [[ ! -d "$OPENMP_SRC_DIR" ]]; then
        echo "Extracting LLVM OpenMP ${LLVM_VERSION}..."
        cd "$BUILD_DIR"
        tar xf "$SCRIPT_DIR/external/openmp-${LLVM_VERSION}.src.tar.xz"

        # LLVM cmake modules required for standalone openmp build
        tar xf "$SCRIPT_DIR/external/cmake-${LLVM_VERSION}.src.tar.xz"
        rm -rf cmake
        mv "cmake-${LLVM_VERSION}.src" cmake
    fi

    echo "Building libomp..."
    mkdir -p "$BUILD_DIR/libomp"
    cd "$BUILD_DIR/libomp"

    cmake \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$DEPS_DIR" \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
        -DOPENMP_STANDALONE_BUILD=ON \
        -DLIBOMP_ENABLE_SHARED=OFF \
        -DOPENMP_ENABLE_LIBOMPTARGET=OFF \
        -DCMAKE_C_COMPILER="$LLVM_PREFIX/bin/clang" \
        -DCMAKE_CXX_COMPILER="$LLVM_PREFIX/bin/clang++" \
        "$OPENMP_SRC_DIR"
    cmake --build "$BUILD_DIR/libomp" -j$(nproc)
    cmake --install "$BUILD_DIR/libomp"
    cd "$SCRIPT_DIR"
fi

# =========================================================================
# Build harnesses with afl-cc (LLVM instrumentation)
# =========================================================================
if [[ $SKIP_BUILD -eq 0 ]]; then
    mkdir -p "$BUILD_AFL"
    cd "$BUILD_AFL"

    echo "Building harnesses with afl-cc..."
    # AFL_PATH: absolute path so afl-cc finds SanitizerCoveragePCGUARD.so
    #           regardless of the working directory (cmake builds from build_afl/).
    # AFL_QUIET: suppresses afl-cc banners during cmake's test compiles, which
    #            would otherwise confuse find_package() checks (e.g. OpenMP).
    # AFL_BUILD: tells CMakeLists.txt to skip -fcf-protection=none, which afl-cc
    #            misinterprets as CFI sanitizer (adds -flto, breaking PCGUARD).
    export AFL_PATH="$AFL_SRC"
    AFL_QUIET=1 cmake "$SCRIPT_DIR" \
        -DCMAKE_C_COMPILER="$AFL_CC_BIN" \
        -DCMAKE_CXX_COMPILER="$AFL_CXX_BIN" \
        -DAFL_BUILD=ON \
        2>&1 | tail -5
    make -j$(nproc) "${HARNESSES[@]}" 2>&1 | tail -10
    if [[ $? -ne 0 ]]; then
        echo "ERROR: afl-cc build failed."
        exit 1
    fi

    echo "Build OK."
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

    local AFL_ARGS=(
        "$INPUT_FLAG"
    )
    if [[ -n "$INPUT_DIR" ]]; then
        AFL_ARGS+=("$INPUT_DIR")
    fi
    AFL_ARGS+=(
        -o "$FINDINGS"
        -m none
        -t 100
    )
    if [[ -n "$DICT_FLAG" ]]; then
        AFL_ARGS+=($DICT_FLAG)
    fi
    if [[ $USE_TIMEOUT -eq 1 ]]; then
        AFL_ARGS+=(-V "${DURATION}")
    fi
    AFL_ARGS+=(-- "$BIN")

    if [[ $USE_TIMEOUT -eq 1 ]]; then
        AFL_SKIP_CPUFREQ=1 timeout -k 10 "${DURATION}" "$AFL_FUZZ_BIN" \
            "${AFL_ARGS[@]}" &
    else
        AFL_SKIP_CPUFREQ=1 "$AFL_FUZZ_BIN" \
            "${AFL_ARGS[@]}" &
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
