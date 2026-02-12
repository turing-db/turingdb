#!/bin/bash
set -e

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DEPENDENCIES_DIR=$SOURCE_DIR/external/dependencies
MACOS_SETENV=$DEPENDENCIES_DIR/macos_setenv.sh

BREW_LLVM_VERSION=llvm@21

mkdir -p $DEPENDENCIES_DIR

# Install system packages
if [[ "$(uname)" == "Darwin" ]]; then
    # macOS - use Homebrew (only cmake and llvm needed)
    if ! command -v brew &> /dev/null; then
        echo "Homebrew not found. Please install Homebrew first."
        exit 1
    fi

    if ! brew list cmake &> /dev/null; then
        echo "Installing cmake via Homebrew..."
        brew install cmake
    else
        echo "cmake is already installed"
    fi
else
    # Linux - detect package manager
    if command -v apt-get &> /dev/null; then
        sudo apt-get install -y cmake
    elif command -v dnf &> /dev/null; then
        sudo dnf install -y cmake
    elif command -v yum &> /dev/null; then
        sudo yum install -y cmake
    else
        echo "No supported package manager found (apt-get, dnf, yum)."
        exit 1
    fi
fi

# LLVM for macos
if [[ "$(uname)" == "Darwin" ]]; then
    if ! brew list $BREW_LLVM_VERSION &> /dev/null; then
        echo "Installing llvm via Homebrew..."
        brew install $BREW_LLVM_VERSION
    else
        echo "llvm is already installed"
    fi

    LLVM_PREFIX=$(brew --prefix $BREW_LLVM_VERSION 2>/dev/null)

    # Detect the macOS SDK path. Homebrew's LLVM formula generates clang
    # config files with -isysroot pointing to the CommandLineTools SDK,
    # but CI runners often only have Xcode (no CLT). CMake does not
    # auto-set CMAKE_OSX_SYSROOT for non-Apple Clang, so we must detect
    # and pass it explicitly to override the (possibly invalid) config.
    MACOS_SDK_PATH=$(xcrun --show-sdk-path 2>/dev/null)

    # Common macOS toolchain args for building all dependencies with LLVM.
    # Do NOT add -isystem for libc++ headers; let the compiler manage its
    # own built-in C++ system include paths via -stdlib=libc++.
    MACOS_COMPILER_ARGS=(
        "-DCMAKE_C_COMPILER=${LLVM_PREFIX}/bin/clang"
        "-DCMAKE_CXX_COMPILER=${LLVM_PREFIX}/bin/clang++"
        "-DCMAKE_CXX_FLAGS=-stdlib=libc++"
        "-DCMAKE_OSX_SYSROOT=${MACOS_SDK_PATH}"
        "-DCMAKE_EXE_LINKER_FLAGS=-L${LLVM_PREFIX}/lib/c++ -Wl,-rpath,${LLVM_PREFIX}/lib/c++"
        "-DCMAKE_SHARED_LINKER_FLAGS=-L${LLVM_PREFIX}/lib/c++ -Wl,-rpath,${LLVM_PREFIX}/lib/c++"
    )

    # Write environment variables in $MACOS_SETENV
    # Build a properly quoted CMAKE_ARGS string
    QUOTED_ARGS=()
    for arg in "${MACOS_COMPILER_ARGS[@]}"; do
        QUOTED_ARGS+=("'$arg'")
    done
    echo "export LLVM_PREFIX=${LLVM_PREFIX}" > "$MACOS_SETENV"
    echo "export CMAKE_ARGS=\"${QUOTED_ARGS[*]}\"" >> "$MACOS_SETENV"
fi
