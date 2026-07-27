#!/bin/bash
set -e

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DEPENDENCIES_DIR=$SOURCE_DIR/external/dependencies
MACOS_SETENV=$DEPENDENCIES_DIR/macos_setenv.sh

LLVM_MAJOR_VERSION=21
BREW_LLVM_VERSION=llvm@${LLVM_MAJOR_VERSION}

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
        # build-essential provides gcc/g++/make. The GitHub-hosted ubuntu ARM
        # images ship a C compiler but no g++, which the C++ dependencies
        # (Arrow, faiss, LLVM/MLIR) need; the Linux dep build uses gcc by default.
        # m4 is required by bison's and flex's configure (and at runtime when
        # they generate the parser/lexer during the main build).
        # lsb-release provides lsb_release, used by the Kitware and LLVM apt
        # repo setup below to resolve the distro codename.
        # patchelf is required by auditwheel repair (build.sh) to rewrite the
        # wheel's ELF rpaths when bundling the Linux wheel.
        #
        # Refresh the package index first. The self-hosted runner containers
        # carry a stale apt index; when a security update supersedes a
        # dependency (e.g. libarchive13, pulled in by cmake) the mirror drops
        # the old .deb, so the cached index 404s at install. apt-get update
        # re-syncs it.
        sudo apt-get update
        sudo apt-get install -y build-essential m4 lsb-release patchelf

        # Apache Arrow requires CMake >= 3.25, but Ubuntu 22.04 (jammy) ships
        # 3.22.x. When the distro's cmake is too old, pull a newer one from
        # Kitware's official APT repo (same signed-by keyring pattern as the
        # LLVM repo below). Newer distros already satisfy the requirement and
        # are left on their distro package.
        REQUIRED_CMAKE_VERSION=3.25
        INSTALLED_CMAKE_VERSION=$(cmake --version 2>/dev/null | head -1 | awk '{print $3}')
        OLDEST_CMAKE_VERSION=$(printf '%s\n%s\n' "$REQUIRED_CMAKE_VERSION" "$INSTALLED_CMAKE_VERSION" | sort -V | head -1)
        if [ "$OLDEST_CMAKE_VERSION" != "$REQUIRED_CMAKE_VERSION" ]; then
            echo "cmake ${INSTALLED_CMAKE_VERSION} is older than ${REQUIRED_CMAKE_VERSION}; installing a newer cmake from Kitware..."
            KITWARE_KEYRING=/etc/apt/keyrings/kitware-archive.asc
            sudo install -d -m 0755 /etc/apt/keyrings
            wget -qO- https://apt.kitware.com/keys/kitware-archive-latest.asc | sudo tee "${KITWARE_KEYRING}" >/dev/null
            CODENAME=$(lsb_release -cs)
            echo "deb [signed-by=${KITWARE_KEYRING}] https://apt.kitware.com/ubuntu/ ${CODENAME} main" \
                | sudo tee /etc/apt/sources.list.d/kitware.list >/dev/null
            sudo apt-get update

            # Pin to the latest CMake 3.x that Kitware serves rather than its
            # default (now 4.x). CMake 4 is strict enough to break some pinned
            # third-party dependency builds: OpenBLAS's ctest/CMakeLists.txt has
            # an unquoted if() on the (empty, because NOFORTRAN) CMAKE_Fortran_COMPILER_ID
            # that CMake 4 rejects outright. Staying on the 3.x line keeps the
            # dependency toolchain on the CMake these submodules were validated against.
            CMAKE_PIN_VERSION=$(apt-cache madison cmake \
                | grep 'apt.kitware.com' \
                | awk '{print $3}' \
                | grep -E '^3\.' \
                | sort -V \
                | tail -1)
            if [ -z "${CMAKE_PIN_VERSION}" ]; then
                echo "No CMake 3.x package available from Kitware for ${CODENAME}" >&2
                exit 1
            fi
            echo "Installing cmake ${CMAKE_PIN_VERSION} from Kitware (capped below 4.0)..."
            sudo apt-get install -y cmake=${CMAKE_PIN_VERSION} cmake-data=${CMAKE_PIN_VERSION}
        fi
    elif command -v dnf &> /dev/null; then
        sudo dnf install -y cmake
    elif command -v yum &> /dev/null; then
        sudo yum install -y cmake
    else
        echo "No supported package manager found (apt-get, dnf, yum)."
        exit 1
    fi
fi

# LLVM 21
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
else
    # Linux - install LLVM 21 via apt.llvm.org
    if command -v apt-get &> /dev/null; then
        if ! command -v "llvm-config-${LLVM_MAJOR_VERSION}" &> /dev/null; then
            echo "Installing LLVM ${LLVM_MAJOR_VERSION} via apt.llvm.org..."
            # Scope the key to this repo via signed-by so a stale key elsewhere on the runner image cannot shadow it
            LLVM_KEYRING=/etc/apt/keyrings/apt.llvm.org.asc
            sudo install -d -m 0755 /etc/apt/keyrings
            wget -qO- https://apt.llvm.org/llvm-snapshot.gpg.key | sudo tee "${LLVM_KEYRING}" >/dev/null
            CODENAME=$(lsb_release -cs)
            echo "deb [signed-by=${LLVM_KEYRING}] http://apt.llvm.org/${CODENAME}/ llvm-toolchain-${CODENAME}-${LLVM_MAJOR_VERSION} main" \
                | sudo tee /etc/apt/sources.list.d/llvm-${LLVM_MAJOR_VERSION}.list >/dev/null
            # apt.llvm.org occasionally serves a snapshot key out of sync with InRelease; retry to ride out the drift
            for attempt in 1 2 3; do
                if sudo apt-get update; then
                    break
                fi
                if [ "${attempt}" = 3 ]; then
                    exit 1
                fi
                sleep 15
            done
            sudo apt-get install -y clang-${LLVM_MAJOR_VERSION} llvm-${LLVM_MAJOR_VERSION}-dev lld-${LLVM_MAJOR_VERSION}
        else
            echo "LLVM ${LLVM_MAJOR_VERSION} is already installed"
        fi

        # On aarch64 the fully-static LLVM/MLIR tools (mlir-opt, ...) and the
        # turingdb binary exceed the +/-128MB reach of AArch64 R_AARCH64_CALL26
        # branch relocations, which GNU ld cannot resolve ("relocation truncated
        # to fit"). lld inserts range-extension thunks, so make it the default
        # linker on aarch64. x86_64 has a wider call range and is left on GNU ld.
        if [[ "$(uname -m)" == "aarch64" ]] && command -v "ld.lld-${LLVM_MAJOR_VERSION}" &> /dev/null; then
            sudo update-alternatives --install /usr/bin/ld ld "$(command -v ld.lld-${LLVM_MAJOR_VERSION})" 100
        fi
    else
        echo "LLVM ${LLVM_MAJOR_VERSION} auto-install is only supported on apt-based distros."
        echo "Please install LLVM ${LLVM_MAJOR_VERSION} manually."
        exit 1
    fi
fi
