#!/bin/bash
set -e

# Restrict number of jobs to 4 on macOS to avoid freeze
if [[ "$(uname)" == "Darwin" ]]; then
    NUM_JOBS=4
else
    NUM_JOBS=$(nproc)
fi

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DEPENDENCIES_DIR=$SOURCE_DIR/external/dependencies
BUILD_DIR=$DEPENDENCIES_DIR/build
MACOS_SETENV=$DEPENDENCIES_DIR/macos_setenv.sh

BREW_LLVM_VERSION=llvm@21

mkdir -p $DEPENDENCIES_DIR
mkdir -p $BUILD_DIR

# Detect package manager (apt-get or dnf) on Linux only
if [[ "$(uname)" != "Darwin" ]]; then
    if command -v apt-get &> /dev/null; then
        PKG_MANAGER="apt-get"
        PKG_INSTALL="install -qqy"
    elif command -v dnf &> /dev/null; then
        PKG_MANAGER="dnf"
        PKG_INSTALL="install -y"
    else
        echo "Neither apt-get nor dnf found. Please install dependencies manually."
        exit 1
    fi
    
    # Update package cache
    echo "Updating $PKG_MANAGER cache..."
    sudo $PKG_MANAGER update
fi

# Install curl, openssl, zlib and cmake
if [[ "$(uname)" == "Darwin" ]]; then
    # macOS - use Homebrew
    if ! command -v brew &> /dev/null; then
        echo "Homebrew not found. Please install Homebrew first."
        exit 1
    fi

    if ! brew list curl &> /dev/null; then
        echo "Installing curl via Homebrew..."
        brew install curl
    else
        echo "curl is already installed"
    fi

    if ! brew list openssl@3 &> /dev/null; then
        echo "Installing openssl@3 via Homebrew..."
        brew install openssl@3
    else
        echo "openssl@3 is already installed"
    fi

    if ! brew list zlib &> /dev/null; then
        echo "Installing zlib via Homebrew..."
        brew install zlib
    else
        echo "zlib is already installed"
    fi

    if ! brew list cmake &> /dev/null; then
        echo "Installing cmake via Homebrew..."
        brew install cmake
    else
        echo "cmake is already installed"
    fi
else
    # Linux - use detected package manager
    echo "Installing curl, openssl, and zlib via $PKG_MANAGER..."
    sudo $PKG_MANAGER $PKG_INSTALL curl libcurl4-openssl-dev zlib1g-dev libssl-dev cmake
fi

# llvm for macos
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

    # Build a properly quoted CMAKE_ARGS string
    QUOTED_ARGS=()
    for arg in "${MACOS_COMPILER_ARGS[@]}"; do
        QUOTED_ARGS+=("'$arg'")
    done
    echo "export LLVM_PREFIX=${LLVM_PREFIX}" > "$MACOS_SETENV"
    echo "export CMAKE_ARGS=\"${QUOTED_ARGS[*]}\"" >> "$MACOS_SETENV"
fi

# Install bison and flex
if [[ "$(uname)" == "Darwin" ]]; then
    # macOS - use Homebrew
    if ! brew list bison &> /dev/null; then
        echo "Installing bison via Homebrew..."
        brew install bison
    else
        echo "bison is already installed"
    fi

    if ! brew list flex &> /dev/null; then
        echo "Installing flex via Homebrew..."
        brew install flex
    else
        echo "flex is already installed"
    fi
else
    # Linux - use detected package manager
    echo "Installing bison and flex via $PKG_MANAGER..."
    sudo $PKG_MANAGER $PKG_INSTALL bison flex libfl-dev
fi

# Install BLAS
if [[ "$(uname)" == "Darwin" ]]; then
    # macOS - use Homebrew
    if ! brew list openblas &> /dev/null; then
        echo "Installing openblas via Homebrew..."
        brew install openblas
    else
        echo "openblas is already installed"
    fi

    # Install libomp for OpenMP support (needed by FAISS)
    if ! brew list libomp &> /dev/null; then
        echo "Installing libomp via Homebrew..."
        brew install libomp
    else
        echo "libomp is already installed"
    fi
else
    # Linux - use detected package manager
    echo "Installing BLAS via $PKG_MANAGER..."
    sudo $PKG_MANAGER $PKG_INSTALL libopenblas-dev
fi

# Install minio-cpp dependencies
if [[ "$(uname)" == "Darwin" ]]; then
    # macOS - use Homebrew
    if ! brew list pkg-config &> /dev/null; then
        echo "Installing pkg-config via Homebrew..."
        brew install pkg-config
    else
        echo "pkg-config is already installed"
    fi

    if ! brew list curlpp &> /dev/null; then
        echo "Installing curlpp via Homebrew..."
        brew install curlpp
    else
        echo "curlpp is already installed"
    fi

    if ! brew list pugixml &> /dev/null; then
        echo "Installing pugixml via Homebrew..."
        brew install pugixml
    else
        echo "pugixml is already installed"
    fi

    if ! brew list inih &> /dev/null; then
        echo "Installing inih via Homebrew..."
        brew install inih
    else
        echo "inih is already installed"
    fi
else
    # Linux - use detected package manager
    echo "Installing minio-cpp dependencies via $PKG_MANAGER..."
    sudo $PKG_MANAGER $PKG_INSTALL libcurlpp-dev libinih-dev libpugixml-dev
fi

# Skip building if cache was hit (set by CI)
if [[ "$SKIP_BUILD_IF_CACHED" == "true" ]]; then
    echo "Dependencies cache hit, skipping build"
    exit 0
fi

# Build faiss
mkdir -p $BUILD_DIR/faiss
cd $BUILD_DIR/faiss

FAISS_CMAKE_ARGS=(
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX=$DEPENDENCIES_DIR
    -DBUILD_TESTING=OFF
    -DBUILD_SHARED_LIBS=OFF
    -DFAISS_ENABLE_GPU=OFF
    -DFAISS_ENABLE_CUVS=OFF
    -DFAISS_ENABLE_MKL=OFF
    -DFAISS_ENABLE_PYTHON=OFF
    -DFAISS_ENABLE_EXTRAS=OFF
)

# On macOS, we need to explicitly tell CMake where to find OpenMP from Homebrew's libomp
if [[ "$(uname)" == "Darwin" ]]; then
    LIBOMP_PREFIX=$(brew --prefix libomp)
    FAISS_CMAKE_ARGS+=(
        "-DOpenMP_CXX_FLAGS=-Xpreprocessor -fopenmp -I${LIBOMP_PREFIX}/include"
        -DOpenMP_CXX_LIB_NAMES=omp
        "-DOpenMP_omp_LIBRARY=${LIBOMP_PREFIX}/lib/libomp.dylib"
        "${MACOS_COMPILER_ARGS[@]}"
    )
fi

cmake "${FAISS_CMAKE_ARGS[@]}" $SOURCE_DIR/external/faiss-1.13.1
cmake --build $BUILD_DIR/faiss -j $NUM_JOBS
cmake --install $BUILD_DIR/faiss

# Build nlohmann_json (needed by minio-cpp)
mkdir -p $BUILD_DIR/nlohmann_json
cd $BUILD_DIR/nlohmann_json

NLOHMANN_CMAKE_ARGS=(
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX=$DEPENDENCIES_DIR
    -DJSON_BuildTests=OFF
)

if [[ "$(uname)" == "Darwin" ]]; then
    NLOHMANN_CMAKE_ARGS+=(
        "${MACOS_COMPILER_ARGS[@]}"
    )
fi

cmake "${NLOHMANN_CMAKE_ARGS[@]}" $SOURCE_DIR/external/nlohmann_json
cmake --build $BUILD_DIR/nlohmann_json -j $NUM_JOBS
cmake --install $BUILD_DIR/nlohmann_json

# Build minio-cpp
mkdir -p $BUILD_DIR/minio-cpp
cd $BUILD_DIR/minio-cpp

MINIO_CMAKE_ARGS=(
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX=$DEPENDENCIES_DIR
    -DCMAKE_PREFIX_PATH=$DEPENDENCIES_DIR
    -DCMAKE_MODULE_PATH=$SOURCE_DIR/cmake
    -DBUILD_SHARED_LIBS=OFF
    -DMINIO_CPP_TEST=OFF
)

if [[ "$(uname)" == "Darwin" ]]; then
    MINIO_CMAKE_ARGS+=(
        "${MACOS_COMPILER_ARGS[@]}"
    )
fi

cmake "${MINIO_CMAKE_ARGS[@]}" -DCMAKE_VERBOSE_MAKEFILE=ON $SOURCE_DIR/external/minio-cpp

# Diagnostic: show search paths and check for shadowing headers
if [[ "$(uname)" == "Darwin" ]]; then
    echo "=== clang config file ==="
    DARWIN_VER=$(uname -r | cut -d. -f1)
    cat /opt/homebrew/etc/clang/arm64-apple-darwin${DARWIN_VER}.cfg 2>/dev/null || echo "(not found)"
    echo "=== CMAKE_OSX_SYSROOT ==="
    grep CMAKE_OSX_SYSROOT $BUILD_DIR/minio-cpp/CMakeCache.txt 2>/dev/null
    echo "=== Header search paths (clang -v) ==="
    echo | ${LLVM_PREFIX}/bin/clang++ -stdlib=libc++ -xc++ -E -v - 2>&1 | sed -n '/#include.*search/,/End of search/p'
    echo "=== Checking for stddef.h in Homebrew include dirs ==="
    for dir in /opt/homebrew/include /opt/homebrew/Cellar/inih/*/include /opt/homebrew/Cellar/pugixml/*/include; do
        test -f "$dir/stddef.h" && echo "FOUND: $dir/stddef.h" || true
    done
    echo "=== end diagnostics ==="
fi

cmake --build $BUILD_DIR/minio-cpp -j $NUM_JOBS
cmake --install $BUILD_DIR/minio-cpp
