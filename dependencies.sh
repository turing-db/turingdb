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

# Install system packages (only what cannot be built from source easily)
if [[ "$(uname)" == "Darwin" ]]; then
    # macOS - use Homebrew
    if ! command -v brew &> /dev/null; then
        echo "Homebrew not found. Please install Homebrew first."
        exit 1
    fi

    if ! brew list openssl@3 &> /dev/null; then
        echo "Installing openssl@3 via Homebrew..."
        brew install openssl@3
    else
        echo "openssl@3 is already installed"
    fi

    if ! brew list cmake &> /dev/null; then
        echo "Installing cmake via Homebrew..."
        brew install cmake
    else
        echo "cmake is already installed"
    fi
else
    # Linux - use detected package manager
    echo "Installing openssl and cmake via $PKG_MANAGER..."
    sudo $PKG_MANAGER $PKG_INSTALL libssl-dev cmake
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

# Skip building if cache was hit (set by CI)
if [[ "$SKIP_BUILD_IF_CACHED" == "true" ]]; then
    echo "Dependencies cache hit, skipping build"
    exit 0
fi

# ============================================================
# Build zlib from source
# ============================================================
echo "Building zlib..."
mkdir -p $BUILD_DIR/zlib
cd $BUILD_DIR/zlib

ZLIB_CMAKE_ARGS=(
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX=$DEPENDENCIES_DIR
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DBUILD_SHARED_LIBS=OFF
    -DZLIB_BUILD_EXAMPLES=OFF
)

if [[ "$(uname)" == "Darwin" ]]; then
    ZLIB_CMAKE_ARGS+=(
        "${MACOS_COMPILER_ARGS[@]}"
    )
fi

cmake "${ZLIB_CMAKE_ARGS[@]}" $SOURCE_DIR/external/zlib
cmake --build $BUILD_DIR/zlib -j $NUM_JOBS
cmake --install $BUILD_DIR/zlib

# Remove shared libraries so CMake's find_package(ZLIB) picks the static lib
rm -f $DEPENDENCIES_DIR/lib/libz.dylib $DEPENDENCIES_DIR/lib/libz.*.dylib
rm -f $DEPENDENCIES_DIR/lib/libz.so $DEPENDENCIES_DIR/lib/libz.so.*

# zlib's cmake renames zconf.h → zconf.h.included in the source tree;
# restore it so the submodule stays clean.
git -C $SOURCE_DIR/external/zlib checkout -- zconf.h 2>/dev/null || true

# ============================================================
# Build OpenBLAS from source
# ============================================================
echo "Building OpenBLAS..."
mkdir -p $BUILD_DIR/OpenBLAS
cd $BUILD_DIR/OpenBLAS

OPENBLAS_CMAKE_ARGS=(
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX=$DEPENDENCIES_DIR
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    -DBUILD_SHARED_LIBS=OFF
    -DBUILD_TESTING=OFF
    -DNOFORTRAN=1
)

if [[ "$(uname)" == "Darwin" ]]; then
    OPENBLAS_CMAKE_ARGS+=(
        "${MACOS_COMPILER_ARGS[@]}"
    )
fi

cmake "${OPENBLAS_CMAKE_ARGS[@]}" $SOURCE_DIR/external/OpenBLAS
cmake --build $BUILD_DIR/OpenBLAS -j $NUM_JOBS
cmake --install $BUILD_DIR/OpenBLAS

# ============================================================
# Build libomp from source (macOS only — Linux uses libgomp)
# ============================================================
if [[ "$(uname)" == "Darwin" ]]; then
    # Detect the LLVM version to download the matching openmp tarball
    LLVM_VERSION=$($LLVM_PREFIX/bin/clang --version | head -1 | sed 's/.*version \([0-9.]*\).*/\1/')
    LLVM_MAJOR=$(echo $LLVM_VERSION | cut -d. -f1)
    OPENMP_TARBALL="openmp-${LLVM_VERSION}.src.tar.xz"
    OPENMP_URL="https://github.com/llvm/llvm-project/releases/download/llvmorg-${LLVM_VERSION}/${OPENMP_TARBALL}"
    OPENMP_SRC_DIR=$BUILD_DIR/openmp-${LLVM_VERSION}.src

    if [[ ! -d "$OPENMP_SRC_DIR" ]]; then
        echo "Downloading LLVM OpenMP ${LLVM_VERSION}..."
        cd $BUILD_DIR
        curl -L -o "$OPENMP_TARBALL" "$OPENMP_URL"
        tar xf "$OPENMP_TARBALL"
        rm -f "$OPENMP_TARBALL"

        # Download LLVM cmake modules (needed for standalone openmp build)
        CMAKE_TARBALL="cmake-${LLVM_VERSION}.src.tar.xz"
        CMAKE_URL="https://github.com/llvm/llvm-project/releases/download/llvmorg-${LLVM_VERSION}/${CMAKE_TARBALL}"
        curl -L -o "$CMAKE_TARBALL" "$CMAKE_URL"
        tar xf "$CMAKE_TARBALL"
        rm -f "$CMAKE_TARBALL"
        # Rename to "cmake" so it sits as a sibling at ../cmake relative to openmp source
        rm -rf cmake
        mv "cmake-${LLVM_VERSION}.src" cmake
    fi

    echo "Building libomp..."
    mkdir -p $BUILD_DIR/libomp
    cd $BUILD_DIR/libomp

    LIBOMP_CMAKE_ARGS=(
        -DCMAKE_BUILD_TYPE=Release
        -DCMAKE_INSTALL_PREFIX=$DEPENDENCIES_DIR
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON
        -DOPENMP_STANDALONE_BUILD=ON
        -DLIBOMP_ENABLE_SHARED=OFF
        -DOPENMP_ENABLE_LIBOMPTARGET=OFF
        "${MACOS_COMPILER_ARGS[@]}"
    )

    cmake "${LIBOMP_CMAKE_ARGS[@]}" $OPENMP_SRC_DIR
    cmake --build $BUILD_DIR/libomp -j $NUM_JOBS
    cmake --install $BUILD_DIR/libomp
fi

# ============================================================
# Build curl from source
# ============================================================
echo "Building curl..."
mkdir -p $BUILD_DIR/curl
cd $BUILD_DIR/curl

CURL_CMAKE_ARGS=(
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX=$DEPENDENCIES_DIR
    -DCMAKE_PREFIX_PATH=$DEPENDENCIES_DIR
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON
    -DBUILD_SHARED_LIBS=OFF
    -DBUILD_CURL_EXE=OFF
    -DBUILD_TESTING=OFF
    -DCURL_USE_OPENSSL=ON
    -DCURL_USE_SECTRANSP=OFF
    -DCURL_DISABLE_LDAP=ON
    -DUSE_LIBIDN2=OFF
    -DCURL_USE_LIBSSH2=OFF
    -DCURL_USE_LIBPSL=OFF
)

if [[ "$(uname)" == "Darwin" ]]; then
    OPENSSL_PREFIX=$(brew --prefix openssl@3)
    CURL_CMAKE_ARGS+=(
        "-DOPENSSL_ROOT_DIR=${OPENSSL_PREFIX}"
        "${MACOS_COMPILER_ARGS[@]}"
    )
fi

cmake "${CURL_CMAKE_ARGS[@]}" $SOURCE_DIR/external/curl
cmake --build $BUILD_DIR/curl -j $NUM_JOBS
cmake --install $BUILD_DIR/curl

# ============================================================
# Build faiss
# ============================================================
echo "Building faiss..."
mkdir -p $BUILD_DIR/faiss
cd $BUILD_DIR/faiss

FAISS_CMAKE_ARGS=(
    -DCMAKE_BUILD_TYPE=Release
    -DCMAKE_INSTALL_PREFIX=$DEPENDENCIES_DIR
    -DCMAKE_PREFIX_PATH=$DEPENDENCIES_DIR
    -DBUILD_TESTING=OFF
    -DBUILD_SHARED_LIBS=OFF
    -DFAISS_ENABLE_GPU=OFF
    -DFAISS_ENABLE_CUVS=OFF
    -DFAISS_ENABLE_MKL=OFF
    -DFAISS_ENABLE_PYTHON=OFF
    -DFAISS_ENABLE_EXTRAS=OFF
    -DBLA_VENDOR=OpenBLAS
    "-DBLAS_LIBRARIES=$DEPENDENCIES_DIR/lib/libopenblas.a"
    "-DLAPACK_LIBRARIES=$DEPENDENCIES_DIR/lib/libopenblas.a"
)

if [[ "$(uname)" == "Darwin" ]]; then
    FAISS_CMAKE_ARGS+=(
        "-DOpenMP_CXX_FLAGS=-Xpreprocessor -fopenmp -I${DEPENDENCIES_DIR}/include"
        -DOpenMP_CXX_LIB_NAMES=omp
        "-DOpenMP_omp_LIBRARY=${DEPENDENCIES_DIR}/lib/libomp.a"
        "${MACOS_COMPILER_ARGS[@]}"
    )
fi

cmake "${FAISS_CMAKE_ARGS[@]}" $SOURCE_DIR/external/faiss-1.13.1
cmake --build $BUILD_DIR/faiss -j $NUM_JOBS
cmake --install $BUILD_DIR/faiss

# ============================================================
# Build nlohmann_json (needed by minio-cpp)
# ============================================================
echo "Building nlohmann_json..."
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

# ============================================================
# Build minio-cpp
# ============================================================
echo "Building minio-cpp..."
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
cmake --build $BUILD_DIR/minio-cpp -j $NUM_JOBS
cmake --install $BUILD_DIR/minio-cpp
