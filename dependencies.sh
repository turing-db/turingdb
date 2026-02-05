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

# Install curl, openssl, and zlib
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
else
    # Linux - use detected package manager
    echo "Installing curl, openssl, and zlib via $PKG_MANAGER..."
    sudo $PKG_MANAGER $PKG_INSTALL curl libcurl4-openssl-dev zlib1g-dev libssl-dev
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

    # inih - may need to be built from source on macOS
    # For now, we'll check if it's available via brew
    if brew list inih &> /dev/null 2>&1; then
        echo "inih is already installed"
    else
        echo "Note: inih may need to be installed manually on macOS"
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
    )
fi

cmake "${FAISS_CMAKE_ARGS[@]}" $SOURCE_DIR/external/faiss-1.13.1
cmake --build $BUILD_DIR/faiss -j $NUM_JOBS
cmake --install $BUILD_DIR/faiss
