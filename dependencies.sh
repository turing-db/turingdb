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

# Update apt cache if linux
if command -v apt-get &> /dev/null; then
    echo "Updating apt cache..."
    sudo apt-get update
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
    # Linux - use apt
    if command -v apt-get &> /dev/null; then
        echo "Installing curl via apt..."
        sudo apt-get install -qqy curl libcurl4-openssl-dev zlib1g-dev libssl-dev
    else
        echo "apt-get not found. Please install curl manually."
        exit 1
    fi
fi

# Install bison and flex
if [[ "$(uname)" == "Darwin" ]]; then
    # macOS - use Homebrew
    if ! command -v brew &> /dev/null; then
        echo "Homebrew not found. Please install Homebrew first."
        exit 1
    fi

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
    # Linux - use apt
    if command -v apt-get &> /dev/null; then
        echo "Installing bison and flex via apt..."
        sudo apt-get install -qqy bison flex libfl-dev
    else
        echo "apt-get not found. Please install bison and flex manually."
        exit 1
    fi
fi

# Install BLAS
if [[ "$(uname)" == "Darwin" ]]; then
    # macOS - use Homebrew
    if ! command -v brew &> /dev/null; then
        echo "Homebrew not found. Please install Homebrew first."
        exit 1
    fi

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
    # Linux - use apt
    if command -v apt-get &> /dev/null; then
        echo "Installing BLAS via apt..."
        sudo apt-get install -qqy libopenblas-dev
    else
        echo "apt-get not found. Please install BLAS manually."
        exit 1
    fi
fi

# Build aws-sdk-cpp
mkdir -p $BUILD_DIR/aws-sdk-cpp
cd $BUILD_DIR/aws-sdk-cpp
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=$DEPENDENCIES_DIR -DBUILD_ONLY="s3;s3-crt;ec2" -DENABLE_TESTING=OFF -DBUILD_SHARED_LIBS=OFF $SOURCE_DIR/external/aws-sdk-cpp
cmake --build $BUILD_DIR/aws-sdk-cpp -j $NUM_JOBS
cmake --install $BUILD_DIR/aws-sdk-cpp

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
