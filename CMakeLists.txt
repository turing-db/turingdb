cmake_minimum_required(VERSION 3.15)

project(turingdb)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if (NIX_BUILD)
    message(STATUS Building with nix)
endif()

# On macOS, require LLVM Clang (not AppleClang) with version >= 15
# AppleClang has compatibility issues with C++23 features and missing headers
if(APPLE)
    if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
        message(FATAL_ERROR
            "AppleClang is not supported. Please use LLVM Clang.\n"
            "Installation with brew: brew install llvm\n"
            "Then configure with: cmake -DCMAKE_C_COMPILER=$(brew --prefix llvm)/bin/clang "
            "-DCMAKE_CXX_COMPILER=$(brew --prefix llvm)/bin/clang++ .."
        )
    elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS "15.0")
            message(FATAL_ERROR
                "LLVM Clang version ${CMAKE_CXX_COMPILER_VERSION} is too old. "
                "Please use version 15.0 or newer.\n"
                "Update with: brew upgrade llvm"
            )
        endif()
    endif()
endif()

# Set CMAKE_INSTALL_PREFIX to build/turing_install if not set
if (CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    set(CMAKE_INSTALL_PREFIX "${CMAKE_BINARY_DIR}/turing_install"
        CACHE PATH "Default install path" FORCE)
endif()

# Check of newlines at EOF
message(STATUS "Checking for newlines at EOF")
execute_process(
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build/findnoeol ${CMAKE_CURRENT_SOURCE_DIR}
    RESULT_VARIABLE NOEOLATEOF)

if (${NOEOLATEOF} EQUAL 1)
    message(FATAL_ERROR "Found files without EOL at EOF")
endif ()

# C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# C++ warning flags
# Note: -Wno-deprecated-declarations is needed for external range-v3 library deprecation warnings
set(CPP_WARNINGS "-Wall -Werror -Wunused-function -Wunused-variable -Wno-deprecated-declarations")

# Detect debug build
set(DEBUG_BUILD FALSE)
if (DEFINED ENV{DEBUG_BUILD})
    if ($ENV{DEBUG_BUILD} EQUAL "1")
        set(DEBUG_BUILD TRUE)
    endif()
endif()

# Detect RELDEBUG
set(RELDEBUG FALSE)
if (DEFINED ENV{RELDEBUG})
    if ($ENV{RELDEBUG} EQUAL "1")
        set(RELDEBUG TRUE)
    endif()
endif()

# Detect CALLGRIND_PROFILE
set(CALLGRIND_PROFILE FALSE)
if (DEFINED ENV{CALLGRIND_PROFILE})
    if ($ENV{CALLGRIND_PROFILE} EQUAL "1")
        set(CALLGRIND_PROFILE TRUE)
    endif()
endif()

if(${CALLGRIND_PROFILE})
    if (NOT ${DEBUG_BUILD})
        set(RELDEBUG TRUE)
    endif()
    add_definitions(-DCALLGRIND_PROFILE)
endif()

# Detect TURING_DEV
set(TURING_DEV FALSE)
if (DEFINED ENV{TURING_DEV})
    set(TURING_DEV TRUE)
endif()

if (${TURING_DEV})
    add_definitions(-DTURING_DEV)
endif()

# Detect TURING_PROFILE
set(TURING_PROFILE 0)
if ($ENV{TURING_PROFILE})
    set(TURING_PROFILE 1)
    message(STATUS "Turing profile mode enabled")
endif()

# Debug settings
# -fcf-protection=none is skipped for AFL builds because afl-cc misinterprets
# any -fcf-protection flag as CFI sanitizer, causing it to inject -flto which
# breaks PCGUARD instrumentation.
# -fcf-protection is x86-only (Intel CET); skip on ARM.
set(AFL_BUILD OFF CACHE BOOL "Set by fuzz/run_afl.sh to disable -fcf-protection")
if(AFL_BUILD OR CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
    set(CFP_FLAG "")
else()
    set(CFP_FLAG "-fcf-protection=none")
endif()

if (${DEBUG_BUILD})
    message(STATUS "Debug build")
    set(OPTIMIZE_FLAGS "-O0 -g ${CFP_FLAG}" )
else()
    message(STATUS "Optimized build")
    add_definitions(-DNDEBUG)
    set(OPTIMIZE_FLAGS "-O3 ${CFP_FLAG}")

    if (${RELDEBUG})
        set(OPTIMIZE_FLAGS "${OPTIMIZE_FLAGS} -g")
    endif()
endif()

# Architecture targeting for portable PyPI wheels (Linux only).
# x86_64: -march=haswell (AVX2, FMA, BMI1/2)
# aarch64: -march=armv8-a (baseline ARMv8)
# TURING_NATIVE_BUILD=1 uses -march=native for local development.
if(NOT APPLE)
    if(DEFINED ENV{TURING_NATIVE_BUILD}
       AND "$ENV{TURING_NATIVE_BUILD}" STREQUAL "1")
        set(OPTIMIZE_FLAGS "${OPTIMIZE_FLAGS} -march=native")
    elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
        set(OPTIMIZE_FLAGS "${OPTIMIZE_FLAGS} -march=armv8-a")
    else()
        set(OPTIMIZE_FLAGS "${OPTIMIZE_FLAGS} -march=haswell")
    endif()
endif()

# C++ flags
# AFL_CXX_FLAGS is prepended to allow AFL builds to suppress warnings (e.g. -Wno-error=maybe-uninitialized)
set(AFL_CXX_FLAGS "" CACHE STRING "Extra C++ flags prepended for AFL/fuzzing builds")
set(CMAKE_CXX_FLAGS "${AFL_CXX_FLAGS} ${CPP_WARNINGS} ${OPTIMIZE_FLAGS}")

if(APPLE AND NOT NIX_BUILD)

# Statically link the entire LLVM C++ runtime (libc++, libc++abi, libunwind)
# so the binary has no dynamic dependency on Homebrew's LLVM libraries.
# -nostdlib++ prevents the compiler from automatically linking libc++.dylib.
# -load_hidden loads the static archives with hidden visibility so that
# the dynamic linker cannot weak-coalesce our libc++ symbols with Apple's
# libc++ (which system frameworks load transitively at runtime).  Without
# hidden visibility, many libc++ template instantiations are emitted as
# weak exports and the dynamic linker replaces them with Apple's non-weak
# definitions, causing ABI mismatches and crashes.

    get_filename_component(COMPILER_DIR ${CMAKE_CXX_COMPILER} DIRECTORY)
    get_filename_component(LLVM_ROOT ${COMPILER_DIR} DIRECTORY)

    foreach(_lib
        "${LLVM_ROOT}/lib/c++/libc++.a"
        "${LLVM_ROOT}/lib/c++/libc++abi.a"
        "${LLVM_ROOT}/lib/unwind/libunwind.a")
        if(NOT EXISTS "${_lib}")
            message(FATAL_ERROR "Can not find ${_lib}")
        endif()
    endforeach()

    set(LLVM_LIBCXX_HIDDEN_FLAGS
        "-Wl,-load_hidden,${LLVM_ROOT}/lib/c++/libc++.a \
-Wl,-load_hidden,${LLVM_ROOT}/lib/c++/libc++abi.a \
-Wl,-load_hidden,${LLVM_ROOT}/lib/unwind/libunwind.a")

    set(CMAKE_EXE_LINKER_FLAGS
        "${CMAKE_EXE_LINKER_FLAGS} -nostdlib++ ${LLVM_LIBCXX_HIDDEN_FLAGS}")
    set(CMAKE_SHARED_LINKER_FLAGS
        "${CMAKE_SHARED_LINKER_FLAGS} -nostdlib++ ${LLVM_LIBCXX_HIDDEN_FLAGS}")
    message(STATUS "Statically linking LLVM libc++ from: ${LLVM_ROOT}/lib/c++")
    message(STATUS "Statically linking LLVM libc++abi from: ${LLVM_ROOT}/lib/c++")
    message(STATUS "Statically linking LLVM libunwind from: ${LLVM_ROOT}/lib/unwind")

    # Use LLVM's libc++ headers.
    if(EXISTS "${LLVM_ROOT}/include/c++/v1")
        add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-stdlib=libc++>)
        add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-isystem${LLVM_ROOT}/include/c++/v1>)
        message(STATUS "Using LLVM libc++ headers from: ${LLVM_ROOT}/include/c++/v1")
    else()
        message(FATAL_ERROR "Can not find ${LLVM_ROOT}/include/c++/v1")
    endif()
endif()

# Create compilation database and symlink in the src folder
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
execute_process(
    COMMAND ${CMAKE_COMMAND} -E create_symlink
        ${CMAKE_BINARY_DIR}/compile_commands.json
        ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json
)

set(EXTERNAL_DIR ${CMAKE_CURRENT_SOURCE_DIR}/external)

list(APPEND CMAKE_PREFIX_PATH ${CMAKE_CURRENT_SOURCE_DIR}/external/dependencies)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Threads
find_package(Threads REQUIRED)

# Curl
find_package(CURL REQUIRED)

# Bison and Flex
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

# Nlohmann json
find_package(nlohmann_json REQUIRED)

# Apache Parquet (pulls in Arrow transitively)
find_package(Parquet REQUIRED)

# Faiss
find_package(Faiss REQUIRED)

set(LLVM_DIR "${CMAKE_SOURCE_DIR}/external/dependencies/build/llvm-project/lib/cmake/llvm")
set(MLIR_DIR "${CMAKE_SOURCE_DIR}/external/dependencies/build/llvm-project/lib/cmake/mlir")

find_package(LLVM REQUIRED)
find_package(MLIR REQUIRED)
message(STATUS "Using MLIRConfig.cmake in: ${MLIR_DIR}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
message(STATUS "MLIR_INCLUDE_DIRS: ${MLIR_INCLUDE_DIRS}")
message(STATUS "MLIR_INCLUDE_DIRS: ${LLVM_INCLUDE_DIRS}")

list(APPEND CMAKE_MODULE_PATH  "${MLIR_CMAKE_DIR}")
list(APPEND CMAKE_MODULE_PATH  "${LLVM_CMAKE_DIR}")

include(TableGen)
include(AddLLVM)
include(AddMLIR)

# OpenMP (required by Faiss)
# When libomp was built from source (clang builds: macOS always, Linux via
# CLANG_BUILD=1), point FindOpenMP at it.  The -Xpreprocessor flag is only
# needed on macOS; Linux clang accepts -fopenmp directly.
set(LIBOMP_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/external/dependencies")
if(NOT NIX_BUILD AND EXISTS "${LIBOMP_PREFIX}/lib/libomp.a")
    if(APPLE)
        set(OpenMP_C_FLAGS "-Xpreprocessor -fopenmp -I${LIBOMP_PREFIX}/include")
        set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I${LIBOMP_PREFIX}/include")
    else()
        set(OpenMP_C_FLAGS "-fopenmp -I${LIBOMP_PREFIX}/include")
        set(OpenMP_CXX_FLAGS "-fopenmp -I${LIBOMP_PREFIX}/include")
    endif()
    set(OpenMP_C_LIB_NAMES "omp")
    set(OpenMP_CXX_LIB_NAMES "omp")
    set(OpenMP_omp_LIBRARY "${LIBOMP_PREFIX}/lib/libomp.a")
endif()
find_package(OpenMP REQUIRED)


# Use bison and flex built from source in external/dependencies
if(NOT NIX_BUILD)
    set(DEPS_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/external/dependencies")
    set(FLEX_INCLUDE_DIRS "${DEPS_PREFIX}/include")
    set(FLEX_EXECUTABLE "${DEPS_PREFIX}/bin/flex")
    set(FLEX_LIBRARIES "${DEPS_PREFIX}/lib/libfl.a")
    set(BISON_EXECUTABLE "${DEPS_PREFIX}/bin/bison")
    set(BISON_LIBRARIES "${DEPS_PREFIX}/lib/liby.a")
    message(STATUS "Using flex from: ${FLEX_EXECUTABLE}")
    message(STATUS "Using bison from: ${BISON_EXECUTABLE}")
endif()

# zlib
find_package(ZLIB REQUIRED)

# openssl
find_package(OpenSSL REQUIRED)

# Resolve the Python used for the nanobind modules before any find_package(Python).
# The first one runs inside add_subdirectory(external) via nanobind, so this must
# come before it. Priority: explicit -DPython_EXECUTABLE, then the active
# virtualenv, then the project .venv. Without this, FindPython picks the system
# python3, whose ABI tag can differ from the Python running the wheel build and
# the regression tests.
if(NOT DEFINED Python_EXECUTABLE)
    if(DEFINED ENV{VIRTUAL_ENV} AND EXISTS "$ENV{VIRTUAL_ENV}/bin/python")
        set(Python_EXECUTABLE "$ENV{VIRTUAL_ENV}/bin/python")
    elseif(EXISTS "${CMAKE_SOURCE_DIR}/.venv/bin/python")
        set(Python_EXECUTABLE "${CMAKE_SOURCE_DIR}/.venv/bin/python")
    endif()
endif()
if(DEFINED Python_EXECUTABLE)
    message(STATUS "Python for native modules: ${Python_EXECUTABLE}")
endif()

enable_testing()

# ======== Git Info =================
# Get hash of current HEAD commit
execute_process(COMMAND git rev-parse HEAD
                OUTPUT_VARIABLE HEAD_COMMIT_HASH
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

# Get timestamp of current HEAD commit
execute_process(COMMAND git show --no-patch --format=%at
                OUTPUT_VARIABLE HEAD_COMMIT_TIMESTAMP
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

# Project version from VERSION file. Tag updates from CI
if(DEFINED ENV{SETUPTOOLS_SCM_PRETEND_VERSION})
    set(PROJECT_VERSION $ENV{SETUPTOOLS_SCM_PRETEND_VERSION})
else()
    file(STRINGS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION PROJECT_VERSION LIMIT_COUNT 1)
endif()

add_subdirectory(cmake)
add_subdirectory(external)

# miniocpp (MinIO S3 client)
find_package(miniocpp REQUIRED)

# NOTE: Disabled 11/03/26 due to being inaccessible in v2 CYPHER
add_compile_definitions(DISABLE_STRING_INDEX)

add_subdirectory(common)
add_subdirectory(base)
add_subdirectory(auth)
add_subdirectory(jobs)
add_subdirectory(io)
add_subdirectory(memory)
add_subdirectory(import)
add_subdirectory(storage)
add_subdirectory(procedures)
add_subdirectory(query)
add_subdirectory(system)
add_subdirectory(report)
add_subdirectory(vector)
add_subdirectory(extensions)
add_subdirectory(net)
add_subdirectory(db)
add_subdirectory(server)
add_subdirectory(tools)
add_subdirectory(examples)
add_subdirectory(test)

add_subdirectory(python/turingdb/_common)
add_subdirectory(python/turingdb/_binary)
add_subdirectory(python/turingdb/_embedded)
add_subdirectory(samples)
add_subdirectory(regress)
add_subdirectory(fuzz)
