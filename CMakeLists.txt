cmake_minimum_required(VERSION 3.15)

project(turingdb)

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
set(CPP_STANDARD "c++23")

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
if (${DEBUG_BUILD})
    message(STATUS "Debug build")
    set(OPTIMIZE_FLAGS "-O0 -g -fcf-protection=none")
else()
    message(STATUS "Optimized build")
    add_definitions(-DNDEBUG)
    set(OPTIMIZE_FLAGS "-O3 -fcf-protection=none")

    if (${RELDEBUG})
        set(OPTIMIZE_FLAGS "${OPTIMIZE_FLAGS} -g")
    endif()
endif()

# C++ flags
set(CMAKE_CXX_FLAGS "-std=${CPP_STANDARD} ${CPP_WARNINGS} ${OPTIMIZE_FLAGS}")

# When using LLVM clang from Homebrew on macOS, configure libc++ paths
if(APPLE AND NOT NIX_BUILD)
    get_filename_component(COMPILER_DIR ${CMAKE_CXX_COMPILER} DIRECTORY)
    get_filename_component(LLVM_ROOT ${COMPILER_DIR} DIRECTORY)
    if(EXISTS "${LLVM_ROOT}/lib/c++")
        # Add libc++ library path for linker
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L${LLVM_ROOT}/lib/c++ -Wl,-rpath,${LLVM_ROOT}/lib/c++")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L${LLVM_ROOT}/lib/c++ -Wl,-rpath,${LLVM_ROOT}/lib/c++")
        message(STATUS "Using LLVM libc++ from: ${LLVM_ROOT}/lib/c++")
    endif()
    # Use LLVM's libc++ and add include path for headers (required for LLVM 21+ on macOS 15+)
    if(EXISTS "${LLVM_ROOT}/include/c++/v1")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++ -isystem ${LLVM_ROOT}/include/c++/v1")
        message(STATUS "Using LLVM libc++ headers from: ${LLVM_ROOT}/include/c++/v1")
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

# Faiss
find_package(Faiss REQUIRED)

# OpenMP (required by Faiss)
# On macOS, AppleClang doesn't include OpenMP, so we need to use Homebrew's libomp
if(APPLE AND NOT NIX_BUILD)
    find_program(BREW_EXECUTABLE brew)
    if(BREW_EXECUTABLE)
        execute_process(
            COMMAND ${BREW_EXECUTABLE} --prefix libomp
            OUTPUT_VARIABLE LIBOMP_PREFIX
            OUTPUT_STRIP_TRAILING_WHITESPACE
        )
        if(LIBOMP_PREFIX)
            set(OpenMP_C_FLAGS "-Xpreprocessor -fopenmp -I${LIBOMP_PREFIX}/include")
            set(OpenMP_C_LIB_NAMES "omp")
            set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I${LIBOMP_PREFIX}/include")
            set(OpenMP_CXX_LIB_NAMES "omp")
            set(OpenMP_omp_LIBRARY "${LIBOMP_PREFIX}/lib/libomp.dylib")
        endif()
    endif()
endif()
find_package(OpenMP REQUIRED)


# Use Homebrew for Flex and Bison on macOS
if(APPLE AND NOT NIX_BUILD)
    find_program(BREW_EXECUTABLE brew)
    if(NOT BREW_EXECUTABLE)
        message(FATAL_ERROR "Homebrew not found")
    endif()

    execute_process(
        COMMAND ${BREW_EXECUTABLE} --prefix flex
        OUTPUT_VARIABLE HOMEBREW_FLEX_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if(NOT HOMEBREW_FLEX_PREFIX)
        message(FATAL_ERROR "Homebrew Flex prefix not found. Please install Flex using Homebrew.")
    endif()

    set(FLEX_INCLUDE_DIRS "${HOMEBREW_FLEX_PREFIX}/include")
    set(FLEX_EXECUTABLE "${HOMEBREW_FLEX_PREFIX}/bin/flex")
    set(FLEX_LIBRARIES "${HOMEBREW_FLEX_PREFIX}/lib/libfl.a")
    message(STATUS "Using Homebrew Flex include directory: ${FLEX_INCLUDE_DIRS}")

    execute_process(
        COMMAND ${BREW_EXECUTABLE} --prefix bison
        OUTPUT_VARIABLE HOMEBREW_BISON_PREFIX
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    if(NOT HOMEBREW_BISON_PREFIX)
        message(FATAL_ERROR "Homebrew Bison prefix not found. Please install Bison using Homebrew.")
    endif()

    set(BISON_INCLUDE_DIRS "${HOMEBREW_BISON_PREFIX}/include")
    set(BISON_EXECUTABLE "${HOMEBREW_BISON_PREFIX}/bin/bison")
    set(BISON_LIBRARIES "${HOMEBREW_BISON_PREFIX}/lib/liby.a")
    message(STATUS "Using Homebrew Bison include directory: ${BISON_INCLUDE_DIRS}")
endif()

# zlib
find_package(ZLIB REQUIRED)

# openssl
find_package(OpenSSL REQUIRED)

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

add_subdirectory(cmake)
add_subdirectory(external)

# miniocpp (MinIO S3 client)
find_package(miniocpp REQUIRED)

add_subdirectory(common)
add_subdirectory(base)
add_subdirectory(log)
add_subdirectory(jobs)
add_subdirectory(io)
add_subdirectory(memory)
add_subdirectory(import)
add_subdirectory(storage)
add_subdirectory(query)
add_subdirectory(system)
add_subdirectory(report)
add_subdirectory(vector)
add_subdirectory(db)
add_subdirectory(net)
add_subdirectory(server)
add_subdirectory(tools)
add_subdirectory(examples)
add_subdirectory(test)
add_subdirectory(samples)
add_subdirectory(regress)
