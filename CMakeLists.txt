cmake_minimum_required(VERSION 3.10)

project(turingdb)

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
set(CPP_WARNINGS "-Wall -Werror -Wunused-function -Wunused-variable")

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

# Create compilation database and symlink in the src folder
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
execute_process(
    COMMAND ${CMAKE_COMMAND} -E create_symlink
        ${CMAKE_BINARY_DIR}/compile_commands.json
        ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json
)

set(EXTERNAL_DIR ${CMAKE_CURRENT_SOURCE_DIR}/external)

list(APPEND CMAKE_PREFIX_PATH ${CMAKE_CURRENT_SOURCE_DIR}/external/dependencies)

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
find_package(OpenMP REQUIRED)


# Use Homebrew for Flex and Bison on macOS
if(APPLE)
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
endif()

if(APPLE)
    find_program(BREW_EXECUTABLE brew)
    if(NOT BREW_EXECUTABLE)
        message(FATAL_ERROR "Homebrew not found")
    endif()

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

find_package(ZLIB REQUIRED)

# aws-sdk-cpp
find_package(AWSSDK REQUIRED COMPONENTS s3 s3-crt ec2
    PATHS ${CMAKE_CURRENT_SOURCE_DIR}/external/dependencies/
)

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
