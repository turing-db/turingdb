
cmake_minimum_required(VERSION 3.6)

project(turingdb)

# Check of newlines at EOF
message(STATUS "Checking for newlines at EOF")
execute_process(
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build/findnoeol ${CMAKE_CURRENT_SOURCE_DIR}
    RESULT_VARIABLE NOEOLATEOF)

if (${NOEOLATEOF} EQUAL 1)
    message(FATAL_ERROR "Found files without EOL at EOF")
endif ()

# C++ compiler
set(CLANG_NEEDED_VERSION "9.4.0")

# C++ standard
set(CPP_STANDARD "c++20")

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

# Threads
find_package(Threads REQUIRED)

# Curl
find_package(CURL REQUIRED)

# Bison and Flex
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

# aws-sdk-cpp
find_package(AWSSDK REQUIRED COMPONENTS s3 s3-crt)

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
add_subdirectory(dump)
add_subdirectory(query)
add_subdirectory(pipeline)
add_subdirectory(plan)
add_subdirectory(system)
add_subdirectory(report)
add_subdirectory(interpreter)
add_subdirectory(db)
add_subdirectory(net)
add_subdirectory(server)
add_subdirectory(tools)
add_subdirectory(v2)
add_subdirectory(examples)
add_subdirectory(test)
add_subdirectory(samples)
