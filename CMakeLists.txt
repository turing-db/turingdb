
cmake_minimum_required(VERSION 3.6)

project(biodesigner)


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
set(CMAKE_CXX_FLAGS "-std=${CPP_STANDARD} ${CPP_FEATURES} ${CPP_WARNINGS} ${OPTIMIZE_FLAGS}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Create compilation database and symlink in the src folder
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
execute_process(
    COMMAND ${CMAKE_COMMAND} -E create_symlink
        ${CMAKE_BINARY_DIR}/compile_commands.json
        ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json
)

# Boost
set(BOOST_VERSION boost-1.74)
set(Boost_USE_STATIC_LIBS ON)
find_package(Boost REQUIRED COMPONENTS regex filesystem system)

# Threads
find_package(Threads REQUIRED)

# Curl
find_package(CURL REQUIRED)

# Python
find_package(Python3 REQUIRED COMPONENTS Interpreter Development)

# Protobuf
set(protobuf_MODULE_COMPATIBLE ON CACHE BOOL "")
find_package(Protobuf REQUIRED
    PATHS "/turing/tools/lib/cmake/protobuf")
message(STATUS "Protobuf version ${Protobuf_VERSION}")

# gRPC
find_package(gRPC REQUIRED)

# Cython utilities
include(CythonLibrary)

# CapnProto
find_package(CapnProto REQUIRED
    PATHS "/turing/tools/lib/cmake/CapnProto")

# Bison and Flex
find_package(BISON REQUIRED)
find_package(FLEX REQUIRED)

# aws-sdk-cpp
find_package(AWSSDK REQUIRED COMPONENTS s3)

# Root source directory
set(BIO_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${BIO_SOURCE_DIR})

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

add_subdirectory(app)
add_subdirectory(app2)
add_subdirectory(external)
add_subdirectory(build)
add_subdirectory(jobsystem)
add_subdirectory(common)
add_subdirectory(logging)
add_subdirectory(perf)
add_subdirectory(db)
add_subdirectory(db2)
add_subdirectory(rpcserver)
add_subdirectory(fs)
#add_subdirectory(demo)
add_subdirectory(formats)
add_subdirectory(import)
add_subdirectory(sgc)
add_subdirectory(network)
add_subdirectory(mind)
#add_subdirectory(workflow)
add_subdirectory(python)
add_subdirectory(tools)
add_subdirectory(test)
add_subdirectory(bioinfo)
add_subdirectory(samples)
