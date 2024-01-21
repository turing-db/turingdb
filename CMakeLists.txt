
cmake_minimum_required(VERSION 3.6)

project(biodesigner)

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

set(TURING_DEV FALSE)
if (DEFINED ENV{TURING_DEV})
    set(TURING_DEV TRUE)
    add_definitions(-DTURING_DEV)
endif()

# Debug settings
if (${DEBUG_BUILD})
    message(STATUS "Debug build")
    set(OPTIMIZE_FLAGS "-O0 -g")
else()
    message(STATUS "Optimized build")
    add_definitions(-DNDEBUG)
    set(OPTIMIZE_FLAGS "-O3")
endif()

# C++ flags
set(CMAKE_CXX_FLAGS "-std=${CPP_STANDARD} ${CPP_FEATURES} ${CPP_WARNINGS} ${OPTIMIZE_FLAGS}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

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

# Root source directory
set(BIO_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${BIO_SOURCE_DIR})

enable_testing()

add_subdirectory(external)
add_subdirectory(build)
add_subdirectory(common)
add_subdirectory(logging)
add_subdirectory(db)
add_subdirectory(db2)
add_subdirectory(rpcserver)
#add_subdirectory(demo)
add_subdirectory(formats)
add_subdirectory(import)
add_subdirectory(sgc)
add_subdirectory(network)
add_subdirectory(mind)
#add_subdirectory(ui)
add_subdirectory(ui.new)
add_subdirectory(python)
add_subdirectory(tools)
add_subdirectory(test)
add_subdirectory(bioinfo)
