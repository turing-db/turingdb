
cmake_minimum_required(VERSION 3.0)

project(biodesigner)

# C++ compiler
set(CLANG_NEEDED_VERSION "9.4.0")

# C++ standard
set(CPP_STANDARD "c++20")

# C++ features
#set(CPP_FEATURES "-fno-rtti")

# C++ warning flags
set(CPP_WARNINGS "-Wall -Werror -Wunused-function -Wunused-variable")

# Detect debug build
set(DEBUG_BUILD FALSE)
if (DEFINED ENV{DEBUG_BUILD})
    if ($ENV{DEBUG_BUILD} EQUAL "1")
        set(DEBUG_BUILD TRUE)
    endif()
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

# CapnProto
set(ENV{PKG_CONFIG_PATH} "/turing/tools/lib/pkgconfig/")
find_package(CapnProto REQUIRED)

# Root source directory
set(BIO_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${BIO_SOURCE_DIR})

enable_testing()

add_subdirectory(external)
add_subdirectory(build)
add_subdirectory(common)
add_subdirectory(logging)
add_subdirectory(db)
add_subdirectory(apiserver)
#add_subdirectory(demo)
add_subdirectory(formats)
add_subdirectory(import)
add_subdirectory(sgc)
add_subdirectory(ui)
add_subdirectory(python)
add_subdirectory(tools)
add_subdirectory(test)
add_subdirectory(bioinfo)
