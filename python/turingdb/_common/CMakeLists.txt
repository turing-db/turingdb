find_package(Python 3.9 REQUIRED COMPONENTS Interpreter Development.Module)

# Shared nanobind glue used by every Python extension module under
# python/turingdb/. INTERFACE rather than STATIC because nanobind_add_module
# wants to compile each module's sources with its own per-module flags
# (visibility, RTTI, etc.); propagating the source via INTERFACE lets it be
# baked into _turingproto and _turinglocal individually without duplicating
# the code on disk.
add_library(turing_py_nanobind_utils INTERFACE)

target_sources(turing_py_nanobind_utils INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/NanobindUtils.cpp)

target_include_directories(turing_py_nanobind_utils INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_py_nanobind_utils INTERFACE
    turing_db_memory_s
    turing_db_storage_s
    turing_db_base_s
    turing_common_s)
