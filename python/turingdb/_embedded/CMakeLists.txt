find_package(Python 3.9 REQUIRED COMPONENTS Interpreter Development.Module)

nanobind_add_module(_turingembedded
    NB_STATIC
    Bindings.cpp
    PyTuringEmbedded.cpp)

target_link_libraries(_turingembedded PRIVATE
    turing_py_nanobind_utils
    turing_db_s
    turing_db_system_s
    turing_db_memory_s
    turing_db_storage_s
    turing_db_base_s
    turing_common_s)

set_target_properties(_turingembedded PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/python/turingdb/_embedded)

install(TARGETS _turingembedded
    LIBRARY DESTINATION python/turingdb/_embedded)
