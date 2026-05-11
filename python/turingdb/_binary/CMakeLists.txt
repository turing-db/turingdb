find_package(Python 3.9 REQUIRED COMPONENTS Interpreter Development.Module)

nanobind_add_module(_turingproto
    NB_STATIC
    Bindings.cpp
    PyTuringClient.cpp)

target_link_libraries(_turingproto PRIVATE
    turing_py_nanobind_utils
    turing_db_proto_client_s
    turing_db_proto_common_s
    turing_db_memory_s
    turing_db_storage_s
    turing_db_base_s
    turing_common_s)

set_target_properties(_turingproto PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/python/turingdb/_binary)

install(TARGETS _turingproto
    LIBRARY DESTINATION python/turingdb/_binary)
