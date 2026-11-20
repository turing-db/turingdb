find_package(Python 3.9 REQUIRED COMPONENTS Interpreter Development.Module)

# The Python client's decode sink, kept free of Python so the tests can drive it
add_library(turing_py_sink_s STATIC
    PyColumn.cpp
    PyColumnContainer.cpp
    PySink.cpp
    PyValueStore.cpp)

target_include_directories(turing_py_sink_s
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_py_sink_s PUBLIC
    turing_db_decoders_s
    turing_db_net_encoders
    turing_db_memory_s
    turing_db_storage_s
    turing_common_s)

nanobind_add_module(_turingproto
    NB_STATIC
    NB_SUPPRESS_WARNINGS
    Bindings.cpp
    PyObjectColumn.cpp
    PyResultBuilder.cpp
    PyTuringClient.cpp)

target_link_libraries(_turingproto PRIVATE
    turing_py_sink_s
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
