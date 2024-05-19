capnp_generate_cpp(CAPNP_SRCS CAPNP_HDRS
    capnp/StringIndex.capnp
    capnp/TypeIndex.capnp
    capnp/EntityIndex.capnp)

set(dumper_sources
    DBLoader.cpp
    DBDumper.cpp
    StringIndexLoader.cpp
    StringIndexDumper.cpp
    TypeLoader.cpp
    TypeDumper.cpp
    EntityLoader.cpp
    EntityDumper.cpp)

add_library(turing_db_dumper_s STATIC
    ${CAPNP_SRCS}
    ${CAPNP_HDRS}
    ${dumper_sources}
)

target_include_directories(turing_db_dumper_s
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}

    PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR} # CapnProto generated files
)

target_link_libraries(turing_db_dumper_s PRIVATE
    turing_db_core_s
    common_s
    turing_log_s
    CapnProto::capnp
)
