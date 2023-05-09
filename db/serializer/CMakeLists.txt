
# CapnProto dependency
find_package(CapnProto REQUIRED)
capnp_generate_cpp(CAPNP_SRCS CAPNP_HDRS capnp_src/StringIndex.capnp)

set(serializer_sources
    DBSerializer.cpp
    StringIndexSerializer.cpp)

add_library(turing_db_serializer_s STATIC
    ${CAPNP_SRCS}
    ${CAPNP_HDRS}
    ${serializer_sources}
)

target_include_directories(turing_db_serializer_s
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}

    PRIVATE
    ${BIO_SOURCE_DIR}/messages
    ${CMAKE_CURRENT_BINARY_DIR} # CapnProto generated files
)

target_link_libraries(turing_db_serializer_s PRIVATE
    turing_db_core_s
    common_s
    turing_log_s
    CapnProto::capnp
)
