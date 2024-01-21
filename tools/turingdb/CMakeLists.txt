
set(turingdb_sources TuringDB.cpp)

add_executable(turingdb ${turingdb_sources})
target_link_libraries(turingdb PRIVATE
    turing_db2_server_s
    tools_common_s)

target_include_directories(turingdb PRIVATE ${BIO_SOURCE_DIR}/messages)

install(TARGETS turingdb DESTINATION bin)
