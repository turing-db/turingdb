set(dump_sources GraphLoader.cpp
                 GraphDumper.cpp
                 DataPartLoader.cpp
                 CommitDumper.cpp
                 DataPartDumper.cpp
                 DumpResult.cpp)

add_library(turing_db_dump_s STATIC ${dump_sources})

target_include_directories(turing_db_dump_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
                                                   ${BUILD_INFO_INCLUDE_DIR})

target_link_libraries(turing_db_dump_s PUBLIC
    turing_db_io_fs_s
    turing_db_storage_s
    turing_common_s)
