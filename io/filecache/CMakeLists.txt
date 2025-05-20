set(filecache_sources
        FileCache.cpp)

add_library(turing_db_io_filecache_s STATIC ${filecache_sources})

target_link_libraries(turing_db_io_filecache_s PRIVATE
        turing_db_s
        turing_common_s)

target_link_libraries(turing_db_io_filecache_s PUBLIC
        turing_db_io_s3_s
        turing_db_io_fs_s)

target_include_directories(turing_db_io_filecache_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

