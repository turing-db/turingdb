add_library(turing_db_io_csv_s STATIC CSVParser.cpp)

target_include_directories(turing_db_io_csv_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_io_csv_s
    PRIVATE turing_common_s
            turing_db_io_fs_s
            turing_db_storage_s
            turing_db_memory_s)
