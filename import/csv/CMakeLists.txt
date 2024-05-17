set(csv_sources
    CSVImport.cpp
    CSVLexer.cpp)

add_library(turing_import_csv_s STATIC ${csv_sources})

target_link_libraries(turing_import_csv_s PRIVATE
    common_s
    turing_log_s
    turing_db_core_s)

target_include_directories(turing_import_csv_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
