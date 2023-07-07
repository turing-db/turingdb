
set(stats_sources SchemaReport.cpp)

add_library(turing_db_stats_s STATIC ${stats_sources})

target_link_libraries(turing_db_stats_s PRIVATE
    common_s
    turing_log_s
    turing_db_core_s)

target_include_directories(turing_db_stats_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_include_directories(turing_db_stats_s PRIVATE ${BIO_SOURCE_DIR}/messages)
