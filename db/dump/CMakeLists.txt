
set(dumper_sources
        DBDumper.cpp
        DBLoader.cpp)

add_library(turing_db_dumper_s STATIC ${dumper_sources})

target_include_directories(turing_db_dumper_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(turing_db_dumper_s PRIVATE ${BIO_SOURCE_DIR}/messages)

target_link_libraries(turing_db_dumper_s PRIVATE
    turing_db_core_s
    common_s
    turing_log_s)
