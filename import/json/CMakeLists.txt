add_library(turing_db_import_json_s
    STATIC  JsonParser.cpp)

target_link_libraries(turing_db_import_json_s
    PUBLIC  turing_common_s
            turing_db_jobs_s
            turing_db_storage_s
    PRIVATE nlohmann_json
            range-v3)

target_include_directories(turing_db_import_json_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
