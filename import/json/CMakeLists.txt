set(import_json_sources
    JsonExamples.cpp
    JsonParser.cpp
    Neo4j4JsonParser.cpp)

add_library(bio_import_json_s STATIC ${import_json_sources})

target_link_libraries(bio_import_json_s
    common_s
    turing_log_s
    nlohmann_json
    turing_db_core_s)

target_include_directories(bio_import_json_s PRIVATE ${BIO_SOURCE_DIR}/messages)
target_include_directories(bio_import_json_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
