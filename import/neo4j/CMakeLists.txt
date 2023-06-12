
set(import_neo4j_sources
            Neo4JInstance.cpp
            Neo4JHttpRequest.cpp)

add_library(bio_import_neo4j_s STATIC ${import_neo4j_sources})

target_link_libraries(bio_import_neo4j_s
    common_s
    turing_log_s
    nlohmann_json
    curl
    ${Boost_LIBRARIES})

target_include_directories(bio_import_neo4j_s PRIVATE ${BIO_SOURCE_DIR}/messages)

target_include_directories(bio_import_neo4j_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
