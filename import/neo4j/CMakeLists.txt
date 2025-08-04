add_library(turing_db_import_neo4j_s
    STATIC  Neo4JHttpRequest.cpp
            Neo4jImporter.cpp
            Neo4jInstance.cpp
            Neo4JQueryManager.cpp)

target_link_libraries(turing_db_import_neo4j_s
    PUBLIC  turing_common_s
            turing_db_storage_s
            turing_db_import_json_s
    PRIVATE curl
            ${Boost_LIBRARIES})

target_include_directories(turing_db_import_neo4j_s
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR})
