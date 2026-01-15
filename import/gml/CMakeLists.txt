add_library(turing_db_import_gml_s
    STATIC GMLImporter.cpp GMLGraphSax.cpp)

target_link_libraries(turing_db_import_gml_s
    PUBLIC  turing_common_s
            turing_db_storage_s
    PRIVATE nlohmann_json)

target_include_directories(turing_db_import_gml_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
