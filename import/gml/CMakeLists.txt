

set(gml_sources
    GMLImport.cpp
    GMLLexer.cpp)

add_library(turing_import_gml_s STATIC ${gml_sources})

target_link_libraries(turing_import_gml_s PRIVATE
    common_s
    turing_log_s
    turing_db_core_s)

target_include_directories(turing_import_gml_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
