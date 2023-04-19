

set(gml_sources
    GMLImport.cpp
    GMLLexer.cpp)

add_library(turing_gml_s STATIC ${gml_sources})

target_link_libraries(turing_gml_s PRIVATE
    common_s
    turing_db_s)

target_include_directories(turing_gml_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
