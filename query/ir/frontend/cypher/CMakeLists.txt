set (SOURCES
  VariableDependency.cpp
  VariableDependencyGraph.cpp
  VariableDependencyGraphDumper.cpp
)

add_library(turing_db_frontend_cypher_s STATIC ${SOURCES})

target_include_directories(turing_db_frontend_cypher_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_frontend_cypher_s
  PRIVATE
  turing_db_ast_s
  turing_db_query_common_s
)
