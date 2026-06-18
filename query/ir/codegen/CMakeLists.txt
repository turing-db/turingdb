set(cg_sources DBProgramGenerator.cpp)

add_library(turing_db_ir_codegen_s STATIC ${cg_sources})

target_link_libraries(turing_db_ir_codegen_s
  PRIVATE
  turing_db_ir_dbdialect_s
  turing_db_ir_common_s
  turing_db_frontend_cypher_s
  turing_db_ast_s
)

target_include_directories(turing_db_ir_codegen_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

