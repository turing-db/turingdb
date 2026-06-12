add_library(turing_db_import_common_s
  STATIC
  EmbeddingsSpec.cpp
)

target_include_directories(turing_db_import_common_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
