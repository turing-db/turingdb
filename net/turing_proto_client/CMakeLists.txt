set (turing_proto_client_source
	TuringClient.cpp)

add_library(turing_db_proto_client_s STATIC ${turing_proto_client_source})

target_include_directories(turing_db_proto_client_s
	PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_proto_client_s
  PUBLIC
  turing_db_memory_s
  turing_db_proto_common_s
  turing_db_http_common_s
  turing_db_storage_s
  turing_common_s)
