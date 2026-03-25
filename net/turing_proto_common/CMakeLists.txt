add_library(turing_db_proto_common_s STATIC
	EmbeddingBuffer.cpp
        ProtocolException.cpp
	TuringProtoHeaders.cpp
	TuringProtoInBuf.cpp
	TuringProtoOutBuf.cpp)

target_include_directories(turing_db_proto_common_s
	PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_proto_common_s PUBLIC
	turing_db_tcp_common_s
	turing_common_s)
