add_library(turing_db_proto_common_s STATIC
	ChunkedBuffer.cpp
	DecodedColumnSchema.cpp
        ProtocolException.cpp
	TuringProtoDecoder.cpp
	TuringProtoHeaders.cpp
	TuringProtoInBuf.cpp
	TuringProtoOutBuf.cpp
	TuringSink.cpp
	TuringSinkColumnContainer.cpp)

target_include_directories(turing_db_proto_common_s
	PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_proto_common_s PUBLIC
	turing_db_tcp_common_s
	turing_db_memory_s
	turing_db_storage_s
	turing_common_s)
