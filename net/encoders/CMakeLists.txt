add_library(turing_db_net_encoders STATIC TuringProtoEncoder.cpp)

target_include_directories(turing_db_net_encoders PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_net_encoders
    PUBLIC turing_db_storage_s
           turing_db_base_s
           turing_db_proto_common_s)
