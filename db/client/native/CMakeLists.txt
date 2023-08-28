
set(native_sources
        QueryValue.cpp
        QueryRecord.cpp
        QueryResult.cpp
        TuringClientImpl.cpp
        TuringClient.cpp)

add_library(turing_client_native_s STATIC ${native_sources})
target_link_libraries(turing_client_native_s PRIVATE
    turing_log_s
    turing_db_proto_s)
target_include_directories(turing_client_native_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# PIC version
add_library(turing_client_native_fpic_s STATIC ${native_sources})
target_link_libraries(turing_client_native_fpic_s PRIVATE
    turing_log_s
    turing_db_proto_fpic_s)
target_include_directories(turing_client_native_fpic_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
set_target_properties(turing_client_native_fpic_s PROPERTIES
    POSITION_INDEPENDENT_CODE ON)
