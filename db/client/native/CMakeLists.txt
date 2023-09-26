
set(native_sources
        TuringClient.cpp)

add_library(turing_client_native_s STATIC ${native_sources})
target_link_libraries(turing_client_native_s PRIVATE
    turing_log_s
    ${Boost_LIBRARIES})
target_include_directories(turing_client_native_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
