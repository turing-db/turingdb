
set(gateway_sources Gateway.cpp)

add_library(turing_gateway_s STATIC ${gateway_sources})

target_include_directories(turing_gateway_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_gateway_s PRIVATE
    cpphttplib
    ssl
    crypto)
