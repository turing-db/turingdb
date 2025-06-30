
set(gateway_sources HttpProxy.cpp
                    TokenValidator.cpp
                    AddressRouter.cpp)

add_library(turing_gateway_s STATIC ${gateway_sources})

target_include_directories(turing_gateway_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_gateway_s
    PRIVATE nlohmann_json
            cpphttplib
            ssl
            crypto
    PUBLIC turing_common_s)
