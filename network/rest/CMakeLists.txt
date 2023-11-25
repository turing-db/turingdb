
set(rest_sources 
        HTTPParser.cpp
        ServerConfig.cpp
        IOContextThreadPool.cpp)

add_library(turing_rest_s STATIC ${rest_sources})

target_include_directories(turing_rest_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_rest_s PRIVATE
    common_s
    ${Boost_LIBRARIES})
