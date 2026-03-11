set(cache_sources AgingRingCacheResult.cpp)

add_library(turing_db_io_cache_s
    STATIC ${cache_sources})

target_link_libraries(turing_db_io_cache_s
    PUBLIC turing_common_s)

target_include_directories(turing_db_io_cache_s
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
