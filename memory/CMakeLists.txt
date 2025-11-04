set(memory_sources
    LocalMemory.cpp
    ColumnAllocator.cpp)

add_library(turing_db_memory_s STATIC ${memory_sources})
target_include_directories(turing_db_memory_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(turing_db_memory_s PUBLIC
    turing_db_storage_s
    turing_common_s)
