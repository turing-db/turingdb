
set(db_sources TuringDB.cpp)

add_library(turing_db_s STATIC ${db_sources})
target_include_directories(turing_db_s PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
