set(filter_sources
    ExploratorTree.cpp
    SearchUtils.cpp
    NodeSearch.cpp
    Explorator.cpp
    SubNetBuilder.cpp)

add_library(turing_db_filter_s STATIC ${filter_sources})
target_link_libraries(turing_db_filter_s PRIVATE
    turing_db_core_s
    turing_db_dumper_s
    bio_gmldump_s
    common_s)

target_include_directories(turing_db_filter_s
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR})
