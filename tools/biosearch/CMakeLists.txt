
set(biosearch_sources
    ExploratorTree.cpp
    SearchUtils.cpp
    NodeSearch.cpp
    Explorator.cpp
    SubNetBuilder.cpp
    BioSearch.cpp)

add_executable(biosearch ${biosearch_sources})
target_link_libraries(biosearch PRIVATE
    turing_db_core_s
    turing_db_dumper_s
    bio_gmldump_s
    common_s
    tools_common_s)

target_include_directories(biosearch PRIVATE ${BIO_SOURCE_DIR}/messages)

install(TARGETS biosearch DESTINATION bin)
