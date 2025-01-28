set(system_sources
    GraphLoadStatus.cpp
    SystemManager.cpp)

add_library(turing_db_system_s STATIC ${system_sources})
target_link_libraries(turing_db_system_s PUBLIC 
    turing_db_storage_s
    turing_db_io_s
    turing_db_jobs_s
    turing_db_import_neo4j_s
    turing_db_import_gml_s
    turing_db_common_s)

