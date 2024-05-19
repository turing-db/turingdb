set(bioimport_sources
    BioImport.cpp)

add_executable(bioimport ${bioimport_sources})

target_link_libraries(bioimport
    turing_db_dumper_s
    turing_db_stats_s
    bio_import_neo4j_s
    bio_import_json_s
    turing_import_gml_s
    turing_import_csv_s
    turing_log_s
    tools_common_s)

install(TARGETS bioimport DESTINATION bin)
