add_executable(turing-import
            TuringImport.cpp)

target_link_libraries(turing-import
    PRIVATE turing_db_storage_s
            turing_db_import_json_s
            turing_db_import_neo4j_s
            turing_db_import_gml_s
            turing_db_dump_s
            turing_db_tools_common_s)

install(
    TARGETS     turing-import
    DESTINATION bin)
