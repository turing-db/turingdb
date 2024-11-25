add_executable(bioimport2
            BioImport2.cpp)

target_link_libraries(bioimport2
    PRIVATE turing_db2_core_s
            turing_db2_import_json_s
            turing_db2_import_neo4j_s
            turing_db2_import_gml_s
            tools_common_s)

target_include_directories(bioimport2
    PRIVATE ${BIO_SOURCE_DIR}/messages)

install(
    TARGETS     bioimport2
    DESTINATION bin)
