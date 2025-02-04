function(add_db2_import_json_gtest exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db2_import_json_s
                nlohmann_json
                turing_fs_s
                gtest_main)
endfunction()

add_db2_import_json_gtest(db2_import_json_neo4j_nodepropertyparser_test Neo4j/NodePropertyParserTest.cpp)

