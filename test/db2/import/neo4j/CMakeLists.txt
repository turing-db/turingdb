function(add_db2_import_neo4j_gtest exe_name cpp_file)
    add_turing_test(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db2_import_neo4j_s
                gtest_main)
endfunction()

add_db2_import_neo4j_gtest(db2_import_neo4j_neo4jimporter_test Neo4jImporterTest.cpp)
