function(add_db2_import_neo4j_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})
    target_link_libraries(${exe_name}
        PRIVATE turing_db2_import_neo4j_s
                gtest_main)
    gtest_discover_tests(${exe_name})
endfunction()

add_db2_import_neo4j_gtest(db2_import_neo4j_neo4jimporter_test Neo4jImporterTest.cpp)

gtest_discover_tests(db2_import_neo4j_neo4jimporter_test)
