function(add_db_import_neo4j_gtest exe_name cpp_file)
    add_executable(${exe_name} ${cpp_file})

    target_compile_definitions(${exe_name}
        PUBLIC NEO4J_DIR="${EXTERNAL_DIR}/neo4j")

    target_link_libraries(${exe_name}
        PRIVATE turing_db_import_neo4j_s
                turing_db_report_s
                gtest_main)
    gtest_discover_tests(${exe_name})
endfunction()

add_db_import_neo4j_gtest(test_import_neo4j_neo4jimporter Neo4jImporterTest.cpp)

gtest_discover_tests(test_import_neo4j_neo4jimporter)
