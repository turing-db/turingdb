
# Copy test data to binary directory
configure_file(LR_graph.gml ${CMAKE_CURRENT_BINARY_DIR}/LR_graph.gml COPYONLY)

# Tests binaries
add_executable(import_gml_tests ImportGMLTest.cpp)
target_link_libraries(import_gml_tests PRIVATE
        turing_db_s
        turing_gml_s
        common_s
        gtest_main)

gtest_discover_tests(import_gml_tests)
