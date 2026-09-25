add_turing_test(test_query_parser_parenthesized_property_access ParenthesizedPropertyAccessTest.cpp)
target_link_libraries(test_query_parser_parenthesized_property_access
    PRIVATE turing_db_parser_s
            turing_db_query_common_s)
