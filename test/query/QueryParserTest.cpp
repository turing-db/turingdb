#include "gmock/gmock.h"
#include <gtest/gtest.h>

#include "ASTContext.h"
#include "QueryParser.h"
#include "ParserException.h"
#include "TuringTest.h"

using namespace db;

class QueryParserTest : public turing::test::TuringTest {
};

TEST_F(QueryParserTest, matchPath1) {
    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const std::string query1 = "MATCH (g:gene)-[e:edge]-(p:protein) RETURN p";
    const std::string query2 = "MATCH (g:gene)--(p:protein) RETURN p";
    const std::string query3 = "MATCH (:gene)-[:edge]-(:protein) RETURN p";

    const std::string query4 = "MATCH (:gene)--(:protein) RETURN p";
    const std::string query5 = "MATCH (:gene)-[e]-(:protein) RETURN p";
    const std::string query6 = "MATCH (g)-[e]-(p) RETURN p";
    const std::string query7 = "MATCH (g)-[e]-(p) RETURN g,e,p";
    const std::string query9 = "MATCH (:gene)-[:edge]-(:protein) RETURN *";

    const std::string query10 = "MATCH (n)--(p:protein) RETURN p";

    const std::string query11 = "MATCH (a)--(b) RETURN a, b";
    const std::string query12 = "MATCH (a)--(b)--(c) RETURN a, b, c";
    const std::string query13 = "MATCH (a)--(b)--(c)--(d) RETURN a, b, c, d";

    ASSERT_TRUE(parser.parse(query1));
    ASSERT_TRUE(parser.parse(query2));
    ASSERT_TRUE(parser.parse(query3));
    ASSERT_TRUE(parser.parse(query4));
    ASSERT_TRUE(parser.parse(query5));
    ASSERT_TRUE(parser.parse(query6));
    ASSERT_TRUE(parser.parse(query7));
    ASSERT_TRUE(parser.parse(query9));
    ASSERT_TRUE(parser.parse(query10));
    ASSERT_TRUE(parser.parse(query11));
    ASSERT_TRUE(parser.parse(query12));
    ASSERT_TRUE(parser.parse(query13));
}

TEST_F(QueryParserTest, matchSingle1) {
    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const auto query1 = "MATCH (g:gene) RETURN g ";
    const auto query2 = "MATCH (g:gene) RETURN * ";
    const auto query3 = "MATCH (:gene) RETURN * ";

    const auto query6 = "MATCH (n) RETURN n ";
    const auto query7 = "MATCH (n) RETURN * ";
    const auto query9 = "MATCH (my_node) RETURN my_node";

    ASSERT_TRUE(parser.parse(query1));
    ASSERT_TRUE(parser.parse(query2));
    ASSERT_TRUE(parser.parse(query3));
    ASSERT_TRUE(parser.parse(query6));
    ASSERT_TRUE(parser.parse(query7));
    ASSERT_TRUE(parser.parse(query9));
}

TEST_F(QueryParserTest, matchProperties) {
    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const auto query1 = "MATCH (n{location = 'cytosol'}) RETURN n";
    const auto query2 = "MATCH (n{magic = 42}) RETURN n";
    const auto query3 = "MATCH (n{magic = -42}) RETURN n";
    const auto query4 = "MATCH (n{\"location {})(\" = -41}) RETURN n";
    const auto query5 = "MATCH (n{`location {})(` = -41}) RETURN n";
    const auto query6 = "MATCH (n:Protein{`location {})(` = -41}) RETURN n";

    ASSERT_TRUE(parser.parse(query1));
    ASSERT_TRUE(parser.parse(query2));
    ASSERT_TRUE(parser.parse(query3));
    ASSERT_TRUE(parser.parse(query4));
    ASSERT_TRUE(parser.parse(query5));
    ASSERT_TRUE(parser.parse(query6));
}

TEST_F(QueryParserTest, returnProperties) {
    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const auto query1 = "MATCH (n{location = 'cytosol'}) RETURN n.name";
    const auto query2 = "MATCH (n{magic = 42}) RETURN n.\"name (String)\"";
    const auto query3 = "MATCH (n{magic = 42}) RETURN n.`name (String)`";

    ASSERT_TRUE(parser.parse(query1));
    ASSERT_TRUE(parser.parse(query2));
    ASSERT_TRUE(parser.parse(query3));
}

TEST_F(QueryParserTest, parseErrors) {
    ASTContext ctxt;
    QueryParser parser(&ctxt);

    // 1 line query with error
    const auto query1 = "MATCH (n{location;'cytosol'}) RETURN n.name";

    // 2 line query with no error
    std::ostringstream query2;
    query2 << "MATCH (n{location:'cytosol'})" << std::endl;
    query2 << "RETURN n.name";

    // 2 line query with error
    std::ostringstream query3;
    query3 << "MATCH (n{location:'cytosol'})" << std::endl;
    query3 << "RETURN n <>name";


    EXPECT_THAT(
    [&]() { parser.parse(query1); },
    testing::Throws<ParserException>(
        testing::Property(&std::exception::what, 
                         testing::AllOf(
                             testing::HasSubstr("syntax error"),
                             testing::HasSubstr("line 1"),
                             testing::HasSubstr("column 18"),
                             testing::HasSubstr("column 19")
                         ))
        )
    );

    ASSERT_TRUE(parser.parse(query2.str()));

    EXPECT_THAT([&]() { parser.parse(query3.str()); },
        testing::Throws<ParserException>(
        testing::Property(&std::exception::what, 
                         testing::AllOf(
                             testing::HasSubstr("syntax error"),
                             testing::HasSubstr("line 2"),
                             testing::HasSubstr("column 10"),
                             testing::HasSubstr("column 11")
                         ))
        )
    );
}
