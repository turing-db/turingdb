#include <gtest/gtest.h>

#include "ASTContext.h"
#include "QueryParser.h"

using namespace db;

class QueryParserTest : public ::testing::Test {
    void SetUp() override {
    }

    void TearDown() override {
    }
};

TEST_F(QueryParserTest, selectPath1) {
    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const std::string query1 = "SELECT p FROM (g:gene)-e:edge-(p:protein)";
    const std::string query2 = "SELECT p FROM (g:gene)--(p:protein)";
    const std::string query3 = "SELECT p FROM (gene)-edge-(protein)";

    const std::string query4 = "SELECT p FROM (gene)--(protein)";
    const std::string query5 = "SELECT p FROM (gene)-e:-(protein)";
    const std::string query6 = "SELECT p FROM (g:)-e:-(p:)";
    const std::string query7 = "SELECT p FROM g:-e:-p:";
    const std::string query8 = "SELECT * FROM (gene)-edge-(protein)";
    const std::string query9 = "SELECT * FROM gene-edge-protein";

    const std::string query10 = "SELECT p FROM (n:)--(p:protein)";

    const std::string query11 = "SELECT a, b FROM (a:)--(b:)";
    const std::string query12 = "SELECT a, b, c FROM (a:)--(b:)--(c:)";
    const std::string query13 = "SELECT a, b, c, d FROM (a:)--(b:)--(c:)--(d:)";

    ASSERT_TRUE(parser.parse(query1));
    ASSERT_TRUE(parser.parse(query2));
    ASSERT_TRUE(parser.parse(query3));
    ASSERT_TRUE(parser.parse(query4));
    ASSERT_TRUE(parser.parse(query5));
    ASSERT_TRUE(parser.parse(query6));
    ASSERT_TRUE(parser.parse(query7));
    ASSERT_TRUE(parser.parse(query8));
    ASSERT_TRUE(parser.parse(query9));
    ASSERT_TRUE(parser.parse(query10));
    ASSERT_TRUE(parser.parse(query11));
    ASSERT_TRUE(parser.parse(query12));
    ASSERT_TRUE(parser.parse(query13));
}

TEST_F(QueryParserTest, selectSingle1) {
    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const auto query1 = "SELECT g FROM (g:gene)";
    const auto query2 = "SELECT * FROM (g:gene)";
    const auto query3 = "SELECT * FROM (gene)";
    const auto query4 = "SELECT * FROM gene";

    const auto query5 = "SELECT n FROM n:";
    const auto query6 = "SELECT n FROM (n:)";
    const auto query7 = "SELECT * FROM (n:)";
    const auto query8 = "SELECT my_node FROM (my_node:)";

    ASSERT_TRUE(parser.parse(query1));
    ASSERT_TRUE(parser.parse(query2));
    ASSERT_TRUE(parser.parse(query3));
    ASSERT_TRUE(parser.parse(query4));
    ASSERT_TRUE(parser.parse(query5));
    ASSERT_TRUE(parser.parse(query6));
    ASSERT_TRUE(parser.parse(query7));
    ASSERT_TRUE(parser.parse(query8));
}

TEST_F(QueryParserTest, selectName) {
    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const auto query1 = "SELECT p FROM (p:protein[APOE4])";
    const auto query2 = "SELECT p FROM p:protein[APOE4]";
    const auto query3 = "SELECT * FROM protein[APOE4]";
    const auto query4 = "SELECT * FROM protein[\"The name of my node\"]";
    const auto query5 = "SELECT * FROM protein['The name of my node']";
    const auto query6 = "SELECT * FROM protein['APOE4 [cytosol]']";

    const auto query7 = "SELECT p FROM p:['APOE4 [cytosol]']";
    const auto query8 = "SELECT * FROM p:['APOE4 [cytosol]']";

    const auto query9 = "SELECT n1, n2 FROM (p:protein[APOE4])--(n1:)--(n2:)";

    ASSERT_TRUE(parser.parse(query1));
    ASSERT_TRUE(parser.parse(query2));
    ASSERT_TRUE(parser.parse(query3));
    ASSERT_TRUE(parser.parse(query4));
    ASSERT_TRUE(parser.parse(query5));
    ASSERT_TRUE(parser.parse(query6));
    ASSERT_TRUE(parser.parse(query7));
    ASSERT_TRUE(parser.parse(query8));
    ASSERT_TRUE(parser.parse(query9));
}

TEST_F(QueryParserTest, selectProperties) {
    ASTContext ctxt;
    QueryParser parser(&ctxt);

    const auto query1 = "SELECT p FROM p:protein[APOE4]{location = 'cytosol'}";
    const auto query2 = "SELECT n FROM n:{location = 'cytosol'}";
    const auto query3 = "SELECT n FROM n:{magic = 42}";
    const auto query4 = "SELECT n FROM n:{magic = -42}";
    const auto query5 = "SELECT n FROM n:{corr >= 0.75}";
    const auto query6 = "SELECT n FROM n:{rate < 1250.752}";
    const auto query7 = "SELECT n FROM n:{corr < -0.752}";

    ASSERT_TRUE(parser.parse(query1));
    ASSERT_TRUE(parser.parse(query2));
    ASSERT_TRUE(parser.parse(query3));
    ASSERT_TRUE(parser.parse(query4));
    ASSERT_TRUE(parser.parse(query5));
    ASSERT_TRUE(parser.parse(query6));
    ASSERT_TRUE(parser.parse(query7));
}
