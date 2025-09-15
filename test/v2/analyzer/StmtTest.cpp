#include "AnalyzeException.h"
#include "TuringTest.h"

#include "CypherAST.h"
#include "CypherParser.h"
#include "CypherAnalyzer.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "CypherAnalyzer.h"
#include "expr/All.h"
#include "expr/Literal.h"
#include "versioning/Transaction.h"

using namespace db;
using namespace db::v2;

class StmtTest : public turing::test::TuringTest {
public:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

protected:
    std::unique_ptr<Graph> _graph;
};

TEST_F(StmtTest, matchAllNodes) {
    const std::string query = "MATCH (n) RETURN n";
    CypherAST ast(query);

    CypherParser parser(&ast);
    ASSERT_NO_THROW(parser.parse(query));

    auto tx = _graph->openTransaction();
    CypherAnalyzer analyzer(&ast, tx.viewGraph());
    ASSERT_NO_THROW(analyzer.analyze());
}

TEST_F(StmtTest, matchAllNodesWithProperties) {
    const std::string query = "MATCH (n{duration: 42}) RETURN n";
    CypherAST ast(query);

    CypherParser parser(&ast);
    ASSERT_NO_THROW(parser.parse(query));

    auto tx = _graph->openTransaction();
    CypherAnalyzer analyzer(&ast, tx.viewGraph());
    ASSERT_NO_THROW(analyzer.analyze());
}

TEST_F(StmtTest, matchAllNodesWithPropertiesWithExpression) {
    {
        const std::string query = "MATCH (n{duration: 42+y}) RETURN n";
        CypherAST ast(query);

        CypherParser parser(&ast);
        ASSERT_NO_THROW(parser.parse(query));

        auto tx = _graph->openTransaction();
        CypherAnalyzer analyzer(&ast, tx.viewGraph());
        EXPECT_THROW(analyzer.analyze(), AnalyzeException);
    }

    {
        const std::string query = "MATCH (n{duration: 42+y.duration}) RETURN n";
        CypherAST ast(query);

        CypherParser parser(&ast);
        ASSERT_NO_THROW(parser.parse(query));

        auto tx = _graph->openTransaction();
        CypherAnalyzer analyzer(&ast, tx.viewGraph());
        EXPECT_THROW(analyzer.analyze(), AnalyzeException);
    }

    {
        const std::string query = "MATCH (n{duration: y.duration})--(y) RETURN n";
        CypherAST ast(query);

        CypherParser parser(&ast);
        ASSERT_NO_THROW(parser.parse(query));

        auto tx = _graph->openTransaction();
        CypherAnalyzer analyzer(&ast, tx.viewGraph());
        EXPECT_NO_THROW(analyzer.analyze());
    }
}
