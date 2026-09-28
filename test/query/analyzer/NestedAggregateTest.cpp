#include "AnalyzeException.h"
#include "TuringTest.h"

#include "CypherAST.h"
#include "CypherParser.h"
#include "CypherAnalyzer.h"
#include "Graph.h"
#include "ProcedureManager.h"
#include "SimpleGraph.h"
#include "versioning/Transaction.h"

using namespace db;

namespace {

const std::string_view nestedCallReason = "Aggregate functions cannot be nested inside other aggregate functions";

}

// An aggregate folds the rows of a group into one value, so it has no rows of its own left
// to fold: an aggregate over another aggregate has nothing to reduce. The query is
// ill-formed, and the analyzer is what says so - before codegen meets an argument it cannot
// lower.
class NestedAggregateTest : public turing::test::TuringTest {
public:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
        _procedures = std::make_unique<ProcedureManager>();
        _procedures->init();
    }

protected:
    void analyzeQuery(const std::string& query) {
        CypherAST ast(_procedures.get(), query);

        CypherParser parser(&ast);
        parser.parse(query);

        const FrozenCommitTx transaction = _graph->openTransaction();
        CypherAnalyzer analyzer(&ast, transaction.viewGraph());

        analyzer.analyze();
    }

    // The query is turned away by the analyzer, on @param reason and as an AnalyzeException:
    // a bioassert tripping downstream would also throw, but as an internal error naming a
    // failed assertion rather than what is wrong with the query
    void expectRejected(const std::string& query, std::string_view reason) {
        try {
            analyzeQuery(query);
        } catch (const AnalyzeException& error) {
            const std::string message = error.what();
            EXPECT_NE(message.find(reason), std::string::npos)
                << "query: " << query << "\nerror: " << message;
            return;
        }

        ADD_FAILURE() << "query was accepted: " << query;
    }

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ProcedureManager> _procedures;
};

TEST_F(NestedAggregateTest, rejectsAnAggregateSpelledInsideAnAggregate) {
    expectRejected("MATCH (a)-[e]->(b) RETURN a, count(DISTINCT sum(e.duration))",
                   nestedCallReason);
}

// The nesting is what the rule is about, not the pair of names: two aggregates over two
// columns under one key are folded side by side, neither reducing the other
TEST_F(NestedAggregateTest, acceptsTwoAggregatesSideBySide) {
    EXPECT_NO_THROW(
        analyzeQuery("MATCH (a)-[e]->(b) RETURN a.name, sum(e.duration), count(DISTINCT b)"));
}

// An expression over two aggregates reads their reduced values, so each one still folds
// rows of its own
TEST_F(NestedAggregateTest, acceptsAnExpressionOverTwoAggregates) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (a)-[e]->(b) RETURN count(a) + sum(e.duration)"));
}

// A plain aggregate over a column of the match, which is what the argument of an aggregate
// is supposed to be
TEST_F(NestedAggregateTest, acceptsAPlainAggregate) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (a) RETURN count(a.age)"));
    EXPECT_NO_THROW(analyzeQuery("MATCH (a) RETURN count(DISTINCT a.age)"));
    EXPECT_NO_THROW(analyzeQuery("MATCH (a) RETURN a.name, count(DISTINCT a.age)"));
}
