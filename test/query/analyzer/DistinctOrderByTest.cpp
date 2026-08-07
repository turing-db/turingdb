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

const std::string_view unprojectedKeyReason =
    "ORDER BY with DISTINCT may only order by returned columns";

}

// After a DISTINCT only the returned columns are left, so an ORDER BY key the projection
// does not carry names a column the dedup dropped. The rule is a property of the query
// alone, so the analyzer is what turns those queries away, before any plan is generated.
class DistinctOrderByTest : public turing::test::TuringTest {
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
        analyzer.setV3();

        analyzer.analyze();
    }

    // The query is turned away, and on @param reason rather than on anything else the
    // analyzer may have to say about it
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

// The key is a property of a returned node, which the projection does not carry: it was
// read once per pre-dedup row, so it no longer lines up with the rows that survived
TEST_F(DistinctOrderByTest, rejectsUnprojectedKey) {
    expectRejected("MATCH (a)-->(b) RETURN DISTINCT b ORDER BY b.name", unprojectedKeyReason);
}

// One projected key does not excuse the other: the query is rejected on the key the
// dedup dropped, wherever it sits among the keys
TEST_F(DistinctOrderByTest, rejectsUnprojectedKeyAmongProjectedOnes) {
    expectRejected("MATCH (a)-->(b) RETURN DISTINCT b ORDER BY b, b.name", unprojectedKeyReason);
}

TEST_F(DistinctOrderByTest, acceptsDistinctWithoutOrderBy) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (a)-->(b) RETURN DISTINCT b"));
}

// The key is the returned column itself, so it survives the dedup and can be sorted on
TEST_F(DistinctOrderByTest, acceptsProjectedKey) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (a)-->(b) RETURN DISTINCT b ORDER BY b"));
}

// A key naming the alias the projection gives an item names that item's column
TEST_F(DistinctOrderByTest, acceptsProjectedAliasKey) {
    EXPECT_NO_THROW(
        analyzeQuery("MATCH (a)-->(b) RETURN DISTINCT b.name AS targetName ORDER BY targetName"));
}

// The key and the returned item are two separate trees holding the same expression, so
// they are one column and the query stands
TEST_F(DistinctOrderByTest, acceptsKeyEqualToAProjectedExpression) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (a:Person), (b:Person) "
                                 "RETURN DISTINCT b.age - a.age ORDER BY b.age - a.age"));
}

// A constant key holds the same value in every row, so it orders nothing and names no
// column the dedup could have dropped
TEST_F(DistinctOrderByTest, acceptsConstantKey) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (a)-->(b) RETURN DISTINCT b ORDER BY 1"));
}

// Without a DISTINCT every row is kept, so an unprojected key is read into a column of
// its own and sorted with the rows it belongs to
TEST_F(DistinctOrderByTest, acceptsUnprojectedKeyWithoutDistinct) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (a)-->(b) RETURN b ORDER BY b.name"));
}
