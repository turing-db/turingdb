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

const std::string_view aggregateKeyReason =
    "Aggregate expressions in ORDER BY are not supported yet";

}

// Ordering by an aggregate is not implemented, and an alias is only another spelling of the
// item it names: a key naming the alias of an aggregate must be turned away exactly as
// spelling the aggregate out again is. An accepted query reaches a sort holding one
// aggregated row and as many row indices as the scan produced.
class OrderByAggregateAliasTest : public turing::test::TuringTest {
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

// The key is the aggregate itself, which carries the aggregate flag the analyzer reads
TEST_F(OrderByAggregateAliasTest, rejectsAggregateKey) {
    expectRejected("MATCH (n) RETURN count(n) ORDER BY count(n)", aggregateKeyReason);
}

// The same query with the aggregate aliased and the alias as the key. A symbol carries no
// aggregate flag of its own, so the key can only be recognised through the item it names.
TEST_F(OrderByAggregateAliasTest, rejectsAggregateAliasKey) {
    expectRejected("MATCH (n) RETURN count(n) AS c ORDER BY c", aggregateKeyReason);
}

// A direction on the key changes nothing about what it names
TEST_F(OrderByAggregateAliasTest, rejectsDescendingAggregateAliasKey) {
    expectRejected("MATCH (n) RETURN count(n) AS c ORDER BY c DESC", aggregateKeyReason);
}

// The rejection is of aggregate keys, not of alias keys: an alias naming a row-wise item
// names a column with one value per row, which is what a sort orders
TEST_F(OrderByAggregateAliasTest, acceptsRowWiseAliasKey) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (n) RETURN n.age AS age ORDER BY age"));
}
