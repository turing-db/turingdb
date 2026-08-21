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

const std::string_view rowReadReason = "must be constant";

}

// A procedure reads a constant argument once per call rather than once per row, so an
// expression that varies with the row cannot be passed to one. The procedures enforce it
// at runtime as well, but the rule is a property of the query alone, so the analyzer is
// what turns those queries away.
class ProcedureConstantArgumentTest : public turing::test::TuringTest {
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

// gnn.neighbourhoodSample reads its sample size once, before it is driven, so a property
// of the matched node cannot give it
TEST_F(ProcedureConstantArgumentTest, rejectsAPropertyAsAConstantArgument) {
    expectRejected("MATCH (n) CALL gnn.neighbourhoodSample(n, n.age) YIELD tgt RETURN tgt",
                   rowReadReason);
}

// The same holds for an optional constant argument the call does pass
TEST_F(ProcedureConstantArgumentTest, rejectsAPropertyAsAnOptionalConstantArgument) {
    expectRejected("MATCH (n) CALL gnn.neighbourhoodSample(n, 2, n.age) YIELD tgt RETURN tgt",
                   rowReadReason);
}

// A variable is no more constant than a property read: it takes a value per row
TEST_F(ProcedureConstantArgumentTest, rejectsAVariableAsAConstantArgument) {
    expectRejected("CALL db.history() YIELD nodeCount MATCH (n) "
                   "CALL gnn.neighbourhoodSample(n, nodeCount) YIELD tgt RETURN tgt",
                   rowReadReason);
}

// The row-aligned argument is the one that may read a row, and does here
TEST_F(ProcedureConstantArgumentTest, acceptsAMatchedNodeBesideConstantArguments) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (n) CALL gnn.neighbourhoodSample(n, 2, 42) YIELD tgt RETURN tgt"));
}

// The rejection is held back until every overload has been tried, so it has to carry the
// argument it was raised on that far: a message naming the wrong one, or none, would say
// nothing about which argument to make constant.
TEST_F(ProcedureConstantArgumentTest, namesTheConstantArgumentItRefused) {
    expectRejected("MATCH (n) CALL gnn.neighbourhoodSample(n, n.age) YIELD tgt RETURN tgt",
                   "sampleSize");
    expectRejected("MATCH (n) CALL gnn.neighbourhoodSample(n, 2, n.age) YIELD tgt RETURN tgt",
                   "seed");
    expectRejected("MATCH (n) CALL gnn.neighbourhoodSample(n, n.age) YIELD tgt RETURN tgt",
                   "gnn.neighbourhoodSample");
}

TEST_F(ProcedureConstantArgumentTest, acceptsALiteralListAsAConstantArgument) {
    EXPECT_NO_THROW(analyzeQuery("CALL db.getNodes([0, 1]) YIELD id RETURN id"));
}
