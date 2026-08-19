#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

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

// A YIELD introduces the variable it names, so it cannot name one the query has already
// bound: the rows a call emits are its own, and a name cannot stand for both them and
// whatever an earlier clause put behind it.
class ProcedureYieldRebindingTest : public turing::test::TuringTest {
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

// A source call opens the query, two hops run from the column it yielded, and a second
// call yields onto the name the last hop bound
TEST_F(ProcedureYieldRebindingTest, rejectsAYieldOntoAMatchedVariable) {
    expectRejected("CALL db.getNodes([0, 1]) YIELD id AS a "
                   "MATCH (a)-->(m) "
                   "MATCH (m)-->(z) "
                   "CALL db.getNodes([2]) YIELD id AS z "
                   "RETURN a, z",
                   "Variable 'z' already declared");
}

// What bound the name makes no difference: one an earlier call yielded is as bound as one
// a pattern matched
TEST_F(ProcedureYieldRebindingTest, rejectsAYieldOntoAYieldedVariable) {
    expectRejected("CALL db.getNodes([0, 1]) YIELD id AS a "
                   "CALL db.getNodes([2]) YIELD id AS a "
                   "RETURN a",
                   "Variable 'a' already declared");
}

// The same crossing shape with a name of its own for the second call's yield is the one
// the engine runs
TEST_F(ProcedureYieldRebindingTest, acceptsAYieldOfAFreshName) {
    EXPECT_NO_THROW(analyzeQuery("CALL db.getNodes([0, 1]) YIELD id AS a "
                                 "MATCH (a)-->(m) "
                                 "MATCH (m)-->(z) "
                                 "CALL db.getNodes([2]) YIELD id AS w "
                                 "RETURN a, z, w"));
}
