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

// The shapes of a CALL the analyzer turns away, each on the reason it states.
class ProcedureCallRejectionTest : public turing::test::TuringTest {
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

TEST_F(ProcedureCallRejectionTest, rejectsAnOptionalCall) {
    expectRejected("OPTIONAL CALL db.labels() YIELD label RETURN label", "OPTIONAL CALL not supported");
}

TEST_F(ProcedureCallRejectionTest, rejectsACallWithoutAYieldInsideAQuery) {
    expectRejected("CALL db.labels() RETURN 1", "requires to name the return items");
}

TEST_F(ProcedureCallRejectionTest, rejectsAnAggregateInAYieldWhere) {
    expectRejected("CALL db.labels() YIELD id WHERE count(id) > 1 RETURN id", "Invalid use of aggregate expression");
}

TEST_F(ProcedureCallRejectionTest, rejectsANonBooleanYieldWhere) {
    expectRejected("CALL db.labels() YIELD id WHERE id RETURN id", "WHERE expression must be a boolean");
}

TEST_F(ProcedureCallRejectionTest, rejectsAYieldOfAnUnknownReturnValue) {
    expectRejected("CALL db.labels() YIELD nope RETURN nope", "does not return item 'nope'");
}

// A function is no procedure: the CALL looks it up among the procedures and finds nothing.
TEST_F(ProcedureCallRejectionTest, rejectsACallOfAFunction) {
    expectRejected("CALL toInteger('1') YIELD x RETURN x", "toInteger");
}
