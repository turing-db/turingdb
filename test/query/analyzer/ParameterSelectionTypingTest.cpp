#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "AnalyzeException.h"
#include "TuringTest.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "ProcedureManager.h"
#include "SimpleGraph.h"
#include "versioning/Transaction.h"

using namespace db;

class ParameterSelectionTypingTest : public turing::test::TuringTest {
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

    void expectAccepted(const std::string& query) {
        EXPECT_NO_THROW(analyzeQuery(query)) << "query: " << query;
    }

    void expectRejected(const std::string& query, std::string_view reason) {
        try {
            analyzeQuery(query);
        } catch (const AnalyzeException& error) {
            const std::string message = error.what();
            EXPECT_NE(message.find(reason), std::string::npos) << "query: " << query << "\nerror: " << message;
            return;
        }

        ADD_FAILURE() << "query was accepted: " << query;
    }

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ProcedureManager> _procedures;
};

TEST_F(ParameterSelectionTypingTest, aCaseSelectsAParameterBesideAnotherType) {
    expectAccepted("RETURN CASE WHEN true THEN $p ELSE 1 END AS x");
    expectAccepted("RETURN CASE WHEN true THEN $p ELSE 'a' END AS x");
    expectAccepted("RETURN CASE WHEN true THEN $p ELSE $q END AS x");
}

TEST_F(ParameterSelectionTypingTest, aCaseKeepsTheCandidatesOfItsBranches) {
    expectAccepted("RETURN CASE WHEN true THEN $p - 1 ELSE null END * 2");
    expectRejected("RETURN CASE WHEN true THEN $p - 1 ELSE null END || 'a'", "'||'");
}

TEST_F(ParameterSelectionTypingTest, aCoalesceSelectsAParameterBesideAnotherType) {
    expectAccepted("RETURN coalesce($p, 1) AS x");
    expectAccepted("RETURN coalesce($p - 1, null) * 2");
    expectRejected("RETURN coalesce($p - 1, null) || 'a'", "'||'");
}

TEST_F(ParameterSelectionTypingTest, aUnionColumnSelectsAParameterBesideAnotherType) {
    expectAccepted("RETURN $p AS x UNION RETURN 1 AS x");
    expectAccepted("RETURN 1 AS x UNION RETURN $p AS x");
}

TEST_F(ParameterSelectionTypingTest, aWhenColumnSelectsAParameterBesideAnotherType) {
    expectAccepted("WHEN true THEN RETURN $p AS x ELSE RETURN 1 AS x");
}

TEST_F(ParameterSelectionTypingTest, aNextSegmentKeepsTheCandidatesOfAVariable) {
    expectAccepted("RETURN $p - 1 AS x NEXT RETURN x * 2 AS y");
    expectRejected("RETURN $p - 1 AS x NEXT RETURN x || 'a' AS y", "'||'");
}

TEST_F(ParameterSelectionTypingTest, aReduceAccumulatorKeepsTheCandidatesOfItsInitialValue) {
    expectAccepted("RETURN reduce(acc = $p, n IN [1, 2] | acc + n) AS total");
    expectAccepted("RETURN reduce(acc = $p - 1, n IN [1, 2] | acc + n) * 2");
    expectRejected("RETURN reduce(acc = $p - 1, n IN [1, 2] | acc + n) || 'a'", "'||'");
}

TEST_F(ParameterSelectionTypingTest, aWritingNextSegmentKeepsTheCandidatesOfACallImport) {
    expectAccepted("WITH $p - 1 AS q CALL (q) { UNWIND [1] AS x RETURN x NEXT CREATE (:T {x: x}) NEXT RETURN q * 2 AS y } RETURN y");
    expectRejected("WITH $p - 1 AS q CALL (q) { UNWIND [1] AS x RETURN x NEXT CREATE (:T {x: x}) NEXT RETURN q || 'a' AS y } RETURN y", "'||'");
}
