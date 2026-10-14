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

class ParameterVariableTypingTest : public turing::test::TuringTest {
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

TEST_F(ParameterVariableTypingTest, aVariableKeepsTheCandidatesOfItsExpression) {
    expectAccepted("WITH $p - 1 AS x RETURN x * 2");
    expectRejected("WITH $p - 1 AS x RETURN x || 'a'", "'||'");
}

TEST_F(ParameterVariableTypingTest, aVariableKeepsItsCandidatesThroughTwoBarriers) {
    expectAccepted("WITH $p - 1 AS x WITH x AS y RETURN y * 2");
}

TEST_F(ParameterVariableTypingTest, anImportedVariableKeepsItsCandidates) {
    expectAccepted("WITH $p - 1 AS x CALL (x) { RETURN x * 2 AS y } RETURN y");
}

TEST_F(ParameterVariableTypingTest, aParameterProjectedAsAVariableKeepsAllItsCandidates) {
    expectAccepted("WITH $p AS x RETURN x - 1, x || 'a'");
}
