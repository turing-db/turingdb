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

class ParameterDeclTest : public turing::test::TuringTest {
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

TEST_F(ParameterDeclTest, aUseNarrowsTheParameterForTheUsesAfterIt) {
    expectAccepted("RETURN $p - 1, $p * 2");
    expectRejected("RETURN $p - 1, $p || 'a'", "'||'");
}

TEST_F(ParameterDeclTest, twoUsesWithNoCommonTypeAreRejected) {
    expectRejected("RETURN $p * size($p || 'a')", "parameter '$p'");
}

TEST_F(ParameterDeclTest, differentParametersDoNotNarrowEachOther) {
    expectAccepted("RETURN $p - 1, $q || 'a'");
}

TEST_F(ParameterDeclTest, theBranchesOfAUnionShareTheParameter) {
    expectRejected("RETURN $p - 1 AS x UNION RETURN $p || 'a' AS x", "'||'");
}

TEST_F(ParameterDeclTest, aVariableOverAParameterDoesNotNarrowIt) {
    expectAccepted("WITH $p AS x RETURN x - 1, $p || 'a'");
}

TEST_F(ParameterDeclTest, aPropertyWriteNarrowsTheParameterForItsOtherUses) {
    expectAccepted("MATCH (n:Person) SET n.age = $p RETURN $p - 1");
    expectRejected("MATCH (n:Person) SET n.age = $p RETURN $p || 'a'", "'||'");
    expectRejected("CREATE (n:Person {age: $p}) RETURN $p || 'a'", "'||'");
}
