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

class ParameterPropertyWriteTypingTest : public turing::test::TuringTest {
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

TEST_F(ParameterPropertyWriteTypingTest, aParameterCreatesANewProperty) {
    expectAccepted("CREATE (n:Thing {v: $p}) RETURN n");
    expectAccepted("MATCH (a:Person {name: 'Remy'}), (b:Person {name: 'Adam'}) CREATE (a)-[:T {w: $p}]->(b)");
    expectAccepted("MATCH (n:Person) SET n.fresh = $p");
}

TEST_F(ParameterPropertyWriteTypingTest, aReadOfThePropertyTakesTheCandidatesOfTheParameter) {
    expectAccepted("CREATE (n:Thing {v: $p}) RETURN n.v - 1");
    expectAccepted("CREATE (n:Thing {v: $p - 1}) RETURN n.v * 2");
    expectRejected("CREATE (n:Thing {v: $p - 1}) RETURN n.v || 'a'", "'||'");
    expectRejected("MATCH (n:Person) SET n.fresh = $p - 1 RETURN n.fresh || 'a'", "'||'");
}

TEST_F(ParameterPropertyWriteTypingTest, anExistingPropertyNarrowsTheParameter) {
    expectAccepted("MATCH (n:Person) SET n.age = $p");
    expectAccepted("CREATE (n:Person {age: $p - 1})");
    expectRejected("MATCH (n:Person) SET n.age = $p || $q", "age");
    expectRejected("CREATE (n:Person {age: $p || $q})", "age");
}

TEST_F(ParameterPropertyWriteTypingTest, twoWritesOfANewPropertyShareItsTypes) {
    expectAccepted("CREATE (:Thing {v: $p}), (:Thing {v: $q - 1})");
    expectRejected("CREATE (:Thing {v: $p - 1}), (:Thing {v: $q || $r})", "'v'");
    expectRejected("CREATE (:Thing {v: 1}), (:Thing {v: $p || $q})", "'v'");
    expectRejected("CREATE (:Thing {v: $p || $q}), (:Thing {v: 1})", "'v'");
}
