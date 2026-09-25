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

// A SET writes one value per row, so its value cannot aggregate the rows. An aggregate
// a WITH computed is a value per row again, and a SET below that WITH may write it.
class SetAggregateRejectionTest : public turing::test::TuringTest {
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

    void expectRejected(const std::string& query) {
        try {
            analyzeQuery(query);
        } catch (const AnalyzeException& error) {
            const std::string message = error.what();
            EXPECT_NE(message.find("Invalid use of aggregate expression in this context"), std::string::npos)
                << "query: " << query << "\nerror: " << message;
            return;
        }

        ADD_FAILURE() << "query was accepted: " << query;
    }

    void expectAccepted(const std::string& query) {
        EXPECT_NO_THROW(analyzeQuery(query)) << "query: " << query;
    }

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ProcedureManager> _procedures;
};

TEST_F(SetAggregateRejectionTest, rejectsAnAggregateSetOnAMatchedNode) {
    expectRejected("MATCH (p:Person) SET p.v = count(p)");
}

TEST_F(SetAggregateRejectionTest, rejectsAnAggregateSetOnACreatedNode) {
    expectRejected("CREATE (t:Tag) SET t.v = count(t)");
}

TEST_F(SetAggregateRejectionTest, rejectsAnAggregateInsideTheValue) {
    expectRejected("MATCH (p:Person) SET p.v = count(p) + 1");
}

TEST_F(SetAggregateRejectionTest, rejectsAnAggregateOfAnotherTypeThanTheProperty) {
    expectRejected("MATCH (p:Person) SET p.name = count(p)");
}

TEST_F(SetAggregateRejectionTest, rejectsAnAggregateInAMergeAction) {
    expectRejected("MERGE (t:Tag {name: 'x'}) ON CREATE SET t.v = count(t)");
    expectRejected("MERGE (t:Tag {name: 'x'}) ON MATCH SET t.v = count(t)");
}

TEST_F(SetAggregateRejectionTest, acceptsAnAggregateAWithComputed) {
    expectAccepted("MATCH (p:Person) WITH p, count(*) AS c SET p.v = c");
    expectAccepted("MATCH (p:Person) WITH count(p) AS c MATCH (q:Person) SET q.v = c");
}
