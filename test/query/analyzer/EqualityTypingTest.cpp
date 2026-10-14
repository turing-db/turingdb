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

// Equality takes two operands of any comparable kind: equal types compare by value, and
// different types compare false. The analyzer turns away only what it cannot compare.
class EqualityTypingTest : public turing::test::TuringTest {
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

TEST_F(EqualityTypingTest, aNumberComparesAgainstANumber) {
    expectAccepted("MATCH (n:Person) WHERE n.age = 1.5 RETURN n");
    expectAccepted("RETURN 32 = 32.0");
}

TEST_F(EqualityTypingTest, anEntityComparesAgainstAnything) {
    expectAccepted("MATCH (n:Person) WHERE n = 'x' RETURN n");
    expectAccepted("MATCH (n:Person) WHERE n = 1.5 RETURN n");
    expectAccepted("MATCH (n:Person) WHERE n = [1] RETURN n");
    expectAccepted("MATCH (n:Person)-[e]->(m) WHERE n = e RETURN n");
    expectAccepted("MATCH (n:Person)-[e]->(m) WHERE n <> m RETURN n");
    expectAccepted("MATCH (n:Person) WHERE n = 1 RETURN n");
}

TEST_F(EqualityTypingTest, aCellComparesAgainstWhatItCanHold) {
    expectAccepted("UNWIND [1, 'a'] AS x RETURN x = 1");
    expectAccepted("UNWIND [1, 'a'] AS x RETURN x = [1]");
    expectAccepted("UNWIND [1, 'a'] AS x RETURN x = null");
    expectAccepted("UNWIND [{a: 1}] AS m RETURN m.a = 'a'");
}

TEST_F(EqualityTypingTest, twoDoublesAreRejected) {
    expectRejected("RETURN 1.5 = 2.5", "rounding");
    expectRejected("RETURN 1.5 <> 2.5", "rounding");
}

TEST_F(EqualityTypingTest, aCellComparesAgainstEveryKindItCanHold) {
    expectAccepted("UNWIND [{a: 1}] AS m UNWIND [1, 'x'] AS x RETURN m.a = x");
    expectAccepted("UNWIND [1, 'a'] AS x RETURN x = (1.0, 2.0)");
    expectAccepted("WITH {d: 1} AS m RETURN m.d = datetime('2024-01-01T00:00:00Z')");
    expectAccepted("MATCH (n:Person) WITH n, {n: n} AS m RETURN m.n = n");
}

TEST_F(EqualityTypingTest, aPathIsNotCompared) {
    expectRejected("MATCH p = (n:Person)-->(m) RETURN p = 1", "not valid or compatible types");
}

TEST_F(EqualityTypingTest, aSchemaNameComparesAgainstItsOwnKindOnly) {
    expectAccepted("CALL db.labels() YIELD id, label WITH id, label WHERE id = id RETURN label");
    expectRejected("CALL db.labels() YIELD id, label WITH id, label WHERE id = 'x' RETURN label", "not valid or compatible types");
    expectRejected("CALL db.labels() YIELD id, label WITH id, label WHERE id = 1 RETURN label", "not valid or compatible types");
}
