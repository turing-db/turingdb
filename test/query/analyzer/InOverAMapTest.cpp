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
#include "ParameterMap.h"
#include "ParameterValue.h"
#include "ProcedureManager.h"
#include "SimpleGraph.h"
#include "versioning/Transaction.h"

using namespace db;

class InOverAMapTest : public turing::test::TuringTest {
public:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
        _procedures = std::make_unique<ProcedureManager>();
        _procedures->init();

        ParameterValue map;
        map.setMap()["name"].setString("Remy");
        _parameters.set("m", map);
    }

protected:
    void analyzeQuery(const std::string& query) {
        CypherAST ast(_procedures.get(), query);

        CypherParser parser(&ast);
        parser.parse(query);

        const FrozenCommitTx transaction = _graph->openTransaction();
        CypherAnalyzer analyzer(&ast, transaction.viewGraph(), &_parameters);

        analyzer.analyze();
    }

    void expectAccepted(const std::string& query) {
        try {
            analyzeQuery(query);
        } catch (const AnalyzeException& error) {
            ADD_FAILURE() << "query was rejected: " << query << "\nerror: " << error.what();
        }
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

    ParameterMap _parameters;
    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ProcedureManager> _procedures;
};

TEST_F(InOverAMapTest, rejectsAMapLiteral) {
    expectRejected("RETURN 'a' IN {a: 1}", "IN operand must be a list, not 'Map'");
}

TEST_F(InOverAMapTest, rejectsAVariableBoundToAMap) {
    expectRejected("WITH {a: 1} AS m RETURN 'a' IN m", "IN operand must be a list, not 'Map'");
    expectRejected("WITH {a: 1} AS m WITH m WHERE 'a' IN m RETURN m", "IN operand must be a list, not 'Map'");
}

TEST_F(InOverAMapTest, rejectsTheMapOfAnEntity) {
    expectRejected("MATCH (n:Person) RETURN 'name' IN properties(n)", "IN operand must be a list, not 'Map'");
    expectRejected("MATCH (n:Person) RETURN 'Remy' IN n{.name}", "IN operand must be a list, not 'Map'");
}

TEST_F(InOverAMapTest, rejectsAMapParameter) {
    expectRejected("RETURN 'name' IN $m", "IN operand must be a list, not 'Map'");
}

TEST_F(InOverAMapTest, rejectsASelectionOfMaps) {
    expectRejected("UNWIND [true, false] AS b RETURN 'a' IN CASE WHEN b THEN {a: 1} ELSE {b: 2} END",
                   "IN operand must be a list, not 'Map'");
    expectRejected("RETURN 'a' IN coalesce(null, {a: 1})", "IN operand must be a list, not 'Map'");
    expectRejected("UNWIND [true, false] AS b RETURN 'a' IN CASE WHEN b THEN {a: 1} ELSE 2 END",
                   "IN operand must be a list, not 'Integer or Map'");
}

TEST_F(InOverAMapTest, acceptsTheKeysOfAMap) {
    expectAccepted("WITH {a: 1} AS m RETURN 'a' IN keys(m)");
    expectAccepted("RETURN 'name' IN keys($m)");
    expectAccepted("MATCH (n:Person) RETURN 'name' IN keys(properties(n))");
}

TEST_F(InOverAMapTest, acceptsAMapOnTheLeft) {
    expectAccepted("RETURN {a: 1} IN [{a: 1}, {b: 2}]");
}

TEST_F(InOverAMapTest, acceptsACaseWithAListBranch) {
    expectAccepted("UNWIND [true, false] AS b RETURN 'a' IN CASE WHEN b THEN ['a'] ELSE {a: 1} END");
}

TEST_F(InOverAMapTest, acceptsAListANullAndACell) {
    expectAccepted("RETURN 1 IN [1, 2]");
    expectAccepted("RETURN 1 IN null");
    expectAccepted("UNWIND [[1], 1] AS x RETURN 1 IN x");
    expectAccepted("WITH {a: [1, 2]} AS m RETURN 1 IN m.a");
}
