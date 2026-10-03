#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// The items of a projection read the scope it ends, as in Neo4j. Their AS aliases bind only
// in what follows the items - the ORDER BY and the scope a WITH opens - so an alias may take
// a name the scope binds, and no item reads the alias of another. Only a variable a
// subquery imports cannot be rebound.
class ProjectionAliasScopeTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              sink);

        return status;
    }

    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectRejected(std::string_view query, std::string_view reason) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_FALSE(status.isOk()) << "query accepted: " << query;

        const std::string& error = status.getError();

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR)
            << "query: " << query << "\nerror: " << error;
        EXPECT_NE(error.find(reason), std::string::npos)
            << "query: " << query << "\nerror: " << error;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ProjectionAliasScopeTest, rebindsAPublishedNameToAnotherColumn) {
    expectRowsInOrder("WITH 1 AS a, 5 AS k WITH k AS a RETURN a", {{"5"}});
}

// The name changes type too: name is a string, then Remy's node
TEST_F(ProjectionAliasScopeTest, rebindsAPublishedNameToANode) {
    expectRowsInOrder("MATCH (p:Person {name: 'Remy'}) "
                      "WITH p.name AS name, p AS name2 "
                      "WITH name2 AS name "
                      "RETURN name.name",
                      {{"Remy"}});
}

TEST_F(ProjectionAliasScopeTest, rebindsAPublishedNameOnReturn) {
    expectRowsInOrder("WITH 1 AS a, 5 AS k RETURN k AS a", {{"5"}});
}

// Remy knows Adam well, and a names Adam past the WITH
TEST_F(ProjectionAliasScopeTest, rebindsAPatternVariable) {
    expectRowsInOrder("MATCH (a:Person {name: 'Remy'})-[:KNOWS_WELL]->(b) "
                      "WITH b AS a "
                      "RETURN a.name",
                      {{"Adam"}});
}

// k is 3, 2, 1 for a 1, 2, 3: ordering on the incoming a would keep 3 first
TEST_F(ProjectionAliasScopeTest, ordersAWithByTheReboundName) {
    expectRowsInOrder("UNWIND [1, 2, 3] AS a "
                      "WITH a, 4 - a AS k "
                      "WITH k AS a ORDER BY a LIMIT 1 "
                      "RETURN a",
                      {{"1"}});
}

TEST_F(ProjectionAliasScopeTest, ordersAReturnByTheReboundName) {
    expectRowsInOrder("UNWIND [1, 2, 3] AS a "
                      "WITH a, 4 - a AS k "
                      "RETURN k AS a ORDER BY a",
                      {{"1"}, {"2"}, {"3"}});
}

// b reads the a the first WITH published, not the alias beside it
TEST_F(ProjectionAliasScopeTest, readsTheIncomingNameBesideTheAliasTakingIt) {
    expectRowsInOrder("WITH 1 AS a, 5 AS k WITH k AS a, a AS b RETURN a, b", {{"5", "1"}});
    expectRowsInOrder("WITH 1 AS a, 5 AS k WITH k + 0 AS a, a AS b RETURN a, b", {{"5", "1"}});
    expectRowsInOrder("WITH 1 AS a, 5 AS k RETURN k AS a, a AS b", {{"5", "1"}});
}

// The key b is the incoming a, so the rows come out in the order of a, not of k
TEST_F(ProjectionAliasScopeTest, ordersByAnAliasOfTheIncomingName) {
    expectRowsInOrder("UNWIND [1, 2, 3] AS a "
                      "WITH a, 4 - a AS k "
                      "RETURN k AS a, a AS b ORDER BY b",
                      {{"3", "1"}, {"2", "2"}, {"1", "3"}});
}

TEST_F(ProjectionAliasScopeTest, rejectsAnItemReadingASiblingAlias) {
    expectRejected("WITH 1 AS x RETURN x + 1 AS y, y * 2 AS z", "Variable 'y' not found");
}

TEST_F(ProjectionAliasScopeTest, rejectsAnAggregateOverASiblingAlias) {
    expectRejected("MATCH (a)-[e]->(b) RETURN a, sum(e.duration) AS s, count(s)",
                   "Variable 's' not found");
    expectRejected("MATCH (a) RETURN a.age AS age, count(DISTINCT age)",
                   "Variable 'age' not found");
}

TEST_F(ProjectionAliasScopeTest, rejectsTwoItemsOfOneProjectionUnderOneName) {
    expectRejected("WITH 1 AS a, 5 AS k WITH k AS a, 2 AS a RETURN a",
                   "Return items must have unique names");
}

TEST_F(ProjectionAliasScopeTest, rejectsRebindingAnImportedVariable) {
    expectRejected("WITH 1 AS x, 2 AS y CALL (x, y) { WITH y AS x RETURN x AS z } RETURN z",
                   "Variable 'x' is imported by the CALL");
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
