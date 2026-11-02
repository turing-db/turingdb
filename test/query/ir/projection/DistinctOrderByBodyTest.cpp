#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class DistinctOrderByBodyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectOrderedRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const Rows& actual = sink.rows();

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectOrderedRowsInAChange(std::string_view query, const Rows& expected) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            const auto res = system.newChange(_graphName);
            ASSERT_TRUE(res);

            changeID = res.value()->id();
        }

        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const Rows& actual = sink.rows();

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(DistinctOrderByBodyTest, ordersOnACountSubquery) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN DISTINCT n ORDER BY COUNT { (n)-->() } DESC, n.name LIMIT 3",
                      {{"0"},
                       {"1"},
                       {"15"}});
}

TEST_F(DistinctOrderByBodyTest, ordersOnAnExistsSubquery) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN DISTINCT n ORDER BY EXISTS { (n)-[:KNOWS_WELL]->() } DESC, n.name LIMIT 3",
                      {{"1"},
                       {"0"},
                       {"15"}});
}

TEST_F(DistinctOrderByBodyTest, ordersOnACollectSubquery) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN DISTINCT n "
                      "ORDER BY size(COLLECT { MATCH (n)-->(y) RETURN y.name }) DESC, n.name LIMIT 3",
                      {{"0"},
                       {"1"},
                       {"15"}});
}

TEST_F(DistinctOrderByBodyTest, ordersOnAPatternComprehension) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN DISTINCT n ORDER BY size([(n)-->(y) | y]) DESC, n.name LIMIT 3",
                      {{"0"},
                       {"1"},
                       {"15"}});
}

TEST_F(DistinctOrderByBodyTest, ordersOnAListComprehension) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN DISTINCT n ORDER BY size([l IN labels(n) | l]) DESC, n.name LIMIT 3",
                      {{"1"},
                       {"0"},
                       {"15"}});
}

TEST_F(DistinctOrderByBodyTest, ordersOnAReduce) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN DISTINCT n "
                      "ORDER BY reduce(total = 0, l IN labels(n) | total + 1) DESC, n.name LIMIT 3",
                      {{"1"},
                       {"0"},
                       {"15"}});
}

TEST_F(DistinctOrderByBodyTest, ordersOnACountSubqueryOverAPropertyItem) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN DISTINCT n.name AS name "
                      "ORDER BY COUNT { MATCH (p:Person)-->() WHERE p.name = name } DESC, name LIMIT 3",
                      {{"Remy"},
                       {"Adam"},
                       {"Cyrus"}});
}

TEST_F(DistinctOrderByBodyTest, ordersAWithDistinctOnACountSubquery) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) WITH DISTINCT n ORDER BY COUNT { (n)-->() } DESC, n.name LIMIT 2 "
                      "RETURN n.name",
                      {{"Remy"},
                       {"Adam"}});
}

TEST_F(DistinctOrderByBodyTest, ordersOnAListComprehensionBesideAnUnwoundVariable) {
    expectOrderedRows("UNWIND [1, 2, 2, 3] AS x UNWIND [10, 20] AS z RETURN DISTINCT x "
                      "ORDER BY size([y IN range(1, x) | y]) DESC",
                      {{"3"},
                       {"2"},
                       {"1"}});
}

TEST_F(DistinctOrderByBodyTest, ordersOnAConstantCaseBesideAConsumedVariable) {
    expectOrderedRows("MATCH (m)<-[e]-(n:Person) RETURN DISTINCT n "
                      "ORDER BY CASE WHEN n.name = 'Remy' THEN 0 ELSE 1 END, n.name LIMIT 2",
                      {{"0"},
                       {"1"}});
}

TEST_F(DistinctOrderByBodyTest, ordersDistinctConstantsOnACountSubquery) {
    expectOrderedRows("MATCH (n:Person)-[e]->(m) RETURN DISTINCT 1 AS one ORDER BY COUNT { (:Person) }",
                      {{"1"}});
}

TEST_F(DistinctOrderByBodyTest, ordersNamedPathsOnACountSubquery) {
    expectOrderedRows("MATCH p = (n:Person)-[e]->(m), (x:Interest) WITH DISTINCT p "
                      "ORDER BY COUNT { MATCH (a)-->(b) WHERE id(a) = id(nodes(p)[0]) } DESC LIMIT 4 "
                      "RETURN id(nodes(p)[0])",
                      {{"0"},
                       {"0"},
                       {"0"},
                       {"0"}});
}

TEST_F(DistinctOrderByBodyTest, ordersNamedPathsOnAPatternComprehension) {
    expectOrderedRows("MATCH p = (n:Person)-[e]->(m), (x:Interest) WITH DISTINCT p "
                      "ORDER BY size([(a)-->(b) WHERE id(a) = id(nodes(p)[0]) | b]) DESC LIMIT 4 "
                      "RETURN id(nodes(p)[0])",
                      {{"0"},
                       {"0"},
                       {"0"},
                       {"0"}});
}

TEST_F(DistinctOrderByBodyTest, ordersWalkPathsOnAPatternComprehension) {
    expectOrderedRows("MATCH p = (n:Person)-[*1..2]->(m), (x:Interest) WITH DISTINCT p "
                      "ORDER BY size([(a)-->(b) WHERE id(a) = id(nodes(p)[0]) | b]) DESC LIMIT 1 "
                      "RETURN id(nodes(p)[0])",
                      {{"0"}});
}

TEST_F(DistinctOrderByBodyTest, ordersWalkPathsOnACountSubquery) {
    expectOrderedRows("MATCH p = (n:Person)-[*1..2]->(m), (x:Interest) WITH DISTINCT p "
                      "ORDER BY COUNT { MATCH (a)-->(b) WHERE id(a) = id(nodes(p)[0]) } DESC LIMIT 1 "
                      "RETURN id(nodes(p)[0])",
                      {{"0"}});
}

TEST_F(DistinctOrderByBodyTest, ordersGroupedWalkPathsOnAPatternComprehension) {
    expectOrderedRows("MATCH p = (n:Person)-[*1..2]->(m), (x:Interest) WITH p, count(*) AS c "
                      "ORDER BY size([(a)-->(b) WHERE id(a) = id(nodes(p)[0]) | b]) DESC LIMIT 1 "
                      "RETURN id(nodes(p)[0]), c",
                      {{"0", "10"}});
}

TEST_F(DistinctOrderByBodyTest, ordersGroupedNamedPathsOnAPatternComprehension) {
    expectOrderedRows("MATCH p = (n:Person)-[e]->(m), (x:Interest) WITH p, count(*) AS c "
                      "ORDER BY size([(a)-->(b) WHERE id(a) = id(nodes(p)[0]) | b]) DESC LIMIT 1 "
                      "RETURN id(nodes(p)[0]), c",
                      {{"0", "10"}});
}

TEST_F(DistinctOrderByBodyTest, ordersOnACountSubqueryBesideAConsumedCreatedNode) {
    expectOrderedRowsInAChange("MATCH (n:Person)-[e]->(m) CREATE (t:Tmp) RETURN DISTINCT n "
                               "ORDER BY COUNT { (n)-->() } DESC, n.name LIMIT 3",
                               {{"0"},
                                {"1"},
                                {"15"}});
}

TEST_F(DistinctOrderByBodyTest, ordersACreatedPropertyOnACountSubquery) {
    expectOrderedRowsInAChange("MATCH (n:Person)-[e]->(m) CREATE (t:Tmp {v: 1}) RETURN DISTINCT t.v AS v "
                               "ORDER BY COUNT { (:Person) } LIMIT 3",
                               {{"1"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
