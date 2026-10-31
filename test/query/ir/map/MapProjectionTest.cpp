#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A map projection builds a map from a node or an edge: `.key` selects a property, null when
// the entity has none, `.*` selects every property, and `key: expr` adds a computed entry.
class MapProjectionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &sink);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    void expectError(std::string_view query, std::string_view expectedError) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);

        ASSERT_FALSE(status.isOk()) << "query: " << query << "\nexpected it to fail";
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(MapProjectionTest, selectsPropertiesOfANode) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p {.name, .age} AS m RETURN m.name, m.age",
               {{"Remy", "32"}});
}

TEST_F(MapProjectionTest, selectsAPropertyTheNodeLacksAsNull) {
    expectRows("MATCH (p:Person {name: 'Maxime'}) WITH p {.name, .age} AS m RETURN m.name, m.age",
               {{"Maxime", "null"}});
}

TEST_F(MapProjectionTest, addsAComputedEntry) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p {.name, rank: p.age + 1} AS m RETURN m.name, m.rank",
               {{"Remy", "33"}});
}

TEST_F(MapProjectionTest, selectsEveryProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p {.*} AS m RETURN m.name, m.dob",
               {{"Remy", "18/01"}});
}

TEST_F(MapProjectionTest, selectsPropertiesOfAnEdge) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() WITH e {.duration} AS m RETURN m.duration",
               {{"20"}});
}

TEST_F(MapProjectionTest, replacesAPropertyWithALaterEntry) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p {.*, name: 'Remi'} AS m RETURN m.name, m.age",
               {{"Remi", "32"}});
}

TEST_F(MapProjectionTest, replacesAnEntryWithALaterProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p {name: 'Remi', .*} AS m RETURN m.name, m.age",
               {{"Remy", "32"}});
}

TEST_F(MapProjectionTest, keepsASelectedPropertyTheNodeLacksBesideEveryProperty) {
    expectRows("MATCH (p:Person {name: 'Maxime'}) WITH p {.age, .*} AS m RETURN m.name, m.age",
               {{"Maxime", "null"}});
}

TEST_F(MapProjectionTest, selectsAVariable) {
    expectRows("MATCH (p:Person {name: 'Remy'}) WITH p, 'Paris' AS city WITH p {.name, city} AS m RETURN m.name, m.city",
               {{"Remy", "Paris"}});
}

TEST_F(MapProjectionTest, projectsAMap) {
    expectRows("WITH {name: 'Remy', age: 32} AS person WITH person {.name, rank: 1} AS m RETURN m.name, m.rank, m.age",
               {{"Remy", "1", "null"}});
}

TEST_F(MapProjectionTest, projectsEveryEntryOfAMap) {
    expectRows("WITH {a: 1, b: 2} AS base WITH base {.*, b: 3} AS m RETURN m.a, m.b",
               {{"1", "3"}});
}

TEST_F(MapProjectionTest, projectsEveryEntryOfAMapReadPerRow) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] WITH {name: p.name, age: p.age} AS base "
               "WITH base {.*, age: 0} AS m RETURN m.name, m.age",
               {{"Adam", "0"}, {"Remy", "0"}});
}

TEST_F(MapProjectionTest, projectsANullBaseToNull) {
    expectRows("OPTIONAL MATCH (p:Person {name: 'Nobody'}) WITH p {.name} AS m RETURN m IS NULL, m.name",
               {{"true", "null"}});
}

TEST_F(MapProjectionTest, projectsEveryPropertyOfANullBaseToNull) {
    expectRows("OPTIONAL MATCH (p:Person {name: 'Nobody'}) WITH p {.*, rank: 1} AS m RETURN m IS NULL, m.rank",
               {{"true", "null"}});
}

TEST_F(MapProjectionTest, selectsEveryPropertyOfAnEdge) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:KNOWS_WELL]->() WITH e {.*} AS m RETURN m.duration",
               {{"20"}});
}

TEST_F(MapProjectionTest, projectsInsideAListComprehension) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Maxime'] WITH collect(p) AS people "
               "UNWIND [x IN people | x {.name, .age}] AS m RETURN m.name, m.age",
               {{"Maxime", "null"}, {"Remy", "32"}});
}

TEST_F(MapProjectionTest, collectsProjections) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] RETURN size(collect(p {.name})) AS count",
               {{"2"}});
}

TEST_F(MapProjectionTest, collectsProjectionsReadBack) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] WITH collect(p {.name, rank: 1}) AS l "
               "UNWIND l AS m RETURN m.name, m.rank",
               {{"Adam", "1"}, {"Remy", "1"}});
}

TEST_F(MapProjectionTest, collectsDistinctProjectionsByTheirEntries) {
    expectRows("MATCH (p:Person) WHERE p.name IN ['Remy', 'Adam'] RETURN size(collect(DISTINCT p {.age})) AS count",
               {{"1"}});
}

TEST_F(MapProjectionTest, collectsNoProjectionOfANullBase) {
    expectRows("OPTIONAL MATCH (p:Person {name: 'Nobody'}) RETURN size(collect(p {.name})) AS count",
               {{"0"}});
}

TEST_F(MapProjectionTest, readsAKeyOffTheProjection) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN p {.name, .age}.age AS age",
               {{"32"}});
}

TEST_F(MapProjectionTest, readsANodePatternWithPropertiesAsAPattern) {
    expectRows("MATCH (a:Person) WHERE (a {name: 'Remy'})-->(:Interest {name: 'Ghosts'}) RETURN a.name",
               {{"Remy"}});
}

TEST_F(MapProjectionTest, rejectsABaseThatIsNoEntityOrMap) {
    expectError("WITH 1 AS x RETURN x {.a} AS m", "A map projection reads a node, a relationship or a map");
}
