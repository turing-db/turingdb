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

// A node or an edge gathered by collect() comes back out of the list as the entity it
// went in as, so a later part of the query reads its properties and matches on it, as it
// would on one the pattern bound.
class CollectedEntityIndexTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
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

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(CollectedEntityIndexTest, readsThePropertyOfTheFirstCollectedNode) {
    expectRows("MATCH (p:Person) WITH p ORDER BY p.name "
               "WITH collect(p) AS people "
               "WITH people[0] AS first "
               "RETURN first.name",
               {{"Adam"}});
}

TEST_F(CollectedEntityIndexTest, readsThePropertyOfTheLastCollectedNode) {
    expectRows("MATCH (p:Person) WITH p ORDER BY p.name "
               "WITH collect(p) AS people, count(p) AS total "
               "WITH people[total - 1] AS last "
               "RETURN last.name",
               {{"Suhas"}});
}

TEST_F(CollectedEntityIndexTest, matchesOnAnIndexedNode) {
    expectRows("MATCH (p:Person) WITH p ORDER BY p.name "
               "WITH collect(p) AS people "
               "WITH people[0] AS first "
               "MATCH (first)-[:INTERESTED_IN]->(i) "
               "RETURN i.name",
               {{"Bio"}, {"Cooking"}});
}

// Adam and Cyrus are the first two Persons by name, and only Adam knows anybody well
TEST_F(CollectedEntityIndexTest, matchesOnTwoIndexedNodesOfOneList) {
    expectRows("MATCH (p:Person) WITH p ORDER BY p.name "
               "WITH collect(p) AS people "
               "WITH people[0] AS first, people[1] AS second "
               "OPTIONAL MATCH (first)-[:KNOWS_WELL]->(f) "
               "OPTIONAL MATCH (second)-[:KNOWS_WELL]->(s) "
               "RETURN first.name, f.name, second.name, s.name",
               {{"Adam", "Remy", "Cyrus", "null"}});
}

TEST_F(CollectedEntityIndexTest, carriesASecondPickThroughAHopOfTheFirst) {
    expectRows("MATCH (p:Person) WITH p ORDER BY p.name "
               "WITH collect(p) AS people "
               "WITH people[0] AS first, people[1] AS second "
               "MATCH (first)-[:KNOWS_WELL]->(known) "
               "RETURN first.name, known.name, second.name",
               {{"Adam", "Remy", "Cyrus"}});
}

TEST_F(CollectedEntityIndexTest, joinsTwoPatternsAnchoredOnTwoPicks) {
    expectRows("MATCH (p:Person) WITH p ORDER BY p.name "
               "WITH collect(p) AS people "
               "WITH people[0] AS first, people[1] AS second "
               "MATCH (first)-[:KNOWS_WELL]->(known), (second)-[:INTERESTED_IN]->(i) "
               "RETURN first.name, known.name, second.name, i.name",
               {{"Adam", "Remy", "Cyrus", "Gym"},
                {"Adam", "Remy", "Cyrus", "Travel"}});
}

// Each interest keeps the fan that sorts first by name
TEST_F(CollectedEntityIndexTest, indexesTheListOfEveryGroup) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH i, p ORDER BY p.name "
               "WITH i, collect(p) AS fans "
               "WITH i, fans[0] AS lead "
               "RETURN i.name, lead.name",
               {{"Animals", "Luc"},
                {"Bio", "Adam"},
                {"Computers", "Luc"},
                {"Cooking", "Adam"},
                {"Eighties", "Remy"},
                {"Ghosts", "Remy"},
                {"Gym", "Cyrus"},
                {"JiuJitsu", "Suhas"},
                {"Padel", "Maxime"},
                {"Travel", "Cyrus"}});
}

TEST_F(CollectedEntityIndexTest, readsAPositionPastTheEndAsANullNode) {
    expectRows("MATCH (p:Person) WITH collect(p) AS people "
               "WITH people[99] AS missing "
               "RETURN missing.name",
               {{"null"}});
}

TEST_F(CollectedEntityIndexTest, readsThePropertyOfACollectedEdge) {
    expectRows("MATCH (p:Person)-[e:KNOWS_WELL]->(q) WITH e ORDER BY e.name "
               "WITH collect(e) AS edges "
               "WITH edges[0] AS first "
               "RETURN first.name, first.duration",
               {{"Adam -> Remy", "20"}});
}

TEST_F(CollectedEntityIndexTest, carriesAnIndexedNodeThroughAFurtherBarrier) {
    expectRows("MATCH (p:Person) WITH p ORDER BY p.name "
               "WITH collect(p) AS people "
               "WITH people[1] AS second "
               "WITH second, second.name AS name "
               "RETURN name, second.hasPhD",
               {{"Cyrus", "false"}});
}

TEST_F(CollectedEntityIndexTest, collectsTheIndexedNodesAgain) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) WITH i, p ORDER BY p.name "
               "WITH i, collect(p) AS fans "
               "WITH fans[0] AS lead "
               "RETURN count(DISTINCT lead)",
               {{"6"}});
}
