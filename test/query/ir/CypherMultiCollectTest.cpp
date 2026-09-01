#include <gtest/gtest.h>

#include <algorithm>
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

// More than one collect() in the same projection. Each is a value column of the one
// accumulator, so the lists come out of a single drain: the group table, the keys and
// the reductions beside them are shared, and only the per-group value buffers multiply.
class CypherMultiCollectTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

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
                              &_env->getMem(),
                              sink);

        return status;
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// Two lists over the whole match: one row, holding both. The names and the identities
// are the same ten interests, collected as values and as entities
TEST_F(CypherMultiCollectTest, collectsTwoValuesOverTheWholeMatch) {
    expectRows("MATCH (i:Interest) RETURN collect(i.name), collect(i)",
               {{"[Computers, Eighties, Bio, Cooking, Ghosts, Padel, Animals, Gym, Travel, JiuJitsu]",
                 "[2, 3, 4, 5, 6, 7, 10, 13, 14, 16]"}});
}

TEST_F(CypherMultiCollectTest, collectsTwoValuesPerGroup) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "RETURN p.name, collect(i.name), collect(i)",
               {{"Remy", "[Ghosts, Computers, Eighties]", "[6, 2, 3]"},
                {"Adam", "[Bio, Cooking]", "[4, 5]"},
                {"Maxime", "[Bio, Padel]", "[4, 7]"},
                {"Luc", "[Animals, Computers]", "[10, 2]"},
                {"Martina", "[Cooking]", "[5]"},
                {"Suhas", "[Gym, JiuJitsu]", "[13, 16]"},
                {"Cyrus", "[Gym, Travel]", "[13, 14]"},
                {"Doruk", "[Gym]", "[13]"}});
}

TEST_F(CypherMultiCollectTest, collectsThreeValuesPerGroup) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "RETURN p.name, collect(i.name), collect(i), collect(p)",
               {{"Remy", "[Ghosts, Computers, Eighties]", "[6, 2, 3]", "[0, 0, 0]"},
                {"Adam", "[Bio, Cooking]", "[4, 5]", "[1, 1]"},
                {"Maxime", "[Bio, Padel]", "[4, 7]", "[8, 8]"},
                {"Luc", "[Animals, Computers]", "[10, 2]", "[9, 9]"},
                {"Martina", "[Cooking]", "[5]", "[11]"},
                {"Suhas", "[Gym, JiuJitsu]", "[13, 16]", "[12, 12]"},
                {"Cyrus", "[Gym, Travel]", "[13, 14]", "[15, 15]"},
                {"Doruk", "[Gym]", "[13]", "[17]"}});
}

// The DISTINCT rides the value column it is written on, so the plain list beside it keeps
// every row: a person repeats in a group once per interest they hold
TEST_F(CypherMultiCollectTest, dedupesOneListAndNotTheOneBesideIt) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "RETURN p.hasPhD, collect(DISTINCT p.name), collect(p.name)",
               {{"true", "[Remy, Adam, Luc, Martina]", "[Remy, Remy, Remy, Adam, Adam, Luc, Luc, Martina]"},
                {"false", "[Maxime, Cyrus, Suhas, Doruk]", "[Maxime, Maxime, Cyrus, Cyrus, Suhas, Suhas, Doruk]"}});
}

TEST_F(CypherMultiCollectTest, dedupesBothListsIndependently) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "RETURN p.hasPhD, collect(DISTINCT p.name), collect(DISTINCT i.name)",
               {{"true", "[Remy, Adam, Luc, Martina]", "[Ghosts, Computers, Eighties, Bio, Cooking, Animals]"},
                {"false", "[Maxime, Cyrus, Suhas, Doruk]", "[Bio, Padel, Gym, Travel, JiuJitsu]"}});
}

// The reductions read the same group table the lists do, so they ride the one op beside
// them rather than grouping the rows a second time
TEST_F(CypherMultiCollectTest, reducesBesideTwoLists) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "RETURN p.name, collect(i.name), collect(i), count(*)",
               {{"Remy", "[Ghosts, Computers, Eighties]", "[6, 2, 3]", "3"},
                {"Adam", "[Bio, Cooking]", "[4, 5]", "2"},
                {"Maxime", "[Bio, Padel]", "[4, 7]", "2"},
                {"Luc", "[Animals, Computers]", "[10, 2]", "2"},
                {"Martina", "[Cooking]", "[5]", "1"},
                {"Suhas", "[Gym, JiuJitsu]", "[13, 16]", "2"},
                {"Cyrus", "[Gym, Travel]", "[13, 14]", "2"},
                {"Doruk", "[Gym]", "[13]", "1"}});
}

TEST_F(CypherMultiCollectTest, aliasesBothLists) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "RETURN p.name AS person, collect(i.name) AS names, collect(i) AS nodes",
               {{"Remy", "[Ghosts, Computers, Eighties]", "[6, 2, 3]"},
                {"Adam", "[Bio, Cooking]", "[4, 5]"},
                {"Maxime", "[Bio, Padel]", "[4, 7]"},
                {"Luc", "[Animals, Computers]", "[10, 2]"},
                {"Martina", "[Cooking]", "[5]"},
                {"Suhas", "[Gym, JiuJitsu]", "[13, 16]"},
                {"Cyrus", "[Gym, Travel]", "[13, 14]"},
                {"Doruk", "[Gym]", "[13]"}});
}

// The sort reads one of the two lists as its key, and carries the other along with it
TEST_F(CypherMultiCollectTest, ordersTheGroupsByOneOfTwoLists) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
                      "RETURN p.name, collect(i.name) AS names, collect(i) AS nodes ORDER BY names",
                      {{"Luc", "[Animals, Computers]", "[10, 2]"},
                       {"Adam", "[Bio, Cooking]", "[4, 5]"},
                       {"Maxime", "[Bio, Padel]", "[4, 7]"},
                       {"Martina", "[Cooking]", "[5]"},
                       {"Remy", "[Ghosts, Computers, Eighties]", "[6, 2, 3]"},
                       {"Doruk", "[Gym]", "[13]"},
                       {"Suhas", "[Gym, JiuJitsu]", "[13, 16]"},
                       {"Cyrus", "[Gym, Travel]", "[13, 14]"}});
}

TEST_F(CypherMultiCollectTest, cutsTheGroupsCarryingTwoLists) {
    expectRowsInOrder("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
                      "RETURN p.name AS person, collect(i.name) AS names, collect(i) AS nodes "
                      "ORDER BY person SKIP 1 LIMIT 2",
                      {{"Cyrus", "[Gym, Travel]", "[13, 14]"},
                       {"Doruk", "[Gym]", "[13]"}});
}

// A constant collected column is folded over the rows of the group it falls in, not over
// the single row it is, so it lands one element per row beside a column that varies
TEST_F(CypherMultiCollectTest, collectsAConstantBesideAVaryingColumn) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) WHERE p.name = 'Adam' "
               "RETURN collect(1), collect(i.name)",
               {{"[1, 1]", "[Bio, Cooking]"}});
}

// A constant tells no two rows apart, so a projection whose only grouping key is one
// groups nothing: the two lists are the one keyless accumulator's, as they are without it
TEST_F(CypherMultiCollectTest, collectsTwoValuesUnderAConstantKey) {
    expectRows("MATCH (i:Interest) RETURN 1 AS x, collect(i.name), collect(i)",
               {{"1",
                 "[Computers, Eighties, Bio, Cooking, Ghosts, Padel, Animals, Gym, Travel, JiuJitsu]",
                 "[2, 3, 4, 5, 6, 7, 10, 13, 14, 16]"}});
}

// A collected list is a value like any other, so DISTINCT over the projected rows reads
// two of them the way it reads two scalars
TEST_F(CypherMultiCollectTest, dedupesTheRowsTwoListsProject) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "RETURN DISTINCT p.name, collect(i.name), collect(i)",
               {{"Remy", "[Ghosts, Computers, Eighties]", "[6, 2, 3]"},
                {"Adam", "[Bio, Cooking]", "[4, 5]"},
                {"Maxime", "[Bio, Padel]", "[4, 7]"},
                {"Luc", "[Animals, Computers]", "[10, 2]"},
                {"Martina", "[Cooking]", "[5]"},
                {"Suhas", "[Gym, JiuJitsu]", "[13, 16]"},
                {"Cyrus", "[Gym, Travel]", "[13, 14]"},
                {"Doruk", "[Gym]", "[13]"}});
}
