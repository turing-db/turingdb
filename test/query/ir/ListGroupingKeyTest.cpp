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

namespace {

// The interests of each of the eight simpledb people, which is what a WITH publishing a
// collect leaves for the next barrier to group on: eight lists, no two of them equal, so
// grouping on the list alone tells every person's rows apart.
const Rows interestListsWithOneRowEach = {
    {"[Ghosts, Computers, Eighties]", "1"},
    {"[Bio, Cooking]", "1"},
    {"[Bio, Padel]", "1"},
    {"[Animals, Computers]", "1"},
    {"[Cooking]", "1"},
    {"[Gym, JiuJitsu]", "1"},
    {"[Gym, Travel]", "1"},
    {"[Gym]", "1"},
};

const Rows interestLists = {
    {"[Ghosts, Computers, Eighties]"},
    {"[Bio, Cooking]"},
    {"[Bio, Padel]"},
    {"[Animals, Computers]"},
    {"[Cooking]"},
    {"[Gym, JiuJitsu]"},
    {"[Gym, Travel]"},
    {"[Gym]"},
};

}

// A list is a value of the language like any other: two of them are equal when their
// elements are, so a list column groups and dedups on that equality. A grouping key or a
// DISTINCT over a collected list is valid Cypher, and the rows it reduces are the ones
// whose lists are equal.
class ListGroupingKeyTest : public TuringTest {
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

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ListGroupingKeyTest, groupsOnACollectedList) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p, collect(i.name) AS interests "
               "WITH interests, count(*) AS people RETURN interests, people",
               interestListsWithOneRowEach);
}

TEST_F(ListGroupingKeyTest, dedupsOnACollectedList) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i:Interest) "
               "WITH p, collect(i.name) AS interests RETURN DISTINCT interests",
               interestLists);
}

// One list crossed with the ten interests: the whole cross product is the single group
// the one list keys, so the count is every row of it.
TEST_F(ListGroupingKeyTest, groupsOnAListCarriedAcrossACrossProduct) {
    expectRows("MATCH (p:Person) WHERE p.name = 'Remy' WITH collect(p.name) AS people "
               "MATCH (i:Interest) RETURN people, count(*)",
               {{"[Remy]", "10"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
