#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

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

// A map or list literal that holds a grouping key beside an aggregate is built once per
// group, so the key inside it reads the grouped column. Out-degree of each person: Remy 4,
// Adam 3, Maxime, Luc, Suhas and Cyrus 2, Martina and Doruk 1. Only Remy and Adam have an
// age, both 32.
class GroupingKeyInContainerTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, RowSink* sink, QueryStatus& status) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              sink);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        runQuery(query, &sink, status);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectError(std::string_view query, std::string_view message) {
        RowSink sink;
        QueryStatus status;
        runQuery(query, &sink, status);

        ASSERT_FALSE(status.isOk()) << "query: " << query << " was expected to fail";
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(GroupingKeyInContainerTest, mapReadsTheGroupingKeyBesideACount) {
    expectRows("MATCH (n:Person) RETURN n.name, {k: n.name, v: count(n)}",
               {{"Remy", "{k: Remy, v: 1}"}, {"Adam", "{k: Adam, v: 1}"},
                {"Maxime", "{k: Maxime, v: 1}"}, {"Luc", "{k: Luc, v: 1}"},
                {"Martina", "{k: Martina, v: 1}"}, {"Suhas", "{k: Suhas, v: 1}"},
                {"Cyrus", "{k: Cyrus, v: 1}"}, {"Doruk", "{k: Doruk, v: 1}"}});
}

TEST_F(GroupingKeyInContainerTest, listReadsTheGroupingKeyBesideACount) {
    expectRows("MATCH (n:Person) RETURN n.name, [n.name, count(n)]",
               {{"Remy", "[Remy, 1]"}, {"Adam", "[Adam, 1]"},
                {"Maxime", "[Maxime, 1]"}, {"Luc", "[Luc, 1]"},
                {"Martina", "[Martina, 1]"}, {"Suhas", "[Suhas, 1]"},
                {"Cyrus", "[Cyrus, 1]"}, {"Doruk", "[Doruk, 1]"}});
}

TEST_F(GroupingKeyInContainerTest, mapReadsAKeyBoundAboveTheCountedTraversal) {
    expectRows("MATCH (n:Person)-->(x) RETURN n.name, {k: n.name, v: count(x)}",
               {{"Remy", "{k: Remy, v: 4}"}, {"Adam", "{k: Adam, v: 3}"},
                {"Maxime", "{k: Maxime, v: 2}"}, {"Luc", "{k: Luc, v: 2}"},
                {"Martina", "{k: Martina, v: 1}"}, {"Suhas", "{k: Suhas, v: 2}"},
                {"Cyrus", "{k: Cyrus, v: 2}"}, {"Doruk", "{k: Doruk, v: 1}"}});
}

TEST_F(GroupingKeyInContainerTest, mapReadsAPropertyOfAGroupedNode) {
    expectRows("MATCH (n:Person)-->(x) RETURN n, {k: n.name, v: count(x)}",
               {{"0", "{k: Remy, v: 4}"}, {"1", "{k: Adam, v: 3}"},
                {"8", "{k: Maxime, v: 2}"}, {"9", "{k: Luc, v: 2}"},
                {"11", "{k: Martina, v: 1}"}, {"12", "{k: Suhas, v: 2}"},
                {"15", "{k: Cyrus, v: 2}"}, {"17", "{k: Doruk, v: 1}"}});
}

TEST_F(GroupingKeyInContainerTest, mapComputesOverTheGroupingKey) {
    expectRows("MATCH (n:Person) RETURN n.age, {k: n.age + 1, v: count(n)}",
               {{"32", "{k: 33, v: 2}"}, {"null", "{k: null, v: 6}"}});
}

TEST_F(GroupingKeyInContainerTest, mapReadsTheGroupingKeyBesideACollect) {
    expectRows("MATCH (n:Person)-->(x) WHERE n.age = 32 RETURN n.name, {isRemy: n.name = 'Remy', targets: collect(x.name)}",
               {{"Remy", "{isRemy: true, targets: [Adam, Ghosts, Computers, Eighties]}"},
                {"Adam", "{isRemy: false, targets: [Remy, Bio, Cooking]}"}});
}

TEST_F(GroupingKeyInContainerTest, withCarriesTheMapPastTheGrouping) {
    expectRows("MATCH (n:Person)-->(x) WITH n.name AS name, {k: n.name, v: count(x)} AS m RETURN m",
               {{"{k: Remy, v: 4}"}, {"{k: Adam, v: 3}"},
                {"{k: Maxime, v: 2}"}, {"{k: Luc, v: 2}"},
                {"{k: Martina, v: 1}"}, {"{k: Suhas, v: 2}"},
                {"{k: Cyrus, v: 2}"}, {"{k: Doruk, v: 1}"}});
}

// openCypher TCK Return6 [20]: an expression beside an aggregate that the projection does
// not return as a key is ambiguous, so the container cannot stand in for the key
TEST_F(GroupingKeyInContainerTest, rejectsAContainerReadingAnExpressionThatIsNoKey) {
    const std::string_view message = "An expression beside an aggregate may only read the grouping keys";

    expectError("MATCH (n:Person) RETURN {k: n.name, v: count(n)}", message);
    expectError("MATCH (n:Person) RETURN [n.name, count(n)]", message);
    expectError("MATCH (n:Person)-->(x) RETURN {k: n.name, v: count(x)}", message);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
