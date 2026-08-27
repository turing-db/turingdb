#include <gtest/gtest.h>

#include <stdint.h>

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

namespace {

using Rows = std::vector<StringRowSink::Row>;
using ColumnNames = std::vector<std::string>;

constexpr uint64_t simpleGraphNodeCount = 18;

// The three elements of the reported list, as the cells they render to: a bool, a string
// and an integer, each keeping its own type in the one column.
const std::vector<std::string> reportedElements = {"true", "string", "10"};

// Every node of SimpleGraph - they are numbered 0 through 17 - paired with @param element.
void fillNodeRowsWithElement(std::string_view element, Rows& rows) {
    for (uint64_t nodeID = 0; nodeID < simpleGraphNodeCount; nodeID++) {
        rows.push_back({std::to_string(nodeID), std::string(element)});
    }
}

}

// The query test suite's unwind-hetero-list case on the v3 engine:
// UNWIND [true, "string", 10] AS row MATCH (n) RETURN n, row
//
// The UNWIND comes first, so the elements drive and the node scan is what runs for each of
// them - where the same pairs written the other way round, MATCH before UNWIND, scan once
// and unwind per node. Neither is an ORDER BY, but the two walk the pairs in opposite
// orders, and the elements share no type either way: the unwound column is type-erased and
// each cell carries its own tag.
class UnwindHeterogeneousListTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    void expectNamedRows(std::string_view query, const ColumnNames& names, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getNames(), names) << "query: " << query;
        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// unwind-hetero-list: every element paired with every node, the elements leading
TEST_F(UnwindHeterogeneousListTest, unwindsTheReportedList) {
    Rows expected;
    for (const std::string& element : reportedElements) {
        fillNodeRowsWithElement(element, expected);
    }

    expectNamedRows("UNWIND [true, \"string\", 10] AS row MATCH (n) RETURN n, row", {"n", "row"}, expected);
}

// The same pairs with the MATCH driving instead: the nodes lead and each unwinds the three
// elements, so the rows come out node by node rather than element by element.
TEST_F(UnwindHeterogeneousListTest, pairsTheSameRowsNodeByNodeWhenTheMatchDrives) {
    Rows expected;
    for (uint64_t nodeID = 0; nodeID < simpleGraphNodeCount; nodeID++) {
        for (const std::string& element : reportedElements) {
            expected.push_back({std::to_string(nodeID), element});
        }
    }

    expectRows("MATCH (n) UNWIND [true, \"string\", 10] AS row RETURN n, row", expected);
}

TEST_F(UnwindHeterogeneousListTest, countsThePairsOfTheReportedList) {
    const Rows expected = {{std::to_string(reportedElements.size() * simpleGraphNodeCount)}};
    expectRows("UNWIND [true, \"string\", 10] AS row MATCH (n) RETURN count(*)", expected);
}

// The scan the elements drive is a filtered one: only Remy (0) survives it, so each element
// keeps the one row rather than 18.
TEST_F(UnwindHeterogeneousListTest, pairsEveryElementWithAFilteredMatch) {
    Rows expected;
    for (const std::string& element : reportedElements) {
        expected.push_back({"0", element});
    }

    expectRows("UNWIND [true, \"string\", 10] AS row MATCH (n) WHERE n.name = 'Remy' RETURN n, row", expected);
}

// The unwound column as a grouping key: cells of three different types group apart, each
// tallying the nodes it was paired with.
TEST_F(UnwindHeterogeneousListTest, talliesTheNodesOfEachElement) {
    Rows expected;
    for (const std::string& element : reportedElements) {
        expected.push_back({element, std::to_string(simpleGraphNodeCount)});
    }

    expectRows("UNWIND [true, \"string\", 10] AS row MATCH (n) RETURN row, count(n)", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
