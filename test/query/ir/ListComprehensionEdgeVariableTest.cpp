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

// A comprehension body that names the relationship variable of the pattern it is read on.
// The edge column is in flight like any other, so the body reads the edge of its own row.
// simpledb holds three KNOWS_WELL edges: Remy -> Adam and Adam -> Remy, both of duration
// 20, and Ghosts -> Remy of duration 200.
class ListComprehensionEdgeVariableTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
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

TEST_F(ListComprehensionEdgeVariableTest, readsAnEdgePropertyInTheProjection) {
    expectRows("MATCH (a)-[r:KNOWS_WELL]->(b) RETURN a.name, [x IN [1,2] | r.duration]",
               {{"Remy", "[20, 20]"},
                {"Adam", "[20, 20]"},
                {"Ghosts", "[200, 200]"}});
}

TEST_F(ListComprehensionEdgeVariableTest, readsAnEdgePropertyInThePredicate) {
    expectRows("MATCH (a)-[r:KNOWS_WELL]->(b) RETURN a.name, [x IN [20,200] WHERE x = r.duration]",
               {{"Remy", "[20]"},
                {"Adam", "[20]"},
                {"Ghosts", "[200]"}});
}

TEST_F(ListComprehensionEdgeVariableTest, readsTheEdgeTypeOfItsOwnRow) {
    expectRows("MATCH (a)-[r:KNOWS_WELL]->(b) RETURN a.name, [x IN [1,2] | type(r)]",
               {{"Remy", "[KNOWS_WELL, KNOWS_WELL]"},
                {"Adam", "[KNOWS_WELL, KNOWS_WELL]"},
                {"Ghosts", "[KNOWS_WELL, KNOWS_WELL]"}});
}

// The list it built, one element per row: the value of every element is the duration of
// that row's edge, not of whichever edge sits at the element's own index.
TEST_F(ListComprehensionEdgeVariableTest, unwindsTheEdgePropertyItProjected) {
    expectRows("MATCH (a)-[r:KNOWS_WELL]->(b) UNWIND [x IN [1,2] | r.duration] AS d "
               "RETURN a.name, d",
               {{"Remy", "20"},
                {"Remy", "20"},
                {"Adam", "20"},
                {"Adam", "20"},
                {"Ghosts", "200"},
                {"Ghosts", "200"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
