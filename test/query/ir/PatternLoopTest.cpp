#include <gtest/gtest.h>

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

// The query test suite's fail-reads-loop-0 case on the v3 engine. A pattern whose last node
// is its first walks back to where it started, which the v1 planner turned away as a loop;
// here it is a hop joined to the node it left.
class PatternLoopTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;

        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::vector<StringRowSink::Row> rows;
        sink.sortedRows(rows);

        EXPECT_EQ(rows, expected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// fail-reads-loop-0: simpledb closes a two-hop loop through the two pairs answering each
// other, Remy (0) with Adam (1) and Remy with Ghosts (6), read from either end
TEST_F(PatternLoopTest, walksBackToTheNodeThePatternStartedFrom) {
    const std::vector<StringRowSink::Row> expected {{"0", "1"}, {"0", "6"}, {"1", "0"}, {"6", "0"}};

    expectRows("MATCH (a)-->(b)-->(a) RETURN a, b;", expected);
}

// The same shape one hop longer, which no simpledb edge closes
TEST_F(PatternLoopTest, matchesNothingWhenNoLoopIsThatLong) {
    expectRows("MATCH (a)-->(b)-->(c)-->(a) RETURN a, b, c;", {});
}

// The tally of the rows above, so the loop is read once through its rows and once through
// the count they are charged to
TEST_F(PatternLoopTest, countsTheRowsTheLoopCloses) {
    const std::vector<StringRowSink::Row> expected {{"4"}};

    expectRows("MATCH (a)-->(b)-->(a) RETURN count(*)", expected);
}

// Naming the two legs shows each is bound to an edge of its own: the hop out to b and the
// hop back to a are answered by different edges, never one edge read twice.
TEST_F(PatternLoopTest, bindsTheTwoLegsToDifferentEdges) {
    const std::vector<StringRowSink::Row> expected {{"0", "1", "0", "4"},
                                                    {"0", "6", "1", "7"},
                                                    {"1", "0", "4", "0"},
                                                    {"6", "0", "7", "1"}};

    expectRows("MATCH (a)-[e]->(b)-[f]->(a) RETURN a, b, e, f", expected);
}

TEST_F(PatternLoopTest, namesTheNodesTheLoopRunsThrough) {
    const std::vector<StringRowSink::Row> expected {{"Adam", "Remy"},
                                                    {"Ghosts", "Remy"},
                                                    {"Remy", "Adam"},
                                                    {"Remy", "Ghosts"}};

    expectRows("MATCH (a)-->(b)-->(a) RETURN a.name, b.name", expected);
}

// A loop of one hop is a self-edge, which simpledb holds none of - so the shape is matched
// rather than waved through, as the three-hop one above is
TEST_F(PatternLoopTest, matchesNoSelfEdge) {
    const std::vector<StringRowSink::Row> expected {{"0"}};

    expectRows("MATCH (a)-->(a) RETURN count(*)", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
