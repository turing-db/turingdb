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

// The query test suite's success-reads-match-9 and success-reads-joins-on-filters-5 cases on
// the v3 engine. Both name the same variable in patterns that reach it from either end, the
// shape the v1 planner turned away as a common successor joined with a common ancestor.
class MultiPatternJoinTest : public TuringTest {
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

// success-reads-match-9: the second pattern already spells the first, so the rows are the
// two-hop walks a -> b -> c of simpledb - out of Remy (0), Adam (1) and Ghosts (6), the only
// nodes an edge both enters and leaves
TEST_F(MultiPatternJoinTest, joinsAPatternWithTheWalkThatContainsIt) {
    const std::vector<StringRowSink::Row> expected {{"0", "1", "0"},
                                                    {"0", "1", "4"},
                                                    {"0", "1", "5"},
                                                    {"0", "6", "0"},
                                                    {"1", "0", "1"},
                                                    {"1", "0", "2"},
                                                    {"1", "0", "3"},
                                                    {"1", "0", "6"},
                                                    {"6", "0", "1"},
                                                    {"6", "0", "2"},
                                                    {"6", "0", "3"},
                                                    {"6", "0", "6"}};

    expectRows("MATCH (b)-->(c), (a)-->(b)-->(c) RETURN a, b, c;", expected);
}

// success-reads-joins-on-filters-5: nothing forces a and b apart, nor c and d, so every pair
// of edges into x is crossed with every pair of two-hop ways out of x onto one e. Remy (0),
// Adam (1) and Ghosts (6) are the only such x, and a is whoever enters them.
TEST_F(MultiPatternJoinTest, joinsTwoWaysIntoANodeWithTwoWaysOutOfIt) {
    std::vector<StringRowSink::Row> expected;
    expected.insert(expected.end(), 8, {"0"});
    expected.insert(expected.end(), 12, {"1"});
    expected.insert(expected.end(), 12, {"6"});

    expectRows("MATCH (a)-->(x), (b)-->(x), (x)-->(c)-->(e), (x)-->(d)-->(e) RETURN a", expected);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
