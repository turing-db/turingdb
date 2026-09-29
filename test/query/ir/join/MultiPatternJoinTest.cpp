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

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());
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
    // The two hops from b to c need two edges between them, and simpledb has no parallel edge
    expectRows("MATCH (b)-->(c), (a)-->(b)-->(c) RETURN a, b, c;", {});
}

// success-reads-joins-on-filters-5: nothing forces a and b apart, nor c and d, so every pair
// of edges into x is crossed with every pair of two-hop ways out of x onto one e. Remy (0),
// Adam (1) and Ghosts (6) are the only such x, and a is whoever enters them.
TEST_F(MultiPatternJoinTest, joinsTwoWaysIntoANodeWithTwoWaysOutOfIt) {
    // Only Remy is entered twice and left by two 2-hop walks to one node, and those walks
    // end on Remy through the very edges that enter it, so no six edges are distinct
    expectRows("MATCH (a)-->(x), (b)-->(x), (x)-->(c)-->(e), (x)-->(d)-->(e) RETURN a", {});
}

// The four patterns alone produce 9792 rows, with n one of Remy (0), Adam (1) and Ghosts (6).
// Only Remy and Adam carry age, both 32, so p.age/10 is 3 and n never equals it. m.age < 0 is
// false on every row too, so the result is empty either way.
TEST_F(MultiPatternJoinTest, filtersAwayEveryRowOfFourJoinedPatterns) {
    expectRows("MATCH (nl)-->(dp)-->(q), (l)-->(mYIELDp)-->(z), (l)-->(m) "
               "MATCH (n)-->()-->(a), (p)-->(z), (l)-->(Dp)-->(z), (l)-->(m) "
               "MATCH (q) WHERE n = p.age/10 AND m.age < p.age+2/p.age/10 "
               "AND m.age < 0 AND m.age < p.age+2/8 RETURN n, m, z, p",
               {});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
