#include <gtest/gtest.h>

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

// A pattern type and a WHERE type that no single edge can both carry leaves the read with an
// empty type set. These run the query rather than read the plan, because the rows are what
// the rewrite has to leave alone.
class NarrowEdgeTypeReadsQueryTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectCounts(std::string_view query, const Counts& expected) {
        CountSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Counts actual;
        sink.sortedCounts(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

// An edge carries one type, so no edge is both: the emptied read walks nothing and the
// match keeps no row.
TEST_F(NarrowEdgeTypeReadsQueryTest, aContradictoryTypeMatchesNothing) {
    expectCounts("MATCH (n)-[e:KNOWS_WELL]->(m) WHERE e:INTERESTED_IN RETURN count(*)", {0});
}

// The same contradiction under OPTIONAL MATCH must still pad: emptying the read may not turn
// the rows of the outer match into no rows at all. This is what keeps the empty type set on
// the read rather than replacing the read with something that emits nothing.
TEST_F(NarrowEdgeTypeReadsQueryTest, aContradictoryTypeUnderOptionalMatchStillPads) {
    expectCounts("MATCH (n:Person) OPTIONAL MATCH (n)-[e:KNOWS_WELL]->(m) WHERE e:INTERESTED_IN RETURN count(*)",
                 {8});
}

// Narrowing that leaves types behind still returns them: the 3 KNOWS_WELL edges, not the 18.
TEST_F(NarrowEdgeTypeReadsQueryTest, aNarrowedReadReturnsTheRemainingType) {
    expectCounts("MATCH (n)-[e:KNOWS_WELL|INTERESTED_IN]->(m) WHERE e:KNOWS_WELL RETURN count(*)", {3});
}
