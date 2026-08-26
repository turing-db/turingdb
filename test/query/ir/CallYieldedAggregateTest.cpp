#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
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

class CallYieldedAggregateTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runQuery(std::string_view query, NLOutputSink& sink) {
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// CALL db.getNodes([0, 1]) YIELD inEdgeCount RETURN sum(...), min(...), max(...), avg(...):
// the reductions read a plain unsigned column a procedure yielded, as they read a property.
// Two edges point at Remy and one at Adam.
TEST_F(CallYieldedAggregateTest, reducesAYieldedUnsignedColumn) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0, 1]) YIELD inEdgeCount "
             "RETURN sum(inEdgeCount), min(inEdgeCount), max(inEdgeCount), avg(inEdgeCount)",
             sink);

    const std::vector<StringRowSink::Row> expected {{"3", "1", "2", "1.5"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallYieldedAggregateTest, reducesAYieldedUnsignedColumnPerGroup) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0, 1]) YIELD id, inEdgeCount RETURN id, sum(inEdgeCount)", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"0", "2"}, {"1", "1"}};
    EXPECT_EQ(rows, expected);
}
