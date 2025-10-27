#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "ID.h"
#include "LocalMemory.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "Graph.h"
#include "SystemManager.h"
#include "QueryInterpreter.h"
#include "TuringDB.h"
#include "SimpleGraph.h"
#include "QueryTester.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"
#include "versioning/Transaction.h"
#include "writers/GraphWriter.h"

using namespace db;
using namespace turing::test;
using namespace testing;

class MergeQueryTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        _graph = _env->getSystemManager().createGraph("simple");
        SimpleGraph::createSimpleGraph(_graph);

        _interp = std::make_unique<QueryInterpreter>(&_env->getSystemManager(),
                                                     &_env->getJobSystem());
    }

protected:
    Graph* _graph;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreter> _interp {nullptr};
};

TEST_F(MergeQueryTest, TestMerge) {
    QueryTester tester {_env->getMem(), *_interp};
    const auto oldTransaction = _graph->openTransaction();
    const auto oldReader = oldTransaction.readGraph();

    const auto prevNumCommits = oldReader.commits().size();
    const auto prevNumDataParts = oldReader.commits().back().history().allDataparts().size();
    const auto oldNodeCount = oldReader.getNodeCount();
    const auto oldEdgeCount = oldReader.getEdgeCount();

    tester.query("data_part_merge").execute();

    const auto newTransaction = _graph->openTransaction();
    const auto newReader = newTransaction.readGraph();
    const auto newNodeCount = newReader.getNodeCount();
    const auto newEdgeCount = newReader.getEdgeCount();

    ASSERT_NE(prevNumDataParts, 1);
    ASSERT_EQ(prevNumCommits + 1, newReader.commits().size());
    ASSERT_EQ(oldNodeCount, newNodeCount);
    ASSERT_EQ(oldEdgeCount, newEdgeCount);
    ASSERT_EQ(newReader.commits().back().history().allDataparts().size(), 1);
}
