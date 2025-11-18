#include <gtest/gtest.h>

#include <math.h>

#include "SystemManager.h"
#include "Graph.h"
#include "TuringDB.h"

#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "views/GraphView.h"
#include "SimpleGraph.h"
#include "LocalMemory.h"

#include "PipelineV2.h"
#include "PipelineBuilder.h"
#include "PipelineExecutor.h"
#include "ExecutionContext.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace db::v2;
using namespace turing::test;

class CountProcessorTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }

protected:
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(CountProcessorTest, multiChunkSourceWithMaterialize) {
    // Get all expected node IDs
    std::vector<NodeID> allNodeIDs;
    std::vector<std::tuple<NodeID, NodeID>> allSourceTargetIDs;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (NodeID node : nodes) {
            allNodeIDs.push_back(node);
            const auto edgeView = reader.getNodeView(node).edges();
            for (const EdgeRecord& outEdge : edgeView.outEdges()) {
                allSourceTargetIDs.emplace_back(node, outEdge._otherID);
            }
        }
    }

    ASSERT_FALSE(allNodeIDs.empty());

    const size_t extra = 10;
    const size_t maxChunkSize = allNodeIDs.size()+extra;

    for (size_t chunkSize = 1; chunkSize < maxChunkSize; chunkSize++) {
        LocalMemory mem;
        PipelineV2 pipeline;
        PipelineBuilder builder(&mem, &pipeline);

        builder.addScanNodes();
        builder.addGetOutEdges();
        builder.addMaterialize();
        builder.addCount();

        bool sinkExecuted = false;
        size_t rowCount = 0;
        auto lambda = [&](const Dataframe* df, auto operation) -> void {
            if (operation != LambdaProcessor::Operation::EXECUTE) {
                return;
            }

            sinkExecuted = true;

            const ColumnConst<size_t>* countValue = df->cols().front()->as<ColumnConst<size_t>>();
            rowCount = countValue->getRaw();
        };

        builder.addLambda(lambda);

        // Execute pipeline
        const auto transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        ExecutionContext execCtxt(view);
        execCtxt.setChunkSize(chunkSize);
        PipelineExecutor executor(&pipeline, &execCtxt);
        executor.execute();

        ASSERT_TRUE(sinkExecuted);
        ASSERT_EQ(rowCount, allSourceTargetIDs.size());
    }
}
