#include "processors/ProcessorTester.h"

#include "SystemManager.h"
#include "SimpleGraph.h"

using namespace db;
using namespace db::v2;
using namespace turing::test;

class ScanNodesProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

TEST_F(ScanNodesProcessorTest, scanNodesVarChunk) {
    LocalMemory mem;

    // Get all node IDs
    std::vector<NodeID> allNodeIDs;
    std::vector<NodeID> resultNodeIDs;
    {
        auto transaction = _graph->openTransaction();
        auto reader = transaction.readGraph();
        auto nodes = reader.scanNodes();
        for (auto node : nodes) {
            allNodeIDs.push_back(node);
        }
    }

    ASSERT_FALSE(allNodeIDs.empty());

    const size_t extra = 1000;
    const size_t maxChunkSize = allNodeIDs.size()+extra;

    for (size_t chunkSize = 1; chunkSize <= maxChunkSize; chunkSize++) {
        // Pipeline definition
        PipelineV2 pipeline;
        PipelineBuilder builder(&mem, &pipeline);
        const ColumnTag scanNodeIDsTag = builder.addScanNodes().getNodeIDs()->getTag();

        resultNodeIDs.clear();
        const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
            if (operation == LambdaProcessor::Operation::RESET) {
                return;
            }

            // Retrieve the results of the current pipeline iteration (chunk)
            EXPECT_EQ(df->size(), 1);

            const ColumnNodeIDs* nodeIDs = df->getColumn<ColumnNodeIDs>(scanNodeIDsTag);
            ASSERT_TRUE(nodeIDs != nullptr);

            resultNodeIDs.insert(resultNodeIDs.end(), nodeIDs->begin(), nodeIDs->end());
        };

        builder.addLambda(callback);

        // Execute pipeline
        {
            std::cout << "Executing chunksize=" << chunkSize << '\n';
            const auto transaction = _graph->openTransaction();
            const GraphView view = transaction.viewGraph();
            ExecutionContext execCtxt(view);
            execCtxt.setChunkSize(chunkSize);
            PipelineExecutor executor(&pipeline, &execCtxt);
            executor.execute();
        }

        ASSERT_EQ(resultNodeIDs.size(), allNodeIDs.size());
        ASSERT_EQ(resultNodeIDs, allNodeIDs);
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
