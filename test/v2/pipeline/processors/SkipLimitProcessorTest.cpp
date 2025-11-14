#include "processors/ProcessorTester.h"

#include <iostream>

#include "SystemManager.h"
#include "SimpleGraph.h"

class SkipLimitProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

template <typename T>
void assertSpanEqual(const std::span<const T>& a, const std::span<const T>& b) {
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
        ASSERT_EQ(a[i], b[i]);
    }
}

TEST_F(SkipLimitProcessorTest, scanNodesSkip) {
    LocalMemory mem;

    // Get all expected node IDs
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
    
    const size_t extra = 10;
    const size_t maxSkip = allNodeIDs.size()+extra;
    const size_t maxChunkSize = 1 /* allNodeIDs.size()+extra */;
    
    for (size_t chunkSize = 1; chunkSize <= maxChunkSize; chunkSize++) {
        for (size_t skipCount = 0; skipCount < maxSkip; skipCount++) {
            std::cout << "Chunk size=" << chunkSize << " skipCount=" << skipCount << '\n';
            PipelineV2 pipeline;

            PipelineBuilder builder(&mem, &pipeline);
            builder.addScanNodes();
            builder.addMaterialize();
            builder.addSkip(skipCount);

            // Lambda
            resultNodeIDs.clear();
            auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
                if (operation == LambdaProcessor::Operation::RESET) {
                    return;
                }

                ASSERT_EQ(df->size(), 1);
                const ColumnNodeIDs* nodeIDs = dynamic_cast<const ColumnNodeIDs*>(df->cols().front()->getColumn());
                ASSERT_TRUE(nodeIDs != nullptr);
                ASSERT_TRUE(!nodeIDs->empty());

                resultNodeIDs.insert(resultNodeIDs.end(), nodeIDs->getRaw().begin(), nodeIDs->getRaw().end());
            };

            builder.addLambda(callback);

            // Execute pipeline
            const auto transaction = _graph->openTransaction();
            const GraphView view = transaction.viewGraph();
            ExecutionContext execCtxt(view);
            execCtxt.setChunkSize(chunkSize);
            PipelineExecutor executor(&pipeline, &execCtxt);
            executor.execute();

            ASSERT_TRUE(resultNodeIDs.size() <= allNodeIDs.size());
            ASSERT_EQ(resultNodeIDs.size(), allNodeIDs.size()-skipCount);

            const size_t skipCompare = std::min(skipCount, allNodeIDs.size());
            assertSpanEqual(std::span<const NodeID>(resultNodeIDs),
                            std::span<const NodeID>(allNodeIDs).subspan(skipCompare, allNodeIDs.size()-skipCompare));
        }
    }
}
