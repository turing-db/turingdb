#include "processors/ProcessorTester.h"

#include "SystemManager.h"
#include "SimpleGraph.h"

using namespace db;
using namespace db::v2;
using namespace turing::test;

class FilterProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

TEST_F(FilterProcessorTest, test) {
    auto [transaction, view, reader] = readGraph();

    std::vector<NodeID> expectedIDs;
    std::vector<NodeID> returnedIDs;
    for (const NodeID nodeID : reader.scanNodes()) {
        if (nodeID.getValue() % 2 == 0) {
            expectedIDs.push_back(nodeID);
        }
    }

    // Pipeline definition
    const ColumnTag nodeIDsTag = _builder->addScanNodes().getNodeIDs()->getTag();

    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        EXPECT_EQ(df->size(), 1);

        const ColumnNodeIDs* nodeIDs = df->getColumn<ColumnNodeIDs>(nodeIDsTag);
        ASSERT_TRUE(nodeIDs != nullptr);

        returnedIDs.insert(returnedIDs.begin(), nodeIDs->begin(), nodeIDs->end());
    };

    _builder->addFilter();
    _builder->addLambda(callback);

    fmt::println("\n- Executing pipeline with chunk size 2...");
    EXECUTE(view, 2);
    ASSERT_EQ(returnedIDs, expectedIDs);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
