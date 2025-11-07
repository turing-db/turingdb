#include "processors/ProcessorTester.h"

#include "SystemManager.h"
#include "SimpleGraph.h"
#include "LineContainer.h"

using namespace db;
using namespace db::v2;
using namespace turing::test;

class GetInEdgesProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

TEST_F(GetInEdgesProcessorTest, test) {
    auto [transaction, view, reader] = readGraph();

    LineContainer<NodeID, EdgeID, NodeID, EdgeTypeID> expLines;
    LineContainer<NodeID, EdgeID, NodeID, EdgeTypeID> resLines;
    const Tombstones& tombstones = view.tombstones();

    for (const NodeID tgtID : reader.scanNodes()) {
        ColumnVector<NodeID> tmpNodeIDs = {tgtID};

        for (const EdgeRecord& edge : reader.getInEdges(&tmpNodeIDs)) {
            if (tombstones.contains(edge._edgeID)) {
                continue;
            }

            expLines.add({edge._nodeID, edge._edgeID, edge._otherID, edge._edgeTypeID});
        }
    }

    fmt::println("- Expected results");
    expLines.print(std::cout);

    // Pipeline definition
    const ColumnTag tgtIDsTag = _builder->addScanNodes().getNodeIDs()->getTag();

    const auto edgeInterface = _builder->addGetInEdges();
    const ColumnTag edgeIDsTag = edgeInterface.getEdgeIDs()->getTag();
    const ColumnTag edgeTypesTag = edgeInterface.getEdgeTypes()->getTag();
    const ColumnTag srcIDsTag = edgeInterface.getOtherNodes()->getTag();

    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        // Retrieve the results of the current pipeline iteration (chunk)
        EXPECT_EQ(df->size(), 4);

        const ColumnNodeIDs* tgtIDs = df->getColumn<ColumnNodeIDs>(tgtIDsTag);
        ASSERT_TRUE(tgtIDs != nullptr);

        const ColumnEdgeIDs* edgeIDs = df->getColumn<ColumnEdgeIDs>(edgeIDsTag);
        ASSERT_TRUE(edgeIDs != nullptr);

        const ColumnNodeIDs* srcIDs = df->getColumn<ColumnNodeIDs>(srcIDsTag);
        ASSERT_TRUE(srcIDs != nullptr);

        const ColumnEdgeTypes* edgeTypes = df->getColumn<ColumnEdgeTypes>(edgeTypesTag);
        ASSERT_TRUE(edgeTypes != nullptr);

        const size_t lineCount = tgtIDs->size();
        ASSERT_EQ(edgeIDs->size(), lineCount);
        ASSERT_EQ(srcIDs->size(), lineCount);
        ASSERT_EQ(edgeTypes->size(), lineCount);

        for (size_t i = 0; i < lineCount; i++) {
            resLines.add({tgtIDs->at(i), edgeIDs->at(i), srcIDs->at(i), edgeTypes->at(i)});
        }
    };

    _builder->addMaterialize();
    _builder->addLambda(callback);

    fmt::println("\n- Executing pipeline with chunk size 100...");
    EXECUTE(view, 100);
    resLines.print(std::cout);
    EXPECT_TRUE(resLines.equals(expLines));

    fmt::println("\n- Executing pipeline with chunk size 10...");
    resLines.clear();
    EXECUTE(view, 10);
    resLines.print(std::cout);
    EXPECT_TRUE(resLines.equals(expLines));

    fmt::println("\n- Executing pipeline with chunk size 2...");
    resLines.clear();
    EXECUTE(view, 2);
    resLines.print(std::cout);
    EXPECT_TRUE(resLines.equals(expLines));

    fmt::println("\n- Executing pipeline with chunk size 1...");
    resLines.clear();
    EXECUTE(view, 1);
    resLines.print(std::cout);
    EXPECT_TRUE(resLines.equals(expLines));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
