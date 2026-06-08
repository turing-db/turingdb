#include "processors/ProcessorTester.h"

#include "processors/MaterializeProcessor.h"

#include "SystemManager.h"
#include "SimpleGraph.h"
#include "LineContainer.h"

using namespace db;
using namespace turing::test;

class ConstScanProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

TEST_F(ConstScanProcessorTest, constScanGetOutEdges) {
    auto [transaction, view, reader] = readGraph();

    // Pick a subset of nodes to use as constant input
    ColumnNodeIDs inputNodeIDs;
    inputNodeIDs.push_back(SimpleGraph::findNodeID(_graph, "Remy"));
    inputNodeIDs.push_back(SimpleGraph::findNodeID(_graph, "Adam"));
    inputNodeIDs.push_back(SimpleGraph::findNodeID(_graph, "Luc"));

    // Build expected results using the reader
    LineContainer<NodeID, EdgeID, NodeID, EdgeTypeID> expLines;
    const Tombstones& tombstones = view.tombstones();

    for (const NodeID originID : inputNodeIDs) {
        ColumnVector<NodeID> tmpNodeIDs = {originID};

        for (const EdgeRecord& edge : reader.getOutEdges(&tmpNodeIDs)) {
            if (tombstones.contains(edge._edgeID)) {
                continue;
            }

            expLines.add({edge._nodeID, edge._edgeID, edge._otherID, edge._edgeTypeID});
        }
    }

    fmt::println("- Expected results");
    expLines.print(std::cout);

    // Pipeline: ConstScan -> GetOutEdges -> Materialize -> Lambda
    _builder->setMaterializeProc(MaterializeProcessor::create(&_pipeline, &_env->getMem()));

    const ColumnTag originIDsTag =
        _builder->addConstScan(&inputNodeIDs).getValues()->getTag();

    const auto& edgeInterface = _builder->addGetOutEdges();
    const ColumnTag edgeIDsTag = edgeInterface.getEdgeIDs()->getTag();
    const ColumnTag edgeTypesTag = edgeInterface.getEdgeTypes()->getTag();
    const ColumnTag otherIDsTag = edgeInterface.getOtherNodes()->getTag();

    LineContainer<NodeID, EdgeID, NodeID, EdgeTypeID> resLines;

    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        EXPECT_EQ(df->size(), 4);

        const ColumnNodeIDs* originIDs = df->getColumn<ColumnNodeIDs>(originIDsTag);
        ASSERT_TRUE(originIDs != nullptr);

        const ColumnEdgeIDs* edgeIDs = df->getColumn<ColumnEdgeIDs>(edgeIDsTag);
        ASSERT_TRUE(edgeIDs != nullptr);

        const ColumnNodeIDs* otherIDs = df->getColumn<ColumnNodeIDs>(otherIDsTag);
        ASSERT_TRUE(otherIDs != nullptr);

        const ColumnEdgeTypes* edgeTypes = df->getColumn<ColumnEdgeTypes>(edgeTypesTag);
        ASSERT_TRUE(edgeTypes != nullptr);

        const size_t lineCount = originIDs->size();
        ASSERT_EQ(edgeIDs->size(), lineCount);
        ASSERT_EQ(otherIDs->size(), lineCount);
        ASSERT_EQ(edgeTypes->size(), lineCount);

        for (size_t i = 0; i < lineCount; i++) {
            resLines.add({originIDs->at(i), edgeIDs->at(i), otherIDs->at(i), edgeTypes->at(i)});
        }
    };

    _builder->addMaterialize();
    _builder->addLambda(callback);

    for (const size_t chunkSize : {100, 10, 2, 1}) {
        fmt::println("\n- Executing pipeline with chunk size {}...", chunkSize);
        resLines.clear();
        EXECUTE(view, chunkSize);
        resLines.print(std::cout);
        EXPECT_TRUE(resLines.equals(expLines));
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
