#include "processors/ProcessorTester.h"

#include "processors/MaterializeProcessor.h"

#include "SystemManager.h"
#include "SimpleGraph.h"
#include "LineContainer.h"

using namespace db;
using namespace db::v2;
using namespace turing::test;

class GetLabelSetIDProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

TEST_F(GetLabelSetIDProcessorTest, test) {
    auto [transaction, view, reader] = readGraph();

    LineContainer<NodeID, LabelSetID> expected;
    LineContainer<NodeID, LabelSetID> results;
    for (const NodeID nodeID : reader.scanNodes()) {
        expected.add({nodeID, reader.getNodeLabelSet(nodeID).getID()});
    }
    
    // Pipeline definition
    _builder->setMaterializeProc(MaterializeProcessor::create(&_pipeline, &_env->getMem()));
    const ColumnTag nodeIDsTag = _builder->addScanNodes().getNodeIDs()->getTag();

    const auto& labelsetIDInterface = _builder->addGetLabelSetID();
    const ColumnTag labelsetIDsTag = labelsetIDInterface.getValues()->getTag();

    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        // Retrieve the results of the current pipeline iteration (chunk)
        EXPECT_EQ(df->size(), 3);

        const ColumnNodeIDs* nodeIDs = df->getColumn<ColumnNodeIDs>(nodeIDsTag);
        ASSERT_TRUE(nodeIDs != nullptr);

        const ColumnVector<LabelSetID>* labelsetIDs = df->getColumn<ColumnVector<LabelSetID>>(labelsetIDsTag);
        ASSERT_TRUE(nodeIDs != nullptr);

        const size_t lineCount = nodeIDs->size();

        for (size_t i = 0; i < lineCount; i++) {
            results.add({nodeIDs->at(i), labelsetIDs->at(i)});
        }
    };

    _builder->addMaterialize();
    _builder->addLambda(callback);
    EXPECT_TRUE(results.equals(expected));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
