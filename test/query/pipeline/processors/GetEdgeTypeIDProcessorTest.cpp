#include "columns/ColumnIDs.h"
#include "processors/ProcessorTester.h"

#include "processors/MaterializeProcessor.h"

#include "SystemManager.h"
#include "SimpleGraph.h"
#include "LineContainer.h"

using namespace db;
using namespace turing::test;

class GetEdgeTypeIDProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

TEST_F(GetEdgeTypeIDProcessorTest, test) {
    auto [transaction, view, reader] = readGraph();

    LineContainer<EdgeID, EdgeTypeID> expected;
    LineContainer<EdgeID, EdgeTypeID> results;
    for (const EdgeRecord& e : reader.scanOutEdges()) {
        expected.add({e._edgeID, e._edgeTypeID});
    }

    // Pipeline definition
    _builder->setMaterializeProc(MaterializeProcessor::create(&_pipeline, &_env->getMem()));
    _builder->addScanNodes();
    const ColumnTag edgeIDsTag = _builder->addGetOutEdges().getEdgeIDs()->getTag();

    const auto& edgeTypeIDInterface = _builder->addGetEdgeTypeID();
    const ColumnTag edgeTypeIDsTag = edgeTypeIDInterface.getValues()->getTag();

    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        // Retrieve the results of the current pipeline iteration (chunk)
        ASSERT_EQ(df->size(), 5); // srcs, edge ids, tgts, edgetypeids (getoutedges), edgetypeids

        const ColumnEdgeIDs* edgeIDs = df->getColumn<ColumnEdgeIDs>(edgeIDsTag);
        ASSERT_TRUE(edgeIDs != nullptr);

        const ColumnVector<EdgeTypeID>* edgeTypeIDs = df->getColumn<ColumnVector<EdgeTypeID>>(edgeTypeIDsTag);
        ASSERT_TRUE(edgeTypeIDs != nullptr);

        const size_t lineCount = edgeIDs->size();

        for (size_t i = 0; i < lineCount; i++) {
            results.add({edgeIDs->at(i), edgeTypeIDs->at(i)});
        }
    };

    _builder->addMaterialize();
    _builder->addLambda(callback);

    EXECUTE(view, 1);
    EXPECT_TRUE(results.equals(expected));

    results.clear();
    EXECUTE(view, 10);
    EXPECT_TRUE(results.equals(expected));

    results.clear();
    EXECUTE(view, 1000);
    EXPECT_TRUE(results.equals(expected));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
