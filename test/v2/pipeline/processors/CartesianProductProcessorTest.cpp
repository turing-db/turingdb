#include "processors/LambdaProcessor.h"
#include "processors/LambdaSourceProcessor.h"
#include "processors/ProcessorTester.h"

#include "processors/CartesianProductProcessor.h"

#include "SystemManager.h"
#include "SimpleGraph.h"
#include "LineContainer.h"

using namespace db;
using namespace db::v2;
using namespace turing::test;

class CartesianProductProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

TEST_F(CartesianProductProcessorTest, scanNodesProduct) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t OUTPUT_NUM_ROWS = 169;
    constexpr size_t NUM_NODES_IN_SCAN = 13;

    auto& scanNodes2 = _builder->addScanNodes();
    [[maybe_unused]] auto& scanNodes1 = _builder->addScanNodes();

    // scanNodes1 as implicit LHS
    const auto& cartProd = _builder->addCartesianProduct(&scanNodes2);

    ASSERT_EQ(cartProd.getDataframe()->cols().size(), 2);

    const ColumnTag lhsNodes = cartProd.getDataframe()->cols().front()->getTag();
    const ColumnTag rhsNodes = cartProd.getDataframe()->cols().back()->getTag();

    const auto callback = [&](const Dataframe* df,
                              LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        const ColumnNodeIDs* outputLHS = df->getColumn<ColumnNodeIDs>(lhsNodes);
        const ColumnNodeIDs* outputRHS = df->getColumn<ColumnNodeIDs>(rhsNodes);

        size_t rowPtr = 0;
        for (const NodeID actualID : reader.scanNodes()) {
            for (size_t i = 0; i < NUM_NODES_IN_SCAN; i++) {
                ASSERT_EQ(actualID, outputLHS->at(rowPtr));
                rowPtr++;
            }
        }

        for (size_t i = 0; i < OUTPUT_NUM_ROWS;) {
            for (const NodeID actualID : reader.scanNodes()) {
                ASSERT_EQ(actualID, outputRHS->at(i));
                i++;
            }
        }
    };

    _builder->addLambda(callback);
    EXECUTE(view, 13 * 13UL);
}

TEST_F(CartesianProductProcessorTest, remyProdRest) {
    auto [transaction, view, reader] = readGraph();

    const auto genRemyCallback = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }
        ASSERT_EQ(df->size(), 1);
        ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(df->cols().front()->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(nodeIDs->empty());
        nodeIDs->emplace_back(0); // Add Remy
        isFinished = true;
    };
    const auto fakeScanNodesCallback = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }
        ASSERT_EQ(df->size(), 1);
        ColumnNodeIDs* nodeIDs = dynamic_cast<ColumnNodeIDs*>(df->cols().front()->getColumn());
        ASSERT_TRUE(nodeIDs != nullptr);
        ASSERT_TRUE(nodeIDs->empty());
        nodeIDs->resize(13);
        std::iota(nodeIDs->begin(), nodeIDs->end(), 0);
        isFinished = true;
    };

    auto& scanNodesLambda = _builder->addLambdaSource(fakeScanNodesCallback);
    _builder->addColumnToOutput<ColumnNodeIDs>(_pipeline.getDataframeManager()->allocTag());

    [[maybe_unused]] auto& remyIF = _builder->addLambdaSource(genRemyCallback);
    _builder->addColumnToOutput<ColumnNodeIDs>(_pipeline.getDataframeManager()->allocTag());

    const auto& cartProd = _builder->addCartesianProduct(&scanNodesLambda);
    ASSERT_EQ(cartProd.getDataframe()->cols().size(), 2);

    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        spdlog::info("DF:");
        df->dump(std::cout);
    };

    _builder->addLambda(callback);
    EXECUTE(view, 1 * 13UL);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
