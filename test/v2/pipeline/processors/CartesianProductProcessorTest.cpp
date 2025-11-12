#include <gtest/gtest.h>

#include "iterators/ChunkConfig.h"
#include "processors/ProcessorTester.h"

#include "processors/CartesianProductProcessor.h"
#include "dataframe/Dataframe.h"
#include "processors/LambdaProcessor.h"
#include "processors/LambdaSourceProcessor.h"
#include "columns/ColumnIDs.h"
#include "SystemManager.h"
#include "SimpleGraph.h"

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
    EXECUTE(view, NUM_NODES_IN_SCAN * NUM_NODES_IN_SCAN);
}

TEST_F(CartesianProductProcessorTest, remyProdRest) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_SIZE = 1;
    constexpr size_t RHS_SIZE = 13;

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
        nodeIDs->resize(RHS_SIZE);
        std::iota(nodeIDs->begin(), nodeIDs->end(), 0);
        isFinished = true;
    };

    auto& scanNodesLambda = _builder->addLambdaSource(fakeScanNodesCallback);
    _builder->addColumnToOutput<ColumnNodeIDs>(_pipeline.getDataframeManager()->allocTag());

    [[maybe_unused]] auto& remyIF = _builder->addLambdaSource(genRemyCallback);
    _builder->addColumnToOutput<ColumnNodeIDs>(_pipeline.getDataframeManager()->allocTag());

    const auto& cartProd = _builder->addCartesianProduct(&scanNodesLambda);
    ASSERT_EQ(cartProd.getDataframe()->cols().size(), 2);

    const ColumnTag lhsNodes = cartProd.getDataframe()->cols().front()->getTag();
    const ColumnTag rhsNodes = cartProd.getDataframe()->cols().back()->getTag();

    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        const ColumnNodeIDs* outputLHS = df->getColumn<ColumnNodeIDs>(lhsNodes);
        const ColumnNodeIDs* outputRHS = df->getColumn<ColumnNodeIDs>(rhsNodes);

        ASSERT_EQ(outputLHS->size(), LHS_SIZE * RHS_SIZE);
        ASSERT_EQ(outputRHS->size(), LHS_SIZE * RHS_SIZE);

        {
            NodeID actualID = 0; // All should be Remy
            for (size_t i = 0; i < LHS_SIZE * RHS_SIZE; i++) {
                EXPECT_EQ(actualID, outputLHS->at(i));
            }
        }

        // RHS should be scan nodes (0-12)
        for (size_t i = 0; i < LHS_SIZE * RHS_SIZE;) {
            for (auto actualID : std::views::iota(0UL, RHS_SIZE)) {
                EXPECT_EQ(actualID, outputRHS->at(i));
                i++;
            }
        }
    };

    _builder->addLambda(callback);
    EXECUTE(view, LHS_SIZE * RHS_SIZE);
}

TEST_F(CartesianProductProcessorTest, twoByTwo) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 2;
    constexpr size_t LHS_NUM_COLS = 2;

    constexpr size_t RHS_NUM_ROWS = 2;
    constexpr size_t RHS_NUM_COLS = 2;

    /*
     * Generate Dataframe looking like:
     * 1   2
     * 2   3
     */
    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(LHS_NUM_ROWS);
            std::iota(col->begin(), col->end(), colPtr + 1);
        }
        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        isFinished = true;
    };

    /*
     * Generate Dataframe looking like:
     * 101   102
     * 102   103
     */
    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(RHS_NUM_ROWS);
            std::iota(col->begin(), col->end(), colPtr + 101);
        }
        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        isFinished = true;
    };

    { // Wire up the cartesian product to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < RHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < LHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }

        const auto& cartProd = _builder->addCartesianProduct(&rhsIF);
        ASSERT_EQ(cartProd.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS);
    }

    /*
     * Output Dataframe should be looking like:
     * 1 2 101 102
     * 1 2 102 103
     * 2 3 101 102
     * 2 3 102 103
     */
    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS);

        auto* fstCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(0)->getColumn());
        ASSERT_EQ(fstCol->size(), LHS_NUM_ROWS * RHS_NUM_ROWS);
        for (size_t i {0}; NodeID expected : {1, 1, 2, 2}) {
            EXPECT_EQ(fstCol->at(i++), expected);
        }
        auto* sndCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(1)->getColumn());
        ASSERT_EQ(sndCol->size(), LHS_NUM_ROWS * RHS_NUM_ROWS);
        for (size_t i {0}; NodeID expected : {2, 2, 3, 3}) {
            EXPECT_EQ(sndCol->at(i++), expected);
        }
        auto* thdCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(2)->getColumn());
        ASSERT_EQ(thdCol->size(), LHS_NUM_ROWS * RHS_NUM_ROWS);
        for (size_t i {0}; NodeID expected : {101, 102, 101, 102}) {
            EXPECT_EQ(thdCol->at(i++), expected);
        }
        auto* fthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(3)->getColumn());
        ASSERT_EQ(fthCol->size(), LHS_NUM_ROWS * RHS_NUM_ROWS);
        for (size_t i {0}; NodeID expected : {102, 103, 102, 103}) {
            EXPECT_EQ(fthCol->at(i++), expected);
        }
    };

    _builder->addLambda(callback);
    EXECUTE(view, LHS_NUM_ROWS * RHS_NUM_ROWS);
}

TEST_F(CartesianProductProcessorTest, nonSymmetric) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 5;
    constexpr size_t LHS_NUM_COLS = 2;

    constexpr size_t RHS_NUM_ROWS = 3;
    constexpr size_t RHS_NUM_COLS = 4;

    /*
     * Generate Dataframe looking like:
     * 101 102
     * 103 104
     * 105 106
     * 107 108
     * 109 110
     */
     const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        NodeID nextID = 101;
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(LHS_NUM_ROWS);
            for (size_t rowPtr = 0; rowPtr < LHS_NUM_ROWS; rowPtr++) {
                col->at(rowPtr) = nextID++;
            }
        }
        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        isFinished = true;
    };

    /*
    * Generate Dataframe looking like:
    * 1  2  3
    * 4  5  6
    * 7  8  9
    * 10 11 12
    */
    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        NodeID nextID = 1;
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(RHS_NUM_ROWS);
            for (size_t rowPtr = 0; rowPtr < RHS_NUM_ROWS; rowPtr++) {
                col->at(rowPtr) = nextID++;
            }
        }
        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        isFinished = true;
    };

    { // Wire up the cartesian product to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < RHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < LHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }

        const auto& cartProd = _builder->addCartesianProduct(&rhsIF);
        ASSERT_EQ(cartProd.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS);
    }

    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        df->dump(std::cout);
    };
    _builder->addLambda(callback);
    EXECUTE(view, LHS_NUM_ROWS * RHS_NUM_ROWS);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
