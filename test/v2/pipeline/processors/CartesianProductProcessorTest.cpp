#include <gtest/gtest.h>

#include <range/v3/view/zip.hpp>

#include "EdgeRecord.h"
#include "ID.h"
#include "LineContainer.h"
#include "dataframe/ColumnTag.h"
#include "iterators/ChunkConfig.h"
#include "processors/ProcessorTester.h"

#include "processors/CartesianProductProcessor.h"
#include "dataframe/Dataframe.h"
#include "processors/LambdaProcessor.h"
#include "processors/LambdaSourceProcessor.h"
#include "processors/MaterializeProcessor.h"
#include "columns/ColumnIDs.h"
#include "SystemManager.h"
#include "SimpleGraph.h"

using namespace db;
using namespace db::v2;
using namespace turing::test;
namespace rg = ranges;
namespace rv = rg::views;

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

    _builder->setMaterializeProc(MaterializeProcessor::create(&_pipeline, &_env->getMem()));
    auto& scanNodes2 = _builder->addScanNodes();
    _builder->setMaterializeProc(MaterializeProcessor::create(&_pipeline, &_env->getMem()));
    [[maybe_unused]] auto& scanNodes1 = _builder->addScanNodes();

    // scanNodes1 as implicit LHS
    const auto& cartProd = _builder->addCartesianProduct(&scanNodes2);

    ASSERT_EQ(cartProd.getDataframe()->cols().size(), 2);

    const ColumnTag lhsNodes = cartProd.getDataframe()->cols().front()->getTag();
    const ColumnTag rhsNodes = cartProd.getDataframe()->cols().back()->getTag();

    bool executed {false};
    const auto VERIFY_CALLBACK = [&](const Dataframe* df,
                              LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;

        const auto* outputLHS = df->getColumn<ColumnNodeIDs>(lhsNodes);
        const auto* outputRHS = df->getColumn<ColumnNodeIDs>(rhsNodes);

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

    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, NUM_NODES_IN_SCAN * NUM_NODES_IN_SCAN);
    ASSERT_TRUE(executed);
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
        auto* nodeIDs = dynamic_cast<ColumnNodeIDs*>(df->cols().front()->getColumn());
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
        auto* nodeIDs = dynamic_cast<ColumnNodeIDs*>(df->cols().front()->getColumn());
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

    bool executed {false};
    const auto VERIFY_CALLBACK = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;

        const auto* outputLHS = df->getColumn<ColumnNodeIDs>(lhsNodes);
        const auto* outputRHS = df->getColumn<ColumnNodeIDs>(rhsNodes);

        ASSERT_EQ(outputLHS->size(), LHS_SIZE * RHS_SIZE);
        ASSERT_EQ(outputRHS->size(), LHS_SIZE * RHS_SIZE);

        {
            NodeID actualID = 0; // All should be Remy
            for (size_t i = 0; i < LHS_SIZE * RHS_SIZE; i++) {
                EXPECT_EQ(actualID.getValue(), outputLHS->at(i).getValue());
            }
        }

        // RHS should be scan nodes (0-12)
        for (size_t i = 0; i < LHS_SIZE * RHS_SIZE;) {
            for (auto actualID : std::views::iota(0UL, RHS_SIZE)) {
                EXPECT_EQ(actualID, outputRHS->at(i).getValue());
                i++;
            }
        }
    };

    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, LHS_SIZE * RHS_SIZE);
    ASSERT_TRUE(executed);
}

TEST_F(CartesianProductProcessorTest, twoByTwo) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 2;
    constexpr size_t LHS_NUM_COLS = 2;

    constexpr size_t RHS_NUM_ROWS = 2;
    constexpr size_t RHS_NUM_COLS = 2;

     // Generate Dataframe looking like:
     //  1   2
     //  2   3
    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS; colPtr++) {
            auto* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(LHS_NUM_ROWS);
            std::iota(col->begin(), col->end(), colPtr + 1);
        }
        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        isFinished = true;
    };

     // Generate Dataframe looking like:
     // 101   102
     // 102   103
     //
    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS; colPtr++) {
            auto* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
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

    bool executed {false};
     // Output Dataframe should be looking like:
     // 1 2 101 102
     // 1 2 102 103
     // 2 3 101 102
     // 2 3 102 103
    const auto VERIFY_CALLBACK = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;

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

    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, LHS_NUM_ROWS * RHS_NUM_ROWS);
    ASSERT_TRUE(executed);
}

TEST_F(CartesianProductProcessorTest, nonSymmetric) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 5;
    constexpr size_t LHS_NUM_COLS = 2;

    constexpr size_t RHS_NUM_ROWS = 4;
    constexpr size_t RHS_NUM_COLS = 3;

     // Generate Dataframe looking like:
     // 101 106
     // 102 107
     // 103 108
     // 104 109
     // 105 110
     const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        NodeID nextID = 101;
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS; colPtr++) {
            auto* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
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

    // Generate Dataframe looking like:
    // 1  5  9
    // 2  6  10
    // 3  7  11
    // 4  8  12
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

    //  Output should be:
    //  101 106 1  5  9
    //  101 106 2  6  10
    //  101 106 3  7  11
    //  101 106 4  8  12
    //
    //  102 107 1  5  9
    //  102 107 2  6  10
    //  102 107 3  7  11
    //  102 107 4  8  12
    //
    //  103 108 1  5  9
    //  103 108 2  6  10
    //  103 108 3  7  11
    //  103 108 4  8  12
    //
    //  104 109 1  5  9
    //  104 109 2  6  10
    //  104 109 3  7  11
    //  104 109 4  8  12
    //
    //  105 110 1  5  9
    //  105 110 2  6  10
    //  105 110 3  7  11
    //  105 110 4  8  12
    bool executed {false};
    const auto VERIFY_CALLBACK = [&](const Dataframe* df,
                                     LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS);

        {
            auto* fstCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(0)->getColumn());
            ASSERT_EQ(fstCol->size(), LHS_NUM_ROWS * RHS_NUM_ROWS);
            const ColumnNodeIDs expected {101, 101, 101, 101, 102, 102, 102,
                                          102, 103, 103, 103, 103, 104, 104,
                                          104, 104, 105, 105, 105, 105};
            for (const auto& [exp, actual] : rv::zip(expected, *fstCol)) {
                EXPECT_EQ(exp, actual);
            }
        }

        {
            auto* sndCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(1)->getColumn());
            ASSERT_EQ(sndCol->size(), LHS_NUM_ROWS * RHS_NUM_ROWS);

            const ColumnNodeIDs expected {106, 106, 106, 106, 107, 107, 107,
                                          107, 108, 108, 108, 108, 109, 109,
                                          109, 109, 110, 110, 110, 110};
            for (const auto& [exp, actual] : rv::zip(expected, *sndCol)) {
                EXPECT_EQ(exp, actual);
            }
        }

        {
            auto* thdCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(2)->getColumn());
            ASSERT_EQ(thdCol->size(), LHS_NUM_ROWS * RHS_NUM_ROWS);

            const ColumnNodeIDs expected {
                1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4,
            };
            for (const auto& [exp, actual] : rv::zip(expected, *thdCol)) {
                EXPECT_EQ(exp, actual);
            }
        }

        {
            auto* frtCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(3)->getColumn());
            ASSERT_EQ(frtCol->size(), LHS_NUM_ROWS * RHS_NUM_ROWS);

            const ColumnNodeIDs expected {
                5, 6, 7, 8, 5, 6, 7, 8, 5, 6, 7, 8, 5, 6, 7, 8, 5, 6, 7, 8,
            };
            for (const auto& [exp, actual] : rv::zip(expected, *frtCol)) {
                EXPECT_EQ(exp, actual);
            }
        }

        {
            auto* fthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(4)->getColumn());
            ASSERT_EQ(fthCol->size(), LHS_NUM_ROWS * RHS_NUM_ROWS);

            const ColumnNodeIDs expected {
                9, 10, 11, 12, 9, 10, 11, 12, 9, 10, 11, 12, 9, 10, 11, 12, 9, 10, 11, 12,
            };
            for (const auto& [exp, actual] : rv::zip(expected, *fthCol)) {
                EXPECT_EQ(exp, actual);
            }
        }
    };

    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, LHS_NUM_ROWS * RHS_NUM_ROWS);
    ASSERT_TRUE(executed);
}

TEST_F(CartesianProductProcessorTest, spanningChunksSimple) {
    using StringCol = ColumnVector<std::string>;

    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 3;
    constexpr size_t LHS_NUM_COLS = 1;

    constexpr size_t RHS_NUM_ROWS = 5;
    constexpr size_t RHS_NUM_COLS = 1;

    constexpr size_t CHUNK_SIZE = (LHS_NUM_ROWS * RHS_NUM_ROWS) / 2;

    constexpr size_t EXP_NUM_CHUNKS = LHS_NUM_ROWS * RHS_NUM_ROWS / CHUNK_SIZE + 1;

    constexpr std::array<size_t, EXP_NUM_CHUNKS> EXPECTED_CHUNK_SIZES = {
        CHUNK_SIZE, CHUNK_SIZE, (LHS_NUM_ROWS * RHS_NUM_ROWS) - 2 * CHUNK_SIZE};

     // Generate Dataframe looking like:
     // a
     // b
     // c
    const auto genLDF = [&](Dataframe* df, bool& isFinished, LambdaSourceProcessor::Operation operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS; colPtr++) {
            auto* col = dynamic_cast<StringCol*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->push_back("a");
            col->push_back("b");
            col->push_back("c");
        }
        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        isFinished = true;
    };

     //  Generate Dataframe looking like:
     //  v
     //  w
     //  x
     //  y
     //  z
    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS; colPtr++) {
            auto* col =
                dynamic_cast<StringCol*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->push_back("v");
            col->push_back("w");
            col->push_back("x");
            col->push_back("y");
            col->push_back("z");
        }
        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        isFinished = true;
    };

    { // Wire up the cartesian product to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < RHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<StringCol>(
                _pipeline.getDataframeManager()->allocTag());
        }

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < LHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<StringCol>(
                _pipeline.getDataframeManager()->allocTag());
        }

        const auto& cartProd = _builder->addCartesianProduct(&rhsIF);
        ASSERT_EQ(cartProd.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS);
    }

    size_t numChunks = 0;
    std::vector<size_t> chunkSizes;
    StringCol expectedFstCol {
        "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "c", "c", "c", "c", "c",
    };
    StringCol expectedSndCol {
        "v", "w", "x", "y", "z", "v", "w", "x", "y", "z", "v", "w", "x", "y", "z",
    };
    auto expFstColIt = begin(expectedFstCol);
    auto expSndColIt = begin(expectedSndCol);
    bool executed {false};
    const auto VERIFY_CALLBACK = [&](const Dataframe* df, auto operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;

        numChunks++;
        chunkSizes.push_back(df->getRowCount());
        {
            auto* fstCol = dynamic_cast<StringCol*>(df->cols().at(0)->getColumn());
            for (const std::string& actual : *fstCol) {
                EXPECT_EQ(*expFstColIt, actual);
                expFstColIt++;
            }
        }
        {
            auto* sndCol = dynamic_cast<StringCol*>(df->cols().at(1)->getColumn());
            for (const std::string& actual : *sndCol) {
                EXPECT_EQ(*expSndColIt, actual);
                expSndColIt++;
            }
        }
    };

    _builder->addLambda(VERIFY_CALLBACK);
    {
        EXECUTE(view, CHUNK_SIZE);
        ASSERT_TRUE(executed);
        EXPECT_EQ(EXP_NUM_CHUNKS, numChunks);
        for (const auto [expected, actual] : rv::zip(EXPECTED_CHUNK_SIZES, chunkSizes)) {
            EXPECT_EQ(expected, actual);
        }
    }
}

TEST_F(CartesianProductProcessorTest, spanningChunksMultiCol) {
    using StringCol = ColumnVector<std::string>;

    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 3;
    constexpr size_t LHS_NUM_COLS = 3;

    constexpr size_t RHS_NUM_ROWS = 4;
    constexpr size_t RHS_NUM_COLS = 2;

    constexpr size_t CHUNK_SIZE = (LHS_NUM_ROWS * RHS_NUM_ROWS) / 2 - 1;

    constexpr size_t EXP_NUM_CHUNKS = LHS_NUM_ROWS * RHS_NUM_ROWS / CHUNK_SIZE + 1;

    [[maybe_unused]] constexpr std::array<size_t, EXP_NUM_CHUNKS> EXPECTED_CHUNK_SIZES = {
        CHUNK_SIZE, CHUNK_SIZE, (LHS_NUM_ROWS * RHS_NUM_ROWS) - 2 * CHUNK_SIZE};

     // Generate Dataframe looking like:
     // a x p
     // b y q
     // c z r
    const auto genLDF = [&](Dataframe* df, bool& isFinished, LambdaSourceProcessor::Operation operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        {
            auto* col = dynamic_cast<StringCol*>(df->cols()[0]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->push_back("a");
            col->push_back("b");
            col->push_back("c");
        }
        {
            auto* col = dynamic_cast<StringCol*>(df->cols()[1]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->push_back("x");
            col->push_back("y");
            col->push_back("z");
        }
        {
            auto* col = dynamic_cast<StringCol*>(df->cols()[2]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->push_back("p");
            col->push_back("q");
            col->push_back("r");
        }

        isFinished = true;
    };

     // Generate Dataframe looking like:
     // 1 5
     // 2 6
     // 3 7
     // 4 8
    const auto genRDF = [&](Dataframe* df, bool& isFinished, LambdaSourceProcessor::Operation operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        {
            auto* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[0]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->push_back(1);
            col->push_back(2);
            col->push_back(3);
            col->push_back(4);
        }
        {
            auto* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[1]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->push_back(5);
            col->push_back(6);
            col->push_back(7);
            col->push_back(8);
        }

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
            _builder->addColumnToOutput<StringCol>(
                _pipeline.getDataframeManager()->allocTag());
        }

        const auto& cartProd = _builder->addCartesianProduct(&rhsIF);
        ASSERT_EQ(cartProd.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS);
    }

    size_t numChunks = 0;
    std::vector<size_t> chunkSizes;
    const StringCol expectedFstCol {
        "a", "a", "a", "a", "b", "b", "b", "b","c", "c", "c", "c",
    };
    const StringCol expectedSndCol {
        "x", "x", "x", "x", "y", "y", "y", "y", "z", "z", "z", "z",
    };
    const StringCol expectedThdCol {
        "p", "p", "p", "p", "q", "q", "q", "q", "r", "r", "r", "r",
    };
    const ColumnNodeIDs expectedFrtCol {
        1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4,
    };
    const ColumnNodeIDs expectedFthCol {
        5, 6, 7, 8, 5, 6, 7, 8, 5, 6, 7, 8,
    };

    auto expFstColIt = begin(expectedFstCol);
    auto expSndColIt = begin(expectedSndCol);
    auto expThdColIt = begin(expectedThdCol);
    auto expFrtColIt = std::begin(expectedFrtCol);
    auto expFthColIt = std::begin(expectedFthCol);
    bool executed {false};
    const auto VERIFY_CALLBACK = [&](const Dataframe* df, auto operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;

        numChunks++;
        chunkSizes.push_back(df->getRowCount());

        {
            auto* fstCol = dynamic_cast<StringCol*>(df->cols().at(0)->getColumn());
            for (const std::string& actual : *fstCol) {
                EXPECT_EQ(*expFstColIt, actual);
                expFstColIt++;
            }
        }
        {
            auto* sndCol = dynamic_cast<StringCol*>(df->cols().at(1)->getColumn());
            for (const std::string& actual : *sndCol) {
                EXPECT_EQ(*expSndColIt, actual);
                expSndColIt++;
            }
        }
        {
            auto* thdCol = dynamic_cast<StringCol*>(df->cols().at(2)->getColumn());
            for (const std::string& actual : *thdCol) {
                EXPECT_EQ(*expThdColIt, actual);
                expThdColIt++;
            }
        }
        {
            auto* frtCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(3)->getColumn());
            for (const NodeID actual : *frtCol) {
                EXPECT_EQ(expFrtColIt->getValue(), actual.getValue());
                expFrtColIt++;
            }
        }
        {
            auto* fthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(4)->getColumn());
            for (const NodeID actual : *fthCol) {
                EXPECT_EQ(expFthColIt->getValue(), actual.getValue());
                expFthColIt++;
            }
        }
    };

    _builder->addLambda(VERIFY_CALLBACK);
    {
        EXECUTE(view, CHUNK_SIZE);
        ASSERT_TRUE(executed);
        EXPECT_EQ(EXP_NUM_CHUNKS, numChunks);
        for (const auto [expected, actual] : rv::zip(EXPECTED_CHUNK_SIZES, chunkSizes)) {
            EXPECT_EQ(expected, actual);
        }
    }
}

TEST_F(CartesianProductProcessorTest, scanNodesChunkSize3) {
    using NodeIDTuple = std::pair<NodeID, NodeID>;

    auto [transaction, view, reader] = readGraph();

    constexpr size_t CHUNK_SIZE = 3;
    constexpr size_t L_COLS = 1;
    constexpr size_t R_COLS = 1;
    constexpr size_t L_ROWS = 13;
    constexpr size_t R_ROWS = 13;

    _builder->setMaterializeProc(MaterializeProcessor::create(&_pipeline, &_env->getMem()));
    auto& scanNodes2 = _builder->addScanNodes();
    [[maybe_unused]] auto& scanNodes1 = _builder->addScanNodes();

    // scanNodes1 as implicit LHS
    const auto& cartProd = _builder->addCartesianProduct(&scanNodes2);

    ASSERT_EQ(cartProd.getDataframe()->cols().size(), 2);

    [[maybe_unused]] const ColumnTag lhsNodes = cartProd.getDataframe()->cols().front()->getTag();
    [[maybe_unused]] const ColumnTag rhsNodes = cartProd.getDataframe()->cols().back()->getTag();

    // We should have n x n for all nodes n
    std::set<NodeIDTuple> expectedTuples;
    {
        for (const NodeID l : reader.scanNodes()) {
            for (const NodeID r : reader.scanNodes()) {
                expectedTuples.emplace(l, r);
            }
        }
    }

    bool executed {false};
    size_t numChunks {0};
    std::set<NodeIDTuple> actualTuples;
    const auto VERIFY_CALLBACK = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;
        ASSERT_EQ(df->size(), L_COLS + R_COLS);
        const auto* lCol = df->cols().front()->as<ColumnNodeIDs>();
        const auto* rCol = df->cols().back()->as<ColumnNodeIDs>();

        const size_t rowCount = df->getRowCount();
        for (size_t rowPtr = 0; rowPtr < rowCount; rowPtr++) {
            const NodeID left = lCol->at(rowPtr);
            const NodeID right = rCol->at(rowPtr);
            actualTuples.emplace(left, right);
        }
        numChunks++;
    };

    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, CHUNK_SIZE);
    {
        ASSERT_TRUE(executed);
        size_t expectedChunks = std::ceil((L_ROWS * R_ROWS) / (float) CHUNK_SIZE);
        EXPECT_EQ(expectedChunks, numChunks);
        EXPECT_EQ(expectedTuples.size(), actualTuples.size());
        for (const auto& expected : expectedTuples) {
            EXPECT_TRUE(actualTuples.contains(expected));
        }
    }
}

TEST_F(CartesianProductProcessorTest, scanNodesXgetOutEdges) {
    using Rows = LineContainer<NodeID, NodeID, EdgeID, EdgeTypeID, NodeID>;
    auto [transaction, view, reader] = readGraph();

    _builder->setMaterializeProc(MaterializeProcessor::create(&_pipeline, &_env->getMem()));
    _builder->addScanNodes();
    _builder->addGetOutEdges();
    auto& matProc = _builder->addMaterialize();

    _builder->setMaterializeProc(MaterializeProcessor::create(&_pipeline, &_env->getMem()));
    _builder->addScanNodes();

    // scanNodes as implicit LHS
    [[maybe_unused]] const auto& cartProd = _builder->addCartesianProduct(&matProc);

    Rows expectedRows;
    {
        for (const NodeID lhs: reader.scanNodes()) {
            for (const NodeID rhsSrc : reader.scanNodes()) {
                ColumnNodeIDs src = {rhsSrc};
                for (const EdgeRecord rhs : reader.getOutEdges(&src)) {
                    expectedRows.add(
                        {lhs, rhs._nodeID, rhs._edgeID, rhs._edgeTypeID, rhs._otherID});
                }
            }
        }
    }

    Rows actualRows;
    bool executed {false};
    const auto VERIFY_CALLBACK = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;

        ASSERT_EQ(df->size(), expectedRows.lineSize());
        size_t rowCount = df->getRowCount();

        const auto* lhs = df->cols().at(0)->as<ColumnNodeIDs>();
        ASSERT_TRUE(lhs);
        const auto* rhsSrc = df->cols().at(1)->as<ColumnNodeIDs>();
        ASSERT_TRUE(rhsSrc);
        const auto* rhsEdge = df->cols().at(2)->as<ColumnEdgeIDs>();
        ASSERT_TRUE(rhsEdge);
        const auto* rhsType = df->cols().at(3)->as<ColumnEdgeTypes>();
        ASSERT_TRUE(rhsType);
        const auto* rhsTgt = df->cols().at(4)->as<ColumnNodeIDs>();
        ASSERT_TRUE(rhsTgt);

        for (size_t rowPtr = 0; rowPtr < rowCount; rowPtr++) {
            actualRows.add({lhs->at(rowPtr), rhsSrc->at(rowPtr), rhsEdge->at(rowPtr),
                            rhsType->at(rowPtr), rhsTgt->at(rowPtr)});
        }
    };
    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, 1000); // Some large chunk size that means we can fit all in one chunk
    {
        ASSERT_TRUE(executed);
        EXPECT_EQ(expectedRows.size(), actualRows.size());
        EXPECT_TRUE(actualRows.equals(expectedRows));
    }
}

TEST_F(CartesianProductProcessorTest, scanNodesx2ChunkSize3) {
    using Rows = LineContainer<NodeID, NodeID>;

    auto [transaction, view, reader] = readGraph();

    constexpr size_t CHUNK_SIZE = 3;
    constexpr size_t L_COLS = 1;
    constexpr size_t R_COLS = 1;
    // constexpr size_t L_ROWS = 13;
    // constexpr size_t R_ROWS = 2;

    // Make a dataframe of
    // 999
    // 998
    const auto genRDF = [&](Dataframe* df, bool& isFinished, LambdaSourceProcessor::Operation operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), R_COLS);
        {
            auto* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[0]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->push_back(999);
            col->push_back(998);
        }
        isFinished = true;
    };

    { // Wire up the cartesian product to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < R_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }

        _builder->setMaterializeProc(MaterializeProcessor::create(&_pipeline, &_env->getMem()));
        [[maybe_unused]] auto& scanNodes1 = _builder->addScanNodes();
        const auto& cartProd = _builder->addCartesianProduct(&rhsIF);
        ASSERT_EQ(cartProd.getDataframe()->cols().size(), L_COLS + R_COLS);
    }

    Rows expected;
    {
        for (const NodeID id : reader.scanNodes()) {
            expected.add({id, 999});
            expected.add({id, 998});
        }
    }

    Rows actual;
    bool executed {false};
    const auto VERIFY_CALLBACK = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;

        ASSERT_EQ(df->size(), 2);
        const auto* lhs = df->cols().front()->as<ColumnNodeIDs>();
        const auto* rhs = df->cols().back()->as<ColumnNodeIDs>();
        const size_t rowCount = df->getRowCount();

        for (size_t row = 0; row < rowCount; row++) {
            actual.add({lhs->at(row), rhs->at(row)});
        }
    };

    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, CHUNK_SIZE);
    ASSERT_TRUE(executed);
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(CartesianProductProcessorTest, x2xGetOutEdgesChunkSize5) {
    using Rows = LineContainer<NodeID, NodeID, EdgeID, EdgeTypeID, NodeID>;

    auto [transaction, view, reader] = readGraph();

    constexpr size_t CHUNK_SIZE = 5;
    constexpr size_t L_COLS = 4;
    constexpr size_t R_COLS = 1;

    // Make a dataframe of
    // 999
    // 998
    const auto genLDF = [&](Dataframe* df, bool& isFinished, LambdaSourceProcessor::Operation operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), R_COLS);
        {
            auto* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[0]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->push_back(999);
            col->push_back(998);
        }
        isFinished = true;
    };

    { // Wire up the cartesian product to the two inputs
        _builder->setMaterializeProc(MaterializeProcessor::create(&_pipeline, &_env->getMem()));
        _builder->addScanNodes();
        _builder->addGetOutEdges();
        auto& matProc = _builder->addMaterialize();

        _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < R_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        [[maybe_unused]] const auto& cartProd = _builder->addCartesianProduct(&matProc);
    }

    Rows expected;
    {
        ColumnNodeIDs allNodes;
        for (const NodeID node : reader.scanNodes()) {
            allNodes.push_back(node);
        }

        for (const NodeID val : {999, 998}) {
            for (const EdgeRecord rec : reader.getOutEdges(&allNodes)) {
                expected.add({val, rec._nodeID, rec._edgeID, rec._edgeTypeID, rec._otherID});
            }
        }
    }

    Rows actual;
    bool executed {false};
    const auto VERIFY_CALLBACK = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;
        ASSERT_EQ(df->size(), L_COLS + R_COLS);

        const size_t rowCount = df->getRowCount();
        const auto* lhs = df->cols().at(0)->as<ColumnNodeIDs>();
        ASSERT_TRUE(lhs);
        const auto* rhsSrc = df->cols().at(1)->as<ColumnNodeIDs>();
        ASSERT_TRUE(rhsSrc);
        const auto* rhsEdge = df->cols().at(2)->as<ColumnEdgeIDs>();
        ASSERT_TRUE(rhsEdge);
        const auto* rhsType = df->cols().at(3)->as<ColumnEdgeTypes>();
        ASSERT_TRUE(rhsType);
        const auto* rhsTgt = df->cols().at(4)->as<ColumnNodeIDs>();
        ASSERT_TRUE(rhsTgt);
        for (size_t rowPtr = 0; rowPtr < rowCount; rowPtr++) {
            actual.add({lhs->at(rowPtr), rhsSrc->at(rowPtr), rhsEdge->at(rowPtr),
                            rhsType->at(rowPtr), rhsTgt->at(rowPtr)});
        }

    };

    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, CHUNK_SIZE);
    ASSERT_TRUE(executed);
    EXPECT_TRUE(expected.equals(actual));
}

TEST_F(CartesianProductProcessorTest, bothOneRow) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 1;
    constexpr size_t LHS_NUM_COLS = 5;

    constexpr size_t RHS_NUM_ROWS = 1;
    constexpr size_t RHS_NUM_COLS = 6;

     // Generate Dataframe looking like:
     //  1  2  3  4  5
    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS; colPtr++) {
            auto* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(LHS_NUM_ROWS);
            col->front() = colPtr + 1;
        }
        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        isFinished = true;
    };

     // Generate Dataframe looking like:
     // 101 102 103 104 105 106
    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS; colPtr++) {
            auto* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(RHS_NUM_ROWS);
            col->front() = 100 + colPtr + 1;
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

    bool executed {false};
    // Output Dataframe should be looking like:
    std::vector<NodeID> expectedRow = {1, 2, 3, 4, 5, 101, 102, 103, 104, 105, 106};
    std::vector<NodeID> actualRow;
    const auto VERIFY_CALLBACK = [&](const Dataframe* df,
                                     LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < df->size(); colPtr++) {
            const auto* col = df->cols().at(colPtr)->as<ColumnNodeIDs>();
            actualRow.push_back(col->front());
        };
    };

    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, LHS_NUM_ROWS * RHS_NUM_ROWS);
    ASSERT_TRUE(executed);
    for (const auto& [exp, act] : rv::zip(expectedRow, actualRow)) {
        EXPECT_EQ(exp.getValue(),act.getValue());
    }
}

TEST_F(CartesianProductProcessorTest, tenThousandRowDataframes) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t CHUNK_SIZE = ChunkConfig::CHUNK_SIZE;

    // Anything greater than this becomes too expensive
    constexpr size_t LHS_ROWS = 10'000;
    constexpr size_t LHS_COLS = 1;
    constexpr size_t RHS_ROWS = LHS_ROWS;
    constexpr size_t RHS_COLS = LHS_COLS;

    size_t EXPECTED_NUM_CHUNKS = std::ceil((LHS_ROWS * RHS_ROWS) / (float)CHUNK_SIZE);

    const auto genDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }
        ASSERT_EQ(df->size(), LHS_COLS);
        auto* col = df->cols().front()->as<ColumnNodeIDs>();
        col->resize(LHS_ROWS);
        std::iota(col->begin(), col->end(), 0);

        isFinished = true;
    };

    { // Wire up the cartesian product to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genDF);
        for (size_t i = 0; i < RHS_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genDF);
        for (size_t i = 0; i < LHS_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }

        const auto& cartProd = _builder->addCartesianProduct(&rhsIF);
        ASSERT_EQ(cartProd.getDataframe()->cols().size(), LHS_COLS + RHS_COLS);
    }

    // It is too expensive to compare for equality
    /*
    Rows expectedRows;
    {
        for (size_t i = 0; i < LHS_ROWS; i++) {
            for (size_t j = 0; j < RHS_ROWS; j++) {
                expectedRows.emplace(i, j);
            }
        }
    }
    */

    bool executed {false};
    size_t numChunks {0};
    size_t numRows {0};
    const auto VERIFY_CALLBACK = [&](const Dataframe* df,
                                     LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        ASSERT_EQ(df->size(), LHS_COLS + RHS_COLS);

        executed = true;
        numChunks++;
        numRows += df->getRowCount();

        /*
        for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
            auto* lhs = df->cols().front()->as<ColumnNodeIDs>();
            auto* rhs = df->cols().back()->as<ColumnNodeIDs>();
            EXPECT_TRUE(expectedRows.contains({lhs->at(rowPtr), rhs->at(rowPtr)}));
        }
        */
    };
    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, CHUNK_SIZE);
    ASSERT_TRUE(executed);
    EXPECT_EQ(EXPECTED_NUM_CHUNKS, numChunks);
    EXPECT_EQ(LHS_ROWS * RHS_ROWS, numRows);
}

TEST_F(CartesianProductProcessorTest, lhsEmpty) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 0;
    constexpr size_t LHS_NUM_COLS = 3;

    constexpr size_t RHS_NUM_ROWS = 3;
    constexpr size_t RHS_NUM_COLS = 2;

     // Generate empty Dataframe
    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        // Empty DF
        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        ASSERT_EQ(df->getRowCount(), LHS_NUM_ROWS);
        isFinished = true;
    };

     // Generate Dataframe looking like:
     // 101 102 103 104 105 106
    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS; colPtr++) {
            auto* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(RHS_NUM_ROWS);
            col->front() = 100 + colPtr + 1;
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

    bool executed {false};
    // Output Dataframe should be looking like:
    std::vector<NodeID> actualRow;
    const auto VERIFY_CALLBACK = [&](const Dataframe* df,
                                     LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS);
        // Push back any rows we see, but we shouldn't have any
        for (size_t colPtr = 0; colPtr < df->size(); colPtr++) {
            for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
                const auto* col = df->cols().at(colPtr)->as<ColumnNodeIDs>();
                actualRow.push_back(col->at(rowPtr));
            }
        };
    };

    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(executed);
    ASSERT_TRUE(actualRow.empty());
}

TEST_F(CartesianProductProcessorTest, rhsEmpty) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 3;
    constexpr size_t LHS_NUM_COLS = 2;

    constexpr size_t RHS_NUM_ROWS = 0;
    constexpr size_t RHS_NUM_COLS = 2;

     // Generate empty Dataframe
    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        // Empty DF
        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        ASSERT_EQ(df->getRowCount(), RHS_NUM_ROWS);
        isFinished = true;
    };

     // Generate Dataframe looking like:
     // 101 102 103 104 105 106
    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS; colPtr++) {
            auto* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(LHS_NUM_ROWS);
            col->front() = 100 + colPtr + 1;
        }
        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        isFinished = true;
    };

    { // Wire up the cartesian product to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < RHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < LHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }

        const auto& cartProd = _builder->addCartesianProduct(&rhsIF);
        ASSERT_EQ(cartProd.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS);
    }

    bool executed {false};
    // Output Dataframe should be looking like:
    std::vector<NodeID> actualRow;
    const auto VERIFY_CALLBACK = [&](const Dataframe* df,
                                     LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS);
        // Push back any rows we see, but we shouldn't have any
        for (size_t colPtr = 0; colPtr < df->size(); colPtr++) {
            for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
                const auto* col = df->cols().at(colPtr)->as<ColumnNodeIDs>();
                actualRow.push_back(col->at(rowPtr));
            }
        };
    };

    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(executed);
    ASSERT_TRUE(actualRow.empty());
}

TEST_F(CartesianProductProcessorTest, bothEmpty) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 0;
    constexpr size_t LHS_NUM_COLS = 2;

    constexpr size_t RHS_NUM_ROWS = 0;
    constexpr size_t RHS_NUM_COLS = 2;

     // Generate empty Dataframe
    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        // Empty DF
        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        ASSERT_EQ(df->getRowCount(), RHS_NUM_ROWS);
        isFinished = true;
    };

     // Generate Dataframe looking like:
     // 101 102 103 104 105 106
    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        // Empty DF
        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        ASSERT_EQ(df->getRowCount(), LHS_NUM_ROWS);
        isFinished = true;
    };

    { // Wire up the cartesian product to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < RHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < LHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }

        const auto& cartProd = _builder->addCartesianProduct(&rhsIF);
        ASSERT_EQ(cartProd.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS);
    }

    bool executed {false};
    // Output Dataframe should be looking like:
    std::vector<NodeID> actualRow;
    const auto VERIFY_CALLBACK = [&](const Dataframe* df,
                                     LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        executed = true;

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS);
        // Push back any rows we see, but we shouldn't have any
        for (size_t colPtr = 0; colPtr < df->size(); colPtr++) {
            for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
                const auto* col = df->cols().at(colPtr)->as<ColumnNodeIDs>();
                actualRow.push_back(col->at(rowPtr));
            }
        };
    };

    _builder->addLambda(VERIFY_CALLBACK);
    EXECUTE(view, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(executed);
    ASSERT_TRUE(actualRow.empty());
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 20;
    });
}
