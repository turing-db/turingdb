#include <gtest/gtest.h>

#include "LineContainer.h"
#include "processors/ProcessorTester.h"

#include "processors/HashJoinProcessor.h"
#include "dataframe/Dataframe.h"
#include "processors/LambdaProcessor.h"
#include "processors/LambdaSourceProcessor.h"
#include "columns/ColumnIDs.h"
#include "SystemManager.h"
#include "SimpleGraph.h"

using namespace db;
using namespace db::v2;
using namespace turing::test;

class HashJoinProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

// A very simple unchunked join case
TEST_F(HashJoinProcessorTest, simpleJoinCase) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 5;
    constexpr size_t LHS_NUM_COLS = 3;

    constexpr size_t RHS_NUM_ROWS = 5;
    constexpr size_t RHS_NUM_COLS = 3;

    /*
     * Generate Dataframe looking like:
     * 101   201   1
     * 102   202   2
     * 103   203   3
     * 104   204   4
     * 105   205   5
     */

    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(LHS_NUM_ROWS);
            std::iota(col->begin(), col->end(), (colPtr + 1) * 100 + 1);
        }

        // Alloc Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[LHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        ASSERT_TRUE(col->empty());
        col->resize(LHS_NUM_ROWS);
        std::iota(col->begin(), col->end(), +1);

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        isFinished = true;
    };

    /*
     * Generate Dataframe looking like:
     * 301   401   1
     * 302   402   2
     * 303   403   3
     * 304   404   4
     * 305   405   5
     */

    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(RHS_NUM_ROWS);

            std::iota(col->begin(), col->end(), (colPtr + LHS_NUM_COLS) * (100) + 1);
        }

        // Allocate The Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[RHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        ASSERT_TRUE(col->empty());
        col->resize(RHS_NUM_ROWS);
        std::iota(col->begin(), col->end(), +1);

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        isFinished = true;
    };
    ColumnTag joinKey {0};
    { // Wire up the hash join to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < RHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        joinKey = _builder->getPendingOutput().getDataframe()->cols().back()->getTag();

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < LHS_NUM_COLS - 1; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        _builder->addColumnToOutput<ColumnNodeIDs>(joinKey);

        const auto& hashJoin = _builder->addHashJoin(&rhsIF, joinKey, joinKey);
        ASSERT_EQ(hashJoin.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);
    }

    constexpr size_t NUM_MATCHES = 5;
    /*
     * Output Dataframe should be looking like:
     * 101 201 1 301 401
     * 102 202 2 302 402
     * 103 203 3 303 403
     * 104 204 4 304 404
     * 105 205 5 305 405
     */
    bool callbackExecuted = false;
    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        callbackExecuted = true;

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);

        auto* fstCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(0)->getColumn());
        ASSERT_EQ(fstCol->size(), NUM_MATCHES);
        for (size_t i {0}; NodeID expected : {101, 102, 103, 104, 105}) {
            EXPECT_EQ(fstCol->at(i++), expected);
        }
        auto* sndCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(1)->getColumn());
        ASSERT_EQ(sndCol->size(), NUM_MATCHES);
        for (size_t i {0}; NodeID expected : {201, 202, 203, 204, 205}) {
            EXPECT_EQ(sndCol->at(i++), expected);
        }

        auto* thirdCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(2)->getColumn());
        ASSERT_EQ(thirdCol->size(), NUM_MATCHES);
        for (size_t i {0}; NodeID expected : {301, 302, 303, 304, 305}) {
            EXPECT_EQ(thirdCol->at(i++), expected);
        }

        auto* fourthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(3)->getColumn());
        ASSERT_EQ(fourthCol->size(), NUM_MATCHES);
        for (size_t i {0}; NodeID expected : {401, 402, 403, 404, 405}) {
            EXPECT_EQ(fourthCol->at(i++), expected);
        }

        auto* joinCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(4)->getColumn());
        ASSERT_EQ(joinCol->size(), NUM_MATCHES);
        for (size_t i {0}; NodeID expected : {1, 2, 3, 4, 5}) {
            EXPECT_EQ(joinCol->at(i++), expected);
        }
    };

    _builder->addLambda(callback);
    EXECUTE(view, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(callbackExecuted);
}

// Simple test where we have no overlapping keys
TEST_F(HashJoinProcessorTest, emptyJoinCase) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 5;
    constexpr size_t LHS_NUM_COLS = 3;

    constexpr size_t RHS_NUM_ROWS = 5;
    constexpr size_t RHS_NUM_COLS = 3;

    /*
     * Generate Dataframe looking like:
     * 101   201   1
     * 102   202   2
     * 103   203   3
     * 104   204   4
     * 105   205   5
     */

    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(LHS_NUM_ROWS);
            std::iota(col->begin(), col->end(), (colPtr + 1) * 100 + 1);
        }

        // Alloc Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[LHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        ASSERT_TRUE(col->empty());
        col->resize(LHS_NUM_ROWS);
        std::iota(col->begin(), col->end(), +1);

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        isFinished = true;
    };

    /*
     * Generate Dataframe looking like:
     * 301   401   1
     * 302   402   2
     * 303   403   3
     * 304   404   4
     * 305   405   5
     */

    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(RHS_NUM_ROWS);

            std::iota(col->begin(), col->end(), (colPtr + LHS_NUM_COLS) * (100) + 1);
        }

        // Allocate The Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[RHS_NUM_COLS - 1]->getColumn());

        ASSERT_TRUE(col != nullptr);
        ASSERT_TRUE(col->empty());
        col->resize(RHS_NUM_ROWS);
        std::iota(col->begin(), col->end(), 10);

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        isFinished = true;
    };
    ColumnTag joinKey {0};
    { // Wire up the hash join to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < RHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        joinKey = _builder->getPendingOutput().getDataframe()->cols().back()->getTag();

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < LHS_NUM_COLS - 1; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        _builder->addColumnToOutput<ColumnNodeIDs>(joinKey);

        const auto& hashJoin = _builder->addHashJoin(&rhsIF, joinKey, joinKey);
        ASSERT_EQ(hashJoin.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);
    }

    constexpr size_t NUM_MATCHES = 0;
    bool callbackExecuted = false;
    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        callbackExecuted = true;

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);

        auto* fstCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(0)->getColumn());
        ASSERT_EQ(fstCol->size(), NUM_MATCHES);
        auto* sndCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(1)->getColumn());
        ASSERT_EQ(sndCol->size(), NUM_MATCHES);
        auto* thdCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(2)->getColumn());
        ASSERT_EQ(thdCol->size(), NUM_MATCHES);
        auto* frthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(3)->getColumn());
        ASSERT_EQ(frthCol->size(), NUM_MATCHES);

        auto* joinCol = dynamic_cast<ColumnNodeIDs*>(df->getColumn(joinKey)->getColumn());
        ASSERT_EQ(joinCol->size(), NUM_MATCHES);
    };

    _builder->addLambda(callback);
    EXECUTE(view, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(callbackExecuted);
}

// A simple unchunked join test with multiple values per join key
TEST_F(HashJoinProcessorTest, multipleValuesPerJoinKey) {
    using Rows = LineContainer<NodeID, NodeID, NodeID, NodeID, NodeID>;

    auto [transaction, view, reader] = readGraph();

    constexpr size_t RHS_NUM_ROWS = 10;
    constexpr size_t LHS_NUM_COLS = 3;

    constexpr size_t LHS_NUM_ROWS = 5;
    constexpr size_t RHS_NUM_COLS = 3;

    /*
     * Generate Dataframe looking like:
     * 11    21    1
     * 12    22    1
     * 13    23    2
     * 14    24    2
     * 15    25    3
     * 16    26    3
     * 17    27    4
     * 18    28    4
     * 19    29    5
     * 20    30    5
     */

    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(RHS_NUM_ROWS);
            std::iota(col->begin(), col->end(), (colPtr + 1) * 10 + 1);
        }

        // Alloc Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[RHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        ASSERT_TRUE(col->empty());
        col->resize(RHS_NUM_ROWS);
        std::generate(col->begin(), col->end(), [n = 2]() mutable { return n++ / 2; });

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        isFinished = true;
    };

    /*
     * Generate Dataframe looking like:
     * 31    41    1
     * 32    42    2
     * 33    43    3
     * 34    44    4
     * 35    45    5
     */

    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(LHS_NUM_ROWS);

            std::iota(col->begin(), col->end(), (colPtr + RHS_NUM_COLS) * (10) + 1);
        }

        // Allocate The Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[LHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        ASSERT_TRUE(col->empty());
        col->resize(LHS_NUM_ROWS);
        std::iota(col->begin(), col->end(), 1);

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        isFinished = true;
    };
    ColumnTag joinKey {0};
    { // Wire up the hash join to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < RHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        joinKey = _builder->getPendingOutput().getDataframe()->cols().back()->getTag();

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < LHS_NUM_COLS - 1; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        _builder->addColumnToOutput<ColumnNodeIDs>(joinKey);

        const auto& hashJoin = _builder->addHashJoin(&rhsIF, joinKey, joinKey);
        ASSERT_EQ(hashJoin.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);
    }

    const std::vector<uint64_t> expectedFirstCol {31, 31, 32, 32, 33, 33, 34, 34, 35, 35};
    const std::vector<uint64_t> expectedSecondCol = {41, 41, 42, 42, 43, 43, 44, 44, 45, 45};
    const auto& expectedThirdCol = std::views::iota(11, 21);
    const auto& expectedFourthCol = std::views::iota(21, 31);
    const std::vector<uint64_t> expectedJoinCol = {1, 1, 2, 2, 3, 3, 4, 4, 5, 5};

    Rows expected;
    {

        for (int i = 0; i < 10; ++i) {
            expected.add({expectedFirstCol[i],
                          expectedSecondCol[i],
                          expectedThirdCol[i],
                          expectedFourthCol[i],
                          expectedJoinCol[i]});
        }
    }
    Rows actual;
    bool callbackExecuted = false;
    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        callbackExecuted = true;

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);

        auto* fstCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(0)->getColumn());
        auto* sndCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(1)->getColumn());
        auto* thdCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(2)->getColumn());
        auto* frthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(3)->getColumn());
        auto* joinCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(4)->getColumn());

        for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
            actual.add({fstCol->at(rowPtr),
                        sndCol->at(rowPtr),
                        thdCol->at(rowPtr),
                        frthCol->at(rowPtr),
                        joinCol->at(rowPtr)});
        }
    };
    /*
     * Output Dataframe should be looking like:
     * 101 201 301 401 1
     * 102 202 302 402 2
     * 103 203 303 403 3
     * 104 204 304 404 4
     * 105 205 305 405 5
     */

    _builder->addLambda(callback);
    EXECUTE(view, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(callbackExecuted);
    EXPECT_TRUE(expected.equals(actual));
}

// A simple unchunked join test where we need to skip every other
// row when joining
TEST_F(HashJoinProcessorTest, joinEveryOtherValue) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 8;
    constexpr size_t LHS_NUM_COLS = 3;

    constexpr size_t RHS_NUM_ROWS = 4;
    constexpr size_t RHS_NUM_COLS = 3;

    /*
     * Generate Dataframe looking like:
     * 101   101   1
     * 102   202   2
     * 103   203   3
     * 104   204   4
     * 105   205   5
     * 106   206   6
     * 107   207   7
     * 108   208   8
     */

    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(LHS_NUM_ROWS);
            std::iota(col->begin(), col->end(), (colPtr + 1) * 100 + 1);
        }

        // Alloc Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[LHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        ASSERT_TRUE(col->empty());
        col->resize(LHS_NUM_ROWS);
        std::iota(col->begin(), col->end(), +1);

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        isFinished = true;
    };

    /*
     * Generate Dataframe looking like:
     * 302   402   2
     * 304   404   4
     * 306   406   6
     * 308   408   8
     */

    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(RHS_NUM_ROWS);

            std::generate(col->begin(), col->end(), [n = (colPtr + LHS_NUM_COLS) * (100)]() mutable { return n = n + 2; });
        }

        // Allocate The Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[RHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        ASSERT_TRUE(col->empty());
        col->resize(RHS_NUM_ROWS);
        std::generate(col->begin(), col->end(), [n = 0]() mutable { return n = n + 2; });

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        isFinished = true;
    };
    ColumnTag joinKey {0};
    { // Wire up the hash join to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < RHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        joinKey = _builder->getPendingOutput().getDataframe()->cols().back()->getTag();

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < LHS_NUM_COLS - 1; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        _builder->addColumnToOutput<ColumnNodeIDs>(joinKey);

        const auto& hashJoin = _builder->addHashJoin(&rhsIF, joinKey, joinKey);
        ASSERT_EQ(hashJoin.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);
    }

    constexpr size_t NUM_MATCHES = 4;
    /*
     * Output Dataframe should be looking like:
     * 102 202 302 402 2
     * 104 204 304 404 4
     * 106 206 306 406 6
     * 108 208 308 408 8
     */
    bool callbackExecuted = false;
    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        callbackExecuted = true;

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);

        auto* firstCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(0)->getColumn());
        ASSERT_EQ(firstCol->size(), NUM_MATCHES);
        for (size_t i {0}; NodeID expected : {102, 104, 106, 108}) {
            EXPECT_EQ(firstCol->at(i++), expected);
        }
        auto* secondCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(1)->getColumn());
        ASSERT_EQ(secondCol->size(), NUM_MATCHES);
        for (size_t i {0}; NodeID expected : {202, 204, 206, 208}) {
            EXPECT_EQ(secondCol->at(i++), expected);
        }
        auto* thirdCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(2)->getColumn());
        ASSERT_EQ(thirdCol->size(), NUM_MATCHES);
        for (size_t i {0}; NodeID expected : {302, 304, 306, 308}) {
            EXPECT_EQ(thirdCol->at(i++), expected);
        }
        auto* fourthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(3)->getColumn());
        ASSERT_EQ(fourthCol->size(), NUM_MATCHES);
        for (size_t i {0}; NodeID expected : {402, 404, 406, 408}) {
            EXPECT_EQ(fourthCol->at(i++), expected);
        }
        auto* joinCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(4)->getColumn());
        ASSERT_EQ(joinCol->size(), NUM_MATCHES);
        for (size_t i {0}; NodeID expected : {2, 4, 6, 8}) {
            EXPECT_EQ(joinCol->at(i++), expected);
        }
    };

    _builder->addLambda(callback);
    EXECUTE(view, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(callbackExecuted);
}

// Testing the joining of 1 million rows (no input chunking) -
// this helps confirm that the Slab Allocator is working when
// multiple slabs are allocated. This also helps test that
// we can handle pausing mid-processing of an input when
// we have hit the output chunksize and continue from
// where we stopped on the next cycle
TEST_F(HashJoinProcessorTest, millionRowJoinCase) {
    using Rows = LineContainer<NodeID, NodeID, NodeID, NodeID, NodeID>;
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 1000000;
    constexpr size_t LHS_NUM_COLS = 3;

    constexpr size_t RHS_NUM_ROWS = 1000000;
    constexpr size_t RHS_NUM_COLS = 3;

    /*
     * Generate Dataframe looking like:
     * 1001   2001   1
     * 1002   2002   2
     * 1003   2003   3
     * 1004   2004   4
     * 1005   2005   5
     * ....   ....  ...
     * 2000   3000 1000
     */

    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(LHS_NUM_ROWS);
            std::iota(col->begin(), col->end(), (colPtr + 1) * 1000000 + 1);
        }

        // Alloc Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[LHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        ASSERT_TRUE(col->empty());
        col->resize(LHS_NUM_ROWS);
        std::iota(col->begin(), col->end(), 1);

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        isFinished = true;
    };

    /*
     * Generate Dataframe looking like:
     * 3001   4001   1
     * 3002   4002   2
     * 3003   4003   3
     * 3004   4004   4
     * 3005   4005   5
     * ....   ....  ...
     * 4000   5000 1000
     */

    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            ASSERT_TRUE(col->empty());
            col->resize(RHS_NUM_ROWS);

            std::iota(col->begin(), col->end(), (colPtr + LHS_NUM_COLS) * (1000000) + 1);
        }

        // Allocate The Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[RHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        ASSERT_TRUE(col->empty());
        col->resize(RHS_NUM_ROWS);
        std::iota(col->begin(), col->end(), 1);

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        isFinished = true;
    };

    ColumnTag joinKey {0};
    { // Wire up the hash join to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < RHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        joinKey = _builder->getPendingOutput().getDataframe()->cols().back()->getTag();

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < LHS_NUM_COLS - 1; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        _builder->addColumnToOutput<ColumnNodeIDs>(joinKey);

        const auto& hashJoin = _builder->addHashJoin(&rhsIF, joinKey, joinKey);
        ASSERT_EQ(hashJoin.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);
    }

    // constexpr size_t NUM_MATCHES = 1000000;
    /*
     * Output Dataframe should be looking like:
     * 101 201 301 401 1
     * 102 202 302 402 2
     * 103 203 303 403 3
     * 104 204 304 404 4
     * 105 205 305 405 5
     */

    const auto& expectedFirstCol = std::views::iota(1000001, 2000001);
    const auto& expectedSecondCol = std::views::iota(2000001, 3000001);
    const auto& expectedThirdCol = std::views::iota(3000001, 4000001);
    const auto& expectedFourthCol = std::views::iota(4000001, 5000001);
    const auto& expectedJoinCol = std::views::iota(1, 1000001);

    Rows expected;
    {

        for (int i = 0; i < 1000000; i++) {
            expected.add({expectedFirstCol[i],
                          expectedSecondCol[i],
                          expectedThirdCol[i],
                          expectedFourthCol[i],
                          expectedJoinCol[i]});
        }
    }
    Rows actual;

    bool callbackExecuted = false;
    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        callbackExecuted = true;

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);

        auto* fstCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(0)->getColumn());
        auto* sndCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(1)->getColumn());
        auto* thdCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(2)->getColumn());
        auto* frthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(3)->getColumn());
        auto* fifthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(4)->getColumn());

        for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
            actual.add({fstCol->at(rowPtr),
                        sndCol->at(rowPtr),
                        thdCol->at(rowPtr),
                        frthCol->at(rowPtr),
                        fifthCol->at(rowPtr)});
        }
    };

    _builder->addLambda(callback);
    EXECUTE(view, ChunkConfig::CHUNK_SIZE);
    ASSERT_TRUE(callbackExecuted);
    EXPECT_TRUE(expected.equals(actual));
}

// In this test we pass in chunked inputs of size 100, to join
// 1000 rows. As the number of matches we will get per cycle is
// the same as the output chunk size this helps test when we have
// finished the input and exactly filled up the output chunk.
TEST_F(HashJoinProcessorTest, thousandRowJoinCaseChunkedInputs) {
    using Rows = LineContainer<NodeID, NodeID, NodeID, NodeID, NodeID>;
    const size_t chunkSize = 100;
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 1000;
    constexpr size_t LHS_NUM_COLS = 3;

    constexpr size_t RHS_NUM_ROWS = 1000;
    constexpr size_t RHS_NUM_COLS = 3;

    /*
     * Generate Dataframe looking like:
     * 1001   2001   1
     * 1002   2002   2
     * 1003   2003   3
     * 1004   2004   4
     * 1005   2005   5
     * ....   ....  ...
     * 2000   3000 1000
     */

    size_t totalLeftOutput = 0;
    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }
        size_t outPutSize = std::min(chunkSize, LHS_NUM_ROWS - totalLeftOutput);

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            col->resize(outPutSize);
            std::iota(col->begin(), col->end(), (colPtr + 1) * 1000 + totalLeftOutput);
        }

        // Alloc Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[LHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        col->resize(outPutSize);
        std::iota(col->begin(), col->end(), totalLeftOutput);

        ASSERT_EQ(df->size(), LHS_NUM_COLS);

        totalLeftOutput += outPutSize;
        if (totalLeftOutput == LHS_NUM_ROWS) {
            isFinished = true;
        }
    };

    /*
     * Generate Dataframe looking like:
     * 3001   4001   1
     * 3002   4002   2
     * 3003   4003   3
     * 3004   4004   4
     * 3005   4005   5
     * ....   ....  ...
     * 4000   5000 1000
     */

    size_t totalRightOutput = 0;
    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }
        size_t outPutSize = std::min(chunkSize, RHS_NUM_ROWS - totalRightOutput);

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            col->resize(outPutSize);

            std::iota(col->begin(), col->end(), (colPtr + LHS_NUM_COLS) * (1000) + totalRightOutput);
        }

        // Allocate The Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[RHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        col->resize(outPutSize);
        std::iota(col->begin(), col->end(), totalRightOutput);

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        totalRightOutput += outPutSize;
        if (totalRightOutput == RHS_NUM_ROWS) {
            isFinished = true;
        }
    };

    ColumnTag joinKey {0};
    { // Wire up the hash join to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < RHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        joinKey = _builder->getPendingOutput().getDataframe()->cols().back()->getTag();

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < LHS_NUM_COLS - 1; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        _builder->addColumnToOutput<ColumnNodeIDs>(joinKey);

        const auto& hashJoin = _builder->addHashJoin(&rhsIF, joinKey, joinKey);
        ASSERT_EQ(hashJoin.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);
    }

    // constexpr size_t NUM_MATCHES = 1000000;
    /*
     * Output Dataframe should be looking like:
     * 101 201 301 401 1
     * 102 202 302 402 2
     * 103 203 303 403 3
     * 104 204 304 404 4
     * 105 205 305 405 5
     */

    const auto& expectedFirstCol = std::views::iota(1000, 2000);
    const auto& expectedSecondCol = std::views::iota(2000, 300);
    const auto& expectedThirdCol = std::views::iota(3000, 4000);
    const auto& expectedFourthCol = std::views::iota(4000, 5000);
    const auto& expectedJoinCol = std::views::iota(0, 1000);

    Rows expected;
    {

        for (int i = 0; i < 1000; i++) {
            expected.add({expectedFirstCol[i],
                          expectedSecondCol[i],
                          expectedThirdCol[i],
                          expectedFourthCol[i],
                          expectedJoinCol[i]});
        }
    }
    Rows actual;

    bool callbackExecuted = false;
    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        callbackExecuted = true;

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);

        auto* fstCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(0)->getColumn());
        auto* sndCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(1)->getColumn());
        auto* thdCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(2)->getColumn());
        auto* frthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(3)->getColumn());
        auto* fifthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(4)->getColumn());

        for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
            actual.add({fstCol->at(rowPtr),
                        sndCol->at(rowPtr),
                        thdCol->at(rowPtr),
                        frthCol->at(rowPtr),
                        fifthCol->at(rowPtr)});
        }
    };

    _builder->addLambda(callback);
    EXECUTE(view, chunkSize);
    ASSERT_TRUE(callbackExecuted);
    EXPECT_TRUE(expected.equals(actual));
}

// Here we test if the hash join works when the output chunk
// is filled up mid processor-execution and needs to start
// from the point it was on in the vector of rows returned by
// the hash table.
//
// In this test as the number of stored rows
// per hash value is 4 and the chunk size is 2, After reading
// 2 rows the processor must restart from the middle of the vector
// returned by the hash on the next cycle. We expect it to copy over
// 2 rows from the returned rows from the hash and then continue with
// processing the next input
TEST_F(HashJoinProcessorTest, finishPausedStreamTest) {
    using Rows = LineContainer<NodeID, NodeID, NodeID, NodeID, NodeID>;
    const size_t chunkSize = 2;
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 5;
    constexpr size_t LHS_NUM_COLS = 3;

    constexpr size_t RHS_NUM_ROWS = 20;
    constexpr size_t RHS_NUM_COLS = 3;

    /*
     * Generate Dataframe looking like:
     * 11 31 1
     * 12 32 1
     * 13 33 1
     * 14 34 1
     * 15 35 2
     * 16 36 2
     * .....
     * 30 50 5
     */


    size_t totalRightOutput = 0;
    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }
        size_t outPutSize = std::min(chunkSize, RHS_NUM_ROWS - totalRightOutput);

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            col->resize(outPutSize);
            std::iota(col->begin(), col->end(), ((2 * colPtr) + 1) * 10 + 1 + totalRightOutput);
        }

        // Alloc Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[RHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        col->resize(outPutSize);
        std::generate(col->begin(), col->end(), [n = totalRightOutput + 4]() mutable { return n++ / 4; });

        ASSERT_EQ(df->size(), RHS_NUM_COLS);

        totalRightOutput += outPutSize;
        if (totalRightOutput == RHS_NUM_ROWS) {
            isFinished = true;
        }
    };
    /*
     * Generate Dataframe looking like:
     * 51 61 1
     * 52 62 2
     * 53 63 3
     * 54 64 4
     * 55 65 5
     */

    size_t totalLeftOutput = 0;
    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }
        size_t outPutSize = std::min(chunkSize, LHS_NUM_ROWS - totalLeftOutput);

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            col->resize(outPutSize);

            std::iota(col->begin(), col->end(), (colPtr + 5) * (10) + 1 + totalLeftOutput);
        }

        // Allocate The Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[LHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        col->resize(outPutSize);
        std::iota(col->begin(), col->end(), totalLeftOutput + 1);

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        totalLeftOutput += outPutSize;
        if (totalLeftOutput == LHS_NUM_ROWS) {
            isFinished = true;
        }
    };

    ColumnTag joinKey {0};
    { // Wire up the hash join to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < RHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        joinKey = _builder->getPendingOutput().getDataframe()->cols().back()->getTag();

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < LHS_NUM_COLS - 1; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        _builder->addColumnToOutput<ColumnNodeIDs>(joinKey);

        const auto& hashJoin = _builder->addHashJoin(&rhsIF, joinKey, joinKey);
        ASSERT_EQ(hashJoin.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);
    }

    // constexpr size_t NUM_MATCHES = 1000000;
    /*
     * Output Dataframe should be looking like:
     * 101 201 301 401 1
     * 102 202 302 402 2
     * 103 203 303 403 3
     * 104 204 304 404 4
     * 105 205 305 405 5
     */


    const auto& expectedFirstCol = std::views::iota(4, 24) | 
                                   std::views::transform([](int i) {
                                        return 50  + (i/4);
                                   });
    const auto& expectedSecondCol = std::views::iota(4, 24) | 
                                   std::views::transform([](int i) {
                                        return 60 + (i/4);
                                   });

    const auto& expectedThirdCol = std::views::iota(11, 31);
    const auto& expectedFourthCol = std::views::iota(31, 51);

    const auto expectedJoinCol = std::views::iota(4, 24) | 
                             std::views::transform([](int i) {
                                 return (i / 4);
                             });

    Rows expected;
    {

        for (int i = 0; i < 20; i++) {
            expected.add({expectedFirstCol[i],
                          expectedSecondCol[i],
                          expectedThirdCol[i],
                          expectedFourthCol[i],
                          expectedJoinCol[i]});
        }
    }
    Rows actual;

    bool callbackExecuted = false;
    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        callbackExecuted = true;

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);

        auto* fstCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(0)->getColumn());
        auto* sndCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(1)->getColumn());
        auto* thdCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(2)->getColumn());
        auto* frthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(3)->getColumn());
        auto* fifthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(4)->getColumn());

        for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
            actual.add({fstCol->at(rowPtr),
                        sndCol->at(rowPtr),
                        thdCol->at(rowPtr),
                        frthCol->at(rowPtr),
                        fifthCol->at(rowPtr)});
        }
    };

    _builder->addLambda(callback);
    EXECUTE(view, chunkSize);
    ASSERT_TRUE(callbackExecuted);
    EXPECT_TRUE(expected.equals(actual));
}

// Here we test if the hash join works when the output chunk
// is filled up mid processor-execution and needs to start
// from the point it was on in the vector of rows returned by
// the hash table.
//
// In this test as the number of stored rows
// per hash value is 5 and the chunk size is 2, After reading
// 2 rows the processor must restart from the middle of the vector
// returned by the hash on the next cycle. We expect it to copy over
// 2 rows and then 1 row (totalling 5) and the continue to copy
// 1 more row from the input we have in the processor .
TEST_F(HashJoinProcessorTest, finishPausedStreamAndContinueToProcessTest) {
    using Rows = LineContainer<NodeID, NodeID, NodeID, NodeID, NodeID>;
    const size_t chunkSize = 2;
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 4;
    constexpr size_t LHS_NUM_COLS = 3;

    constexpr size_t RHS_NUM_ROWS = 20;
    constexpr size_t RHS_NUM_COLS = 3;

    /*
     * Generate Dataframe looking like:
     * 11 31 1
     * 12 32 1
     * 13 33 1
     * 14 34 1
     * 15 35 1
     * 16 36 2
     * .....
     * 30 50 4
     */


    size_t totalRightOutput = 0;
    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }
        size_t outPutSize = std::min(chunkSize, RHS_NUM_ROWS - totalRightOutput);

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            col->resize(outPutSize);
            std::iota(col->begin(), col->end(), ((2 * colPtr) + 1) * 10 + 1 + totalRightOutput);
        }

        // Alloc Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[RHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        col->resize(outPutSize);
        std::generate(col->begin(), col->end(), [n = totalRightOutput + 5]() mutable { return n++ / 5; });

        ASSERT_EQ(df->size(), RHS_NUM_COLS);

        totalRightOutput += outPutSize;
        if (totalRightOutput == RHS_NUM_ROWS) {
            isFinished = true;
        }
    };
    /*
     * Generate Dataframe looking like:
     * 51 61 1
     * 52 62 2
     * 53 63 3
     * 54 64 4
     */

    size_t totalLeftOutput = 0;
    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }
        size_t outPutSize = std::min(chunkSize, LHS_NUM_ROWS - totalLeftOutput);

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            col->resize(outPutSize);

            std::iota(col->begin(), col->end(), (colPtr + 5) * (10) + 1 + totalLeftOutput);
        }

        // Allocate The Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[LHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        col->resize(outPutSize);
        std::iota(col->begin(), col->end(), totalLeftOutput + 1);

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        totalLeftOutput += outPutSize;
        if (totalLeftOutput == LHS_NUM_ROWS) {
            isFinished = true;
        }
    };

    ColumnTag joinKey {0};
    { // Wire up the hash join to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < RHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        joinKey = _builder->getPendingOutput().getDataframe()->cols().back()->getTag();

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < LHS_NUM_COLS - 1; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        _builder->addColumnToOutput<ColumnNodeIDs>(joinKey);

        const auto& hashJoin = _builder->addHashJoin(&rhsIF, joinKey, joinKey);
        ASSERT_EQ(hashJoin.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);
    }

    // constexpr size_t NUM_MATCHES = 1000000;
    /*
     * Output Dataframe should be looking like:
     * 101 201 301 401 1
     * 102 202 302 402 2
     * 103 203 303 403 3
     * 104 204 304 404 4
     * 105 205 305 405 5
     */

    const auto& expectedFirstCol = std::views::iota(5, 25) | 
                                   std::views::transform([](int i) {
                                        return 50  + (i/5);
                                   });
    const auto& expectedSecondCol = std::views::iota(5, 25) | 
                                   std::views::transform([](int i) {
                                        return 60 + (i/5);
                                   });

    const auto& expectedThirdCol = std::views::iota(11, 31);
    const auto& expectedFourthCol = std::views::iota(31, 51);

    const auto expectedJoinCol = std::views::iota(5, 25) | 
                             std::views::transform([](int i) {
                                 return (i / 5);
                             });



    Rows expected;
    {

        for (int i = 0; i < 20; i++) {
            expected.add({expectedFirstCol[i],
                          expectedSecondCol[i],
                          expectedThirdCol[i],
                          expectedFourthCol[i],
                          expectedJoinCol[i]});
        }
    }
    Rows actual;

    bool callbackExecuted = false;
    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        callbackExecuted = true;

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);

        auto* fstCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(0)->getColumn());
        auto* sndCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(1)->getColumn());
        auto* thdCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(2)->getColumn());
        auto* frthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(3)->getColumn());
        auto* fifthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(4)->getColumn());

        for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
            actual.add({fstCol->at(rowPtr),
                        sndCol->at(rowPtr),
                        thdCol->at(rowPtr),
                        frthCol->at(rowPtr),
                        fifthCol->at(rowPtr)});
        }
    };

    _builder->addLambda(callback);
    EXECUTE(view, chunkSize);
    ASSERT_TRUE(callbackExecuted);
    EXPECT_TRUE(expected.equals(actual));
}

// The below test helps test the code path of right joins
// but this doesn't work right now because of how we execute
// the pipeline so it is disabled for now
TEST_F(HashJoinProcessorTest, multipleValuesPerKeyWithChunkedInputs) {
    using Rows = LineContainer<NodeID, NodeID, NodeID, NodeID, NodeID>;
    const size_t chunkSize = 100;
    auto [transaction, view, reader] = readGraph();

    constexpr size_t LHS_NUM_ROWS = 200;
    constexpr size_t LHS_NUM_COLS = 3;

    constexpr size_t RHS_NUM_ROWS = 1000;
    constexpr size_t RHS_NUM_COLS = 3;

    /*
     * Generate Dataframe looking like:
     * 1001   2001   1
     * 1002   2002   1
     * 1003   2003   1
     * 1004   2004   1
     * 1005   2005   1
     * 1006   2006   2
     * ....   ....  ...
     * 2000   3000  200
     */

    size_t totalRightOutput = 0;
    const auto genRDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }
        size_t outPutSize = std::min(chunkSize, RHS_NUM_ROWS - totalRightOutput);

        ASSERT_EQ(df->size(), RHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < RHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            col->resize(outPutSize);
            std::iota(col->begin(), col->end(), (colPtr + 1) * 1000 + totalRightOutput + 1);
        }

        // Alloc Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[RHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        col->resize(outPutSize);
        std::generate(col->begin(), col->end(), [n = totalRightOutput + 5]() mutable { return n++ / 5; });

        ASSERT_EQ(df->size(), RHS_NUM_COLS);

        totalRightOutput += outPutSize;
        if (totalRightOutput == RHS_NUM_ROWS) {
            isFinished = true;
        }
    };

    /*
     * Generate Dataframe looking like:
     * 3001   4001   1
     * 3002   4002   2
     * 3003   4003   3
     * 3004   4004   4
     * 3005   4005   5
     * 3006   4006   6
     * ....   ....  ...
     * 3200   4200  200
     */

    size_t totalLeftOutput = 0;
    const auto genLDF = [&](Dataframe* df, bool& isFinished, auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }
        size_t outPutSize = std::min(chunkSize, LHS_NUM_ROWS - totalLeftOutput);

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        for (size_t colPtr = 0; colPtr < LHS_NUM_COLS - 1; colPtr++) {
            ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[colPtr]->getColumn());
            ASSERT_TRUE(col != nullptr);
            col->resize(outPutSize);
            std::iota(col->begin(), col->end(), (colPtr + LHS_NUM_COLS) * (1000) + totalLeftOutput + 1);
        }

        // Allocate The Join Column
        ColumnNodeIDs* col = dynamic_cast<ColumnNodeIDs*>(df->cols()[LHS_NUM_COLS - 1]->getColumn());
        ASSERT_TRUE(col != nullptr);
        col->resize(outPutSize);
        std::iota(col->begin(), col->end(), totalLeftOutput + 1);

        ASSERT_EQ(df->size(), LHS_NUM_COLS);
        totalLeftOutput += outPutSize;
        if (totalLeftOutput == LHS_NUM_ROWS) {
            isFinished = true;
        }
    };

    ColumnTag joinKey {0};
    { // Wire up the hash join to the two inputs
        auto& rhsIF = _builder->addLambdaSource(genRDF);
        for (size_t i = 0; i < RHS_NUM_COLS; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        joinKey = _builder->getPendingOutput().getDataframe()->cols().back()->getTag();

        [[maybe_unused]] auto& lhsIF = _builder->addLambdaSource(genLDF);
        for (size_t i = 0; i < LHS_NUM_COLS - 1; i++) {
            _builder->addColumnToOutput<ColumnNodeIDs>(
                _pipeline.getDataframeManager()->allocTag());
        }
        _builder->addColumnToOutput<ColumnNodeIDs>(joinKey);

        const auto& hashJoin = _builder->addHashJoin(&rhsIF, joinKey, joinKey);
        ASSERT_EQ(hashJoin.getDataframe()->cols().size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);
    }

    /*
     * Output Dataframe should be looking like:
     * 101 201 301 401 1
     * 102 202 302 402 2
     * 103 203 303 403 3
     * 104 204 304 404 4
     * 105 205 305 405 5
     */

    const auto& expectedThirdCol = std::views::iota(1001, 2001);
    const auto& expectedFourthCol = std::views::iota(2001, 3001);

    const auto& expectedFirstCol = std::views::iota(5, 1001) | 
                                   std::views::transform([](int i) {
                                        return 3000 + (i/5);
                                   });
    const auto& expectedSecondCol = std::views::iota(5, 1001) | 
                                   std::views::transform([](int i) {
                                        return 4000 + (i/5);
                                   });


    const auto expectedJoinCol = std::views::iota(5, 1001) | 
                             std::views::transform([](int i) {
                                 return (i / 5);
                             });

    Rows expected;
    {

        for (int i = 0; i < 1000; i++) {
            expected.add({expectedFirstCol[i],
                          expectedSecondCol[i],
                          expectedThirdCol[i],
                          expectedFourthCol[i],
                          expectedJoinCol[i]});
        }
    }
    Rows actual;

    bool callbackExecuted = false;
    const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        callbackExecuted = true;

        ASSERT_EQ(df->size(), LHS_NUM_COLS + RHS_NUM_COLS - 1);

        auto* fstCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(0)->getColumn());
        auto* sndCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(1)->getColumn());
        auto* thdCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(2)->getColumn());
        auto* frthCol = dynamic_cast<ColumnNodeIDs*>(df->cols().at(3)->getColumn());
        auto* joinCol = dynamic_cast<ColumnNodeIDs*>(df->getColumn(joinKey)->getColumn());

        for (size_t rowPtr = 0; rowPtr < df->getRowCount(); rowPtr++) {
            actual.add({fstCol->at(rowPtr),
                        sndCol->at(rowPtr),
                        thdCol->at(rowPtr),
                        frthCol->at(rowPtr),
                        joinCol->at(rowPtr)});
        }
    };

    _builder->addLambda(callback);
    EXECUTE(view, chunkSize);
    ASSERT_TRUE(callbackExecuted);
    EXPECT_TRUE(expected.equals(actual));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
