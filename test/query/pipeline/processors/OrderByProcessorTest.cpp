#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <ranges>
#include <vector>

#include "EntityType.h"
#include "LineContainer.h"
#include "ProcessorTester.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "dataframe/NamedColumn.h"
#include "iterators/ChunkConfig.h"
#include "metadata/PropertyType.h"
#include "processors/LambdaProcessor.h"
#include "processors/MaterializeProcessor.h"

#include "processors/OrderByProcessor.h"

class OrderByProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        _graph = _env->getSystemManager().createGraph("simpledb");
        SimpleGraph::createSimpleGraph(_graph);
    }
};

TEST_F(OrderByProcessorTest, simpleOrder) {
    using Rows = LineContainer<std::optional<types::String::Primitive>>;

    const auto runTest = [&](bool asc, size_t chunkSize) {
        SCOPED_TRACE("asc=" + std::to_string(asc) +
                     " chunkSize=" + std::to_string(chunkSize));

        auto [transaction, view, reader] = readGraph();
        const PropertyType nameType = view.metadata().propTypes().get("name").value();

        auto* matProc = MaterializeProcessor::create(&_pipeline, &_env->getMem());
        _builder->setMaterializeProc(matProc);
        const auto& scanNodes = _builder->addScanNodes();
        const ColumnTag nodeTag = scanNodes.getNodeIDs()->getTag();
        const auto& getNames =
            _builder->addGetPropertiesWithNull<EntityType::Node, types::String>(nodeTag,
                                                                                nameType);
        const ColumnTag nameTag = getNames.getValues()->getTag();
        OrderByProcessor::OrderByKeys keys = {
            {._col = nameTag, ._asc = asc}
        };
        const PipelineBlockOutputInterface& orderBy = _builder->addOrderBy(keys);
        ASSERT_EQ(orderBy.getDataframe()->size(), 2);

        // Build expected result by collecting all names
        Rows expected;
        {
            for (const NodeID id : reader.scanNodes()) {
                const ColumnNodeIDs idc {id};
                auto names = reader.getNodePropertiesWithNull<types::String>(nameType._id, &idc);
                auto maybeName = *names.begin();
                ASSERT_TRUE(maybeName.has_value());
                expected.add({*maybeName});
            }
        }

        Rows actual;
        bool executed = false;
        const auto VERIFY_CALLBACK = [&](const Dataframe* df,
                                         LambdaProcessor::Operation operation) -> void {
            if (operation == LambdaProcessor::Operation::RESET) {
                return;
            }
            executed = true;

            const NamedColumn* nnames = df->getColumn(nameTag);
            ASSERT_TRUE(nnames);
            const auto* casted = nnames->as<ColumnOptVector<types::String::Primitive>>();
            ASSERT_TRUE(casted);
            ASSERT_TRUE(casted->size() <= chunkSize);

            // Ensure the names are sorted
            const auto& data = casted->getRaw();
            if (asc) {
                ASSERT_TRUE(std::ranges::is_sorted(data));
            } else {
                ASSERT_TRUE(std::ranges::is_sorted(data, std::greater<> {}));
            }

            // Record the names to compare with expected
            const size_t rowCount = df->getLogicalRowCount();
            for (size_t row = 0; row < rowCount; row++) {
                actual.add({casted->at(row)});
            }
        };
        _builder->addLambda(VERIFY_CALLBACK);
        EXECUTE(view, chunkSize);
        ASSERT_TRUE(executed);
        EXPECT_TRUE(expected.equals(actual));
    };

    for (const bool asc : {true, false}) {
        for (const size_t chunkSize : {1UL, 3UL, 9UL, 16UL, ChunkConfig::CHUNK_SIZE}) {
            runTest(asc, chunkSize);
        }
    }
}

TEST_F(OrderByProcessorTest, millionRandomInts) {
    constexpr types::Int64::Primitive ONE_MILLION = 1'000'000;
    constexpr size_t NUM_COLS = 2;
    using ColumnInts = ColumnVector<types::Int64::Primitive>;

    std::mt19937 rng(333); // Seeded; deterministic over test runs
    std::uniform_int_distribution<types::Int64::Primitive> dist(-ONE_MILLION, ONE_MILLION);

    size_t rowsWritten = 0;
    const auto genInputChunk = [&](Dataframe* df,
                                   bool& isFinished,
                                   auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), NUM_COLS);

        auto* lhs = df->cols().front()->as<ColumnInts>();
        auto* rhs = df->cols().back()->as<ColumnInts>();
        ASSERT_TRUE(lhs && rhs);

        const size_t rowsRemaining = ONE_MILLION - rowsWritten;
        const size_t rowsToWrite = std::min(ChunkConfig::CHUNK_SIZE, rowsRemaining);

        lhs->resize(rowsToWrite);
        rhs->resize(rowsToWrite);

        auto& ld = lhs->getRaw();
        auto& rd = rhs->getRaw();

        // If we then sort by lhs, rhs should be sorted in the reverse order
        for (size_t i = 0; i < rowsToWrite; i++) {
            const types::Int64::Primitive randomNumber = dist(rng);
            ld[i] = randomNumber;
            rd[i] = -randomNumber;
        }

        rowsWritten += rowsToWrite;

        if (rowsWritten == ONE_MILLION) {
            isFinished = true;
        }
    };

    [[maybe_unused]] const auto& rngEmitter = _builder->addLambdaSource(genInputChunk);
    const ColumnTag orderByTag = _pipeline.getDataframeManager()->allocTag();
    const ColumnTag otherTag = _pipeline.getDataframeManager()->allocTag();
    _builder->addColumnToOutput<ColumnInts>(orderByTag);
    _builder->addColumnToOutput<ColumnInts>(otherTag);

    OrderByProcessor::OrderByKeys keys = {
        {._col = orderByTag, ._asc = true}
    };

    const auto& orderBy = _builder->addOrderBy(keys);
    ASSERT_EQ(orderBy.getDataframe()->size(), 2);

    std::vector<types::Int64::Primitive> lhsRes;
    std::vector<types::Int64::Primitive> rhsRes;
    lhsRes.reserve(ONE_MILLION);
    rhsRes.reserve(ONE_MILLION);
    size_t seenRows = 0;

    const auto COLLECT_RESULTS = [&](const Dataframe* df,
                                     LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }

        ASSERT_EQ(df->size(), NUM_COLS);

        {
            const NamedColumn* lhsNCol = df->getColumn(orderByTag);
            ASSERT_TRUE(lhsNCol);
            const auto* casted = lhsNCol->as<ColumnInts>();
            ASSERT_TRUE(casted);
            ASSERT_TRUE(casted->size() <= ChunkConfig::CHUNK_SIZE);
            const auto& lhsd = casted->getRaw();

            lhsRes.insert(end(lhsRes), begin(lhsd), end(lhsd));
        }

        {
            const NamedColumn* rhsNCol = df->getColumn(otherTag);
            ASSERT_TRUE(rhsNCol);
            const auto* casted = rhsNCol->as<ColumnInts>();
            ASSERT_TRUE(casted);
            ASSERT_TRUE(casted->size() <= ChunkConfig::CHUNK_SIZE);
            const auto& rhsd = casted->getRaw();

            rhsRes.insert(end(rhsRes), begin(rhsd), end(rhsd));
        }

        seenRows += df->getLogicalRowCount();
    };

    auto [transaction, view, reader] = readGraph();
    _builder->addLambda(COLLECT_RESULTS);
    EXECUTE(view, ChunkConfig::CHUNK_SIZE);

    ASSERT_EQ(seenRows, ONE_MILLION);
    ASSERT_EQ(lhsRes.size(), rhsRes.size());
    // We sort by lhs, but since rhs[i] = -lhs[i], rhsRes should also be sorted, in the
    // opposite direction
    ASSERT_TRUE(std::ranges::is_sorted(lhsRes, std::less <> {}));
    ASSERT_TRUE(std::ranges::is_sorted(rhsRes, std::greater<> {}));
}

/// Test the OrderByProcessor recieving empty and non-empty chunks, alternating
TEST_F(OrderByProcessorTest, alternatingEmpty) {
    using Int = types::Int64::Primitive;
    using ColumnInts = ColumnVector<Int>;
    Int nextVal = 1;
    constexpr Int MAX_VAL = ChunkConfig::CHUNK_SIZE + 10;

    /// Produce ints 1-10, but every other chunk is empty
    bool writeEmpty = false;
    const auto genInputChunk = [&](Dataframe* df,
                                   bool& isFinished,
                                   auto operation) -> void {
        if (operation != LambdaSourceProcessor::Operation::EXECUTE) {
            return;
        }

        ASSERT_EQ(df->size(), 1);

        auto* col = df->cols().front()->as<ColumnInts>();
        col->clear();

        if (!writeEmpty) {
            col->push_back(nextVal++);
        }

        writeEmpty = !writeEmpty;

        if (nextVal == MAX_VAL + 1) {
            isFinished = true;
        }
    };

    [[maybe_unused]] const auto& rngEmitter = _builder->addLambdaSource(genInputChunk);
    const ColumnTag intTag = _pipeline.getDataframeManager()->allocTag();
    _builder->addColumnToOutput<ColumnInts>(intTag);

    OrderByProcessor::OrderByKeys keys = {
      {._col = intTag, ._asc = true}
    };

    const auto& orderBy = _builder->addOrderBy(keys);
    ASSERT_EQ(orderBy.getDataframe()->size(), 1);

    std::vector<Int> res;
    size_t seenChunks = 0;

    const auto COLLECT_RESULTS = [&](const Dataframe* df,
                                     LambdaProcessor::Operation operation) -> void {
        if (operation == LambdaProcessor::Operation::RESET) {
            return;
        }
        seenChunks++;

        ASSERT_EQ(df->size(), 1);

        {
            const NamedColumn* lhsNCol = df->getColumn(intTag);
            ASSERT_TRUE(lhsNCol);
            const auto* casted = lhsNCol->as<ColumnInts>();
            ASSERT_TRUE(casted);
            ASSERT_TRUE(casted->size() <= ChunkConfig::CHUNK_SIZE);
            const auto& lhsd = casted->getRaw();

            res.insert(end(res), begin(lhsd), end(lhsd));
        }
    };

    auto [transaction, view, reader] = readGraph();
    _builder->addLambda(COLLECT_RESULTS);
    EXECUTE(view, ChunkConfig::CHUNK_SIZE);

    // OrderBy should've aggregated those chunks, only output 1
    ASSERT_EQ(seenChunks, 2);
    ASSERT_EQ(res.size(), MAX_VAL);
    ASSERT_TRUE(std::ranges::is_sorted(res));
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 10;
    });
}
