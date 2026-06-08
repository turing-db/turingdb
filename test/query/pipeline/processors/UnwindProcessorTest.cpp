#include "ProcessorTester.h"

#include <math.h>

#include "list/ListBuffer.h"
#include "SystemManager.h"

#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"

#include "processors/LambdaProcessor.h"

#include "list/ListElementView.h"

using namespace db;
using namespace turing::test;

class UnwindProcessorTest : public ProcessorTester {
public:
    void initialize() override {
        ProcessorTester::initialize();
        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph("testdb");
    }
};

TEST_F(UnwindProcessorTest, listLengthsExceedingChunkSize) {
    auto [transaction, view, reader] = readGraph();

    constexpr size_t SMALL_CHUNK_SIZE = 3;

    const std::vector<size_t> listSizes = {
        1,
        SMALL_CHUNK_SIZE,
        SMALL_CHUNK_SIZE + 1,
        SMALL_CHUNK_SIZE * 2,
        (SMALL_CHUNK_SIZE * 2) + 1,
        (SMALL_CHUNK_SIZE * 5) + 2,
    };

    for (const size_t listSize : listSizes) {

        std::vector<int64_t> expected;
        expected.reserve(listSize);
        for (int64_t i = 0; i < static_cast<int64_t>(listSize); i++) {
            expected.push_back(i);
        }

        ListBuffer listBuf;
        std::vector<ListBuffer<>::ListItemVariant> items;
        items.reserve(listSize);
        for (const int64_t v : expected) {
            items.emplace_back(v);
        }
        const ListView list = listBuf.insert(items);

        const size_t maxChunkSize = listSize + 2;
        for (size_t chunkSize = 1; chunkSize <= maxChunkSize; chunkSize++) {

            LocalMemory mem;
            PipelineV2 pipeline;
            PipelineBuilder builder(&mem, &pipeline);

            const PipelineValuesOutputInterface& unwindOut = builder.addUnwind(list);
            const ColumnTag valuesTag = unwindOut.getValues()->getTag();

            std::vector<int64_t> results;
            size_t numChunks = 0;

            const auto callback = [&](const Dataframe* df, LambdaProcessor::Operation op) -> void {
                if (op != LambdaProcessor::Operation::EXECUTE) {
                    return;
                }

                numChunks++;

                const auto* col = df->getColumn<ColumnVector<ListElementView>>(valuesTag);
                ASSERT_TRUE(col);

                for (const ListElementView val : *col) {
                    results.push_back(val.getAs<int64_t>());
                }
            };

            builder.addLambda(callback);

            ExecutionContext execCtxt(&_env->getSystemManager(), view);
            execCtxt.setChunkSize(chunkSize);
            PipelineExecutor executor(&pipeline, &execCtxt);
            executor.execute();

            const size_t expectedChunks = (std::ceil((double)(listSize) / chunkSize));
            ASSERT_EQ(numChunks, expectedChunks);
            ASSERT_EQ(results, expected);
        }
    }
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, [] {
        testing::GTEST_FLAG(repeat) = 1;
    });
}
