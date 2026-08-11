#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"

#include "GraphQueryTest.h"

using namespace turing::test;

class CallProcedureChunkedInputTest : public GraphQueryTest {
protected:
    using CommitStats = std::pair<uint64_t, uint64_t>;

    void collectCommitStats(std::string_view q, std::vector<CommitStats>& rows) {
        const auto res = query(q, [&](const Dataframe* df) -> void {
            ASSERT_TRUE(df != nullptr);
            ASSERT_EQ(df->cols().size(), 2);

            const auto& cols = df->cols();
            const auto* nodeCounts = cols.at(0)->as<ColumnVector<uint64_t>>();
            const auto* edgeCounts = cols.at(1)->as<ColumnVector<uint64_t>>();

            ASSERT_TRUE(nodeCounts != nullptr);
            ASSERT_TRUE(edgeCounts != nullptr);
            ASSERT_EQ(nodeCounts->size(), edgeCounts->size());

            for (size_t i = 0; i < nodeCounts->size(); ++i) {
                rows.emplace_back(nodeCounts->at(i), edgeCounts->at(i));
            }
        });

        ASSERT_TRUE(res.isOk());
    }
};

TEST_F(CallProcedureChunkedInputTest, describeCommitOverSeveralInputChunks) {
    const std::string statsQuery = "CALL db.history() YIELD commit AS c "
                                   "CALL db.describeCommit(c) YIELD nodeCount, edgeCount "
                                   "RETURN nodeCount, edgeCount";

    std::vector<CommitStats> wholeInputInOneChunk;
    collectCommitStats(statsQuery, wholeInputInOneChunk);
    ASSERT_EQ(wholeInputInOneChunk.size(), 8);

    _queryConfig.setChunkSize(2);

    std::vector<CommitStats> inputSplitInFourChunks;
    collectCommitStats(statsQuery, inputSplitInFourChunks);

    ASSERT_EQ(inputSplitInFourChunks, wholeInputInOneChunk);
}
