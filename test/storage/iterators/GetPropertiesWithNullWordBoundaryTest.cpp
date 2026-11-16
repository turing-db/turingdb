#include <gtest/gtest.h>

#include <stddef.h>

#include <array>
#include <optional>
#include <set>
#include <string_view>
#include <vector>

#include "TuringTest.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "iterators/GetPropertiesWithNullIterator.h"
#include "metadata/GraphMetadata.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "ColumnOptVectorInvariants.h"

using namespace db;
using namespace turing::test;

namespace {

using StringColumn = ColumnOptVector<types::String::Primitive>;
using Rows = std::vector<std::optional<std::string_view>>;

// Remy, Adam and Maxime carry a dob; Computers does not
constexpr std::array<size_t, 3> NODES_WITH_DOB {0, 1, 8};
constexpr std::array<std::string_view, 3> DOBS {"18/01", "18/08", "24/07"};
constexpr size_t NODE_WITHOUT_DOB = 2;

std::set<size_t> rowsBetween(size_t first, size_t last) {
    std::set<size_t> rows;
    for (size_t row = first; row < last; row++) {
        rows.insert(row);
    }

    return rows;
}

void buildInput(size_t rowCount, const std::set<size_t>& nullRows, ColumnNodeIDs& input, Rows& expected) {
    for (size_t row = 0; row < rowCount; row++) {
        if (nullRows.contains(row)) {
            input.push_back(NodeID {NODE_WITHOUT_DOB});
            expected.push_back(std::nullopt);
        } else {
            input.push_back(NodeID {NODES_WITH_DOB[row % NODES_WITH_DOB.size()]});
            expected.push_back(DOBS[row % DOBS.size()]);
        }
    }
}

}

class GetPropertiesWithNullWordBoundaryTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    // Fill the rows chunk by chunk into one output column, checking each chunk as it comes
    void expectChunks(size_t rowCount, const std::set<size_t>& nullRows, size_t chunkSize) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();
        const GraphView view = reader.getView();

        const std::optional<PropertyType> dobType = view.metadata().propTypes().get("dob");
        ASSERT_TRUE(dobType.has_value());

        ColumnNodeIDs input;
        Rows expected;
        buildInput(rowCount, nullRows, input, expected);

        StringColumn output;
        GetNodePropertiesWithNullChunkWriter<types::String> writer(view, dobType->_id, &input);
        writer.setOutput(&output);

        size_t firstRow = 0;
        while (writer.isValid()) {
            writer.fill(chunkSize);
            ASSERT_NO_FATAL_FAILURE(expectColumnOptVectorInvariants(output));

            const size_t chunkRowCount = output.size();
            ASSERT_EQ(chunkRowCount, std::min(chunkSize, rowCount - firstRow));

            size_t nullCount = 0;
            for (size_t row = 0; row < chunkRowCount; row++) {
                const std::optional<std::string_view>& expectedRow = expected[firstRow + row];
                EXPECT_EQ(output.get(row), expectedRow) << "row " << firstRow + row;
                nullCount += expectedRow.has_value() ? 0 : 1;
            }

            EXPECT_EQ(output.getNullCount(), nullCount);
            EXPECT_EQ(output.hasMask(), nullCount != 0);
            firstRow += chunkRowCount;
        }

        EXPECT_EQ(firstRow, rowCount);
    }

    std::unique_ptr<Graph> _graph;
};

TEST_F(GetPropertiesWithNullWordBoundaryTest, nullsAtWordBoundaries) {
    for (const size_t rowCount : {63, 64, 65, 129}) {
        SCOPED_TRACE(rowCount);

        std::set<size_t> nullRows;
        for (const size_t row : {size_t {0}, size_t {62}, size_t {63}, size_t {64}, rowCount - 1}) {
            if (row < rowCount) {
                nullRows.insert(row);
            }
        }

        expectChunks(rowCount, nullRows, rowCount);
    }
}

TEST_F(GetPropertiesWithNullWordBoundaryTest, allNull) {
    for (const size_t rowCount : {63, 64, 65}) {
        SCOPED_TRACE(rowCount);
        expectChunks(rowCount, rowsBetween(0, rowCount), rowCount);
    }
}

TEST_F(GetPropertiesWithNullWordBoundaryTest, noNull) {
    for (const size_t rowCount : {63, 64, 65}) {
        SCOPED_TRACE(rowCount);
        expectChunks(rowCount, {}, rowCount);
    }
}

// The output is reused: a chunk with nulls, one with none, one all null, then one with none
TEST_F(GetPropertiesWithNullWordBoundaryTest, chunksReuseTheOutput) {
    std::set<size_t> nullRows = rowsBetween(130, 195);
    for (size_t row = 0; row < 65; row += 3) {
        nullRows.insert(row);
    }

    expectChunks(200, nullRows, 65);
}
