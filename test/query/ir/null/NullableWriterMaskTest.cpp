#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnOptVector.h"
#include "versioning/ChangeID.h"

#include "StringRowSink.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr size_t ANY_ROW_COUNT = 0;

struct MaskFollowsNulls {
    size_t& _violations;

    template <typename T>
    void operator()(const ColumnOptVector<T>* column) {
        if (column->hasMask() != (column->getNullCount() != 0)) {
            _violations++;
        }
    }

    template <typename ColumnType>
    void operator()(const ColumnType*) {
    }
};

// Counts the nullable chunks that keep a validity mask with no null row, or hold a null row
// with no mask, and renders the rows as text
class MaskCheckingSink : public NLOutputSink {
public:
    void declareOutput(std::span<const std::string_view> names, std::span<const Column* const> chunks) override {
        _rows.declareOutput(names, chunks);
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        using Dispatcher = ColumnSingleDispatcher<OutputtedTypes::Allowed, MaskFollowsNulls, OutputtedTypes::Excluded>;

        MaskFollowsNulls check {_violations};
        for (const Column* chunk : chunks) {
            Dispatcher::dispatch(chunk, check);
        }

        _rows.appendChunks(chunks, offset, rowCount);
    }

    size_t getViolations() const { return _violations; }
    const std::vector<StringRowSink::Row>& getRows() const { return _rows.getRows(); }

private:
    StringRowSink _rows;
    size_t _violations {0};
};

std::string integerList(size_t count, const std::vector<size_t>& nullPositions) {
    std::string list = "[";
    for (size_t position = 0; position < count; position++) {
        if (position != 0) {
            list += ", ";
        }

        const bool isNull = std::find(nullPositions.begin(), nullPositions.end(), position) != nullPositions.end();
        list += isNull ? "null" : std::to_string(position + 1);
    }

    return list + "]";
}

}

class NullableWriterMaskTest : public WriteQueryTest {
protected:
    // Every nullable chunk of the result holds a mask exactly when it holds a null. The last
    // column holds @param expectedNulls nulls over @param expectedRows rows
    void expectMaskFollowsNulls(const std::string& query, size_t expectedRows, size_t expectedNulls) {
        expectMaskFollowsNulls(query, ChangeID::head(), expectedRows, expectedNulls);
    }

    void expectMaskFollowsNulls(const std::string& query, const ChangeID& changeID, size_t expectedRows, size_t expectedNulls) {
        for (const size_t chunkSize : CHUNK_SIZES) {
            _interpreter->setChunkSize(chunkSize);

            MaskCheckingSink sink;
            const QueryStatus status = runQuery(query, changeID, &sink);
            ASSERT_TRUE(status.isOk()) << "query: " << query << "\nchunk size: " << chunkSize
                                       << "\nerror: " << status.getError();

            EXPECT_EQ(sink.getViolations(), 0u) << "query: " << query << "\nchunk size: " << chunkSize;

            const std::vector<StringRowSink::Row>& rows = sink.getRows();
            if (expectedRows != ANY_ROW_COUNT) {
                EXPECT_EQ(rows.size(), expectedRows) << "query: " << query << "\nchunk size: " << chunkSize;
            } else {
                EXPECT_FALSE(rows.empty()) << "query: " << query;
            }

            size_t nulls = 0;
            for (const StringRowSink::Row& row : rows) {
                nulls += row.back() == "null" ? 1 : 0;
            }

            EXPECT_EQ(nulls, expectedNulls) << "query: " << query << "\nchunk size: " << chunkSize;
        }
    }

    static constexpr size_t CHUNK_SIZES[] = {63, 64, 65, 129};
};

TEST_F(NullableWriterMaskTest, unwoundRange) {
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN x", 300, 0);
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN x * 2", 300, 0);
}

TEST_F(NullableWriterMaskTest, unwoundLiteralList) {
    expectMaskFollowsNulls("UNWIND " + integerList(300, {}) + " AS x RETURN x", 300, 0);
    expectMaskFollowsNulls("UNWIND " + integerList(300, {62, 63, 64, 127, 128, 299}) + " AS x RETURN x", 300, 6);
}

TEST_F(NullableWriterMaskTest, unwoundListColumn) {
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x WITH [x, x + 1] AS l UNWIND l AS y RETURN y", 600, 0);
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x WITH collect(x) AS xs UNWIND xs AS y RETURN y", 300, 0);
}

TEST_F(NullableWriterMaskTest, caseExpression) {
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN CASE WHEN x > 0 THEN x END", 300, 0);
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN CASE WHEN x % 64 <> 0 THEN x END", 300, 4);
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN reduce(s = 0, y IN [x, x] | s + y)", 300, 0);
}

TEST_F(NullableWriterMaskTest, stringFunctions) {
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN substring(toString(x), 0, 1)", 300, 0);
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN left(toString(x), 1)", 300, 0);
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN right(toString(x), 1)", 300, 0);
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN replace(toString(x), '1', 'a')", 300, 0);
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN size(split(toString(x), '0'))", 300, 0);
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x "
                           "RETURN substring(CASE WHEN x % 64 <> 0 THEN toString(x) END, 0, 1)",
                           300,
                           4);
}

TEST_F(NullableWriterMaskTest, listFunctions) {
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN all(y IN [x, x + 1] WHERE y > 0)", 300, 0);
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN any(y IN [x] WHERE y > 150)", 300, 0);
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN [x, x + 1][0]", 300, 0);
}

TEST_F(NullableWriterMaskTest, pointFunctions) {
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x "
                           "RETURN point.distance(point({x: toFloat(x), y: 0.0}), point({x: 0.0, y: 0.0}))",
                           300,
                           0);
}

TEST_F(NullableWriterMaskTest, groupedAggregates) {
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN x % 100 AS k, sum(x)", 100, 0);
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN x % 100 AS k, max(x)", 100, 0);
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN x % 100 AS k, avg(x)", 100, 0);
    expectMaskFollowsNulls("UNWIND range(1, 300) AS x RETURN x % 100 AS k, percentileCont(x, 0.5)", 100, 0);
}

TEST_F(NullableWriterMaskTest, pathLength) {
    expectMaskFollowsNulls("MATCH p = (a)-->(b) RETURN length(p)", ANY_ROW_COUNT, 0);
}

TEST_F(NullableWriterMaskTest, propertyReadOverItsWrites) {
    applyWrite("UNWIND range(1, 300) AS x CREATE (:Item {k: x})");
    applyWrite("CREATE (:Item {k: 0, v: 0})");

    ChangeID changeID;
    openChange(changeID);
    expectMaskFollowsNulls("MATCH (n:Item) WHERE n.k > 0 SET n.v = n.k RETURN n.v", changeID, 300, 0);
}
