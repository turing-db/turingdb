#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include "iterators/ChunkConfig.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "StringRowSink.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

// A UNION or a WHEN whose branches give a column a label, edge type or property type ID, or
// a value type, in one branch and null in another. A label's name, like every column a
// procedure yields, is typed only in lowering. SimpleGraph has 9 labels, 2 edge types and
// 8 property types of 3 value types.
class UnionSchemaIDNullBranchTest : public WriteQueryTest {
protected:
    void resultRows(std::string_view query, size_t chunkSize, std::vector<StringRowSink::Row>& rows) {
        _interpreter->setChunkSize(chunkSize);

        StringRowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nchunk size: " << chunkSize
                                   << "\nerror: " << status.getError();

        sink.sortedRows(rows);
    }

    void expectResultRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        for (const size_t chunkSize : _chunkSizes) {
            std::vector<StringRowSink::Row> actual;
            resultRows(query, chunkSize, actual);

            EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\nchunk size: " << chunkSize;
        }
    }

    void yieldedRows(const std::string& yielded, bool distinct, std::vector<StringRowSink::Row>& rows) {
        resultRows(yielded, ChunkConfig::CHUNK_SIZE, rows);

        if (distinct) {
            rows.erase(std::unique(rows.begin(), rows.end()), rows.end());
        }
    }

    void expectNullBranchHeld(std::string_view call, std::string_view column, size_t rows, size_t distinctValues) {
        const std::string calling(call);
        const std::string value(column);
        const std::string yielded = calling + " RETURN " + value + " AS x";
        const std::string guarded = calling + " WITH k, " + value + " WHERE k = 1 RETURN " + value + " AS x";

        std::vector<StringRowSink::Row> everyValue;
        yieldedRows(yielded, false, everyValue);
        ASSERT_EQ(everyValue.size(), rows);

        std::vector<StringRowSink::Row> eachValue;
        yieldedRows(yielded, true, eachValue);
        ASSERT_EQ(eachValue.size(), distinctValues);

        std::vector<StringRowSink::Row> everyValueAndNull = everyValue;
        everyValueAndNull.push_back({"null"});

        std::vector<StringRowSink::Row> eachValueAndNull = eachValue;
        eachValueAndNull.push_back({"null"});

        const std::string present = std::to_string(rows);
        const std::string distinct = std::to_string(distinctValues);
        const std::string bothRows = std::to_string(2 * rows);

        expectResultRows(yielded + " UNION RETURN null AS x", eachValueAndNull);
        expectResultRows("RETURN null AS x UNION " + yielded, eachValueAndNull);
        expectResultRows(yielded + " UNION ALL RETURN null AS x", everyValueAndNull);
        expectResultRows("RETURN null AS x UNION ALL " + yielded, everyValueAndNull);
        expectResultRows("UNWIND [1, 2] AS j " + yielded + " UNION RETURN null AS x UNION RETURN null AS x",
                         eachValueAndNull);
        expectResultRows(yielded + " UNION " + yielded, eachValue);

        expectResultRows("UNWIND [1, 2] AS k CALL { WITH k " + guarded + " UNION RETURN null AS x } "
                         "RETURN k, x IS NULL, count(*)",
                         {{"1", "false", distinct}, {"1", "true", "1"}, {"2", "true", "1"}});
        expectResultRows("UNWIND [1, 2] AS k CALL { RETURN null AS x UNION WITH k " + guarded + " } "
                         "RETURN k, x IS NULL, count(*)",
                         {{"1", "false", distinct}, {"1", "true", "1"}, {"2", "true", "1"}});
        expectResultRows("UNWIND [1, 2] AS k CALL { WITH k " + guarded + " UNION ALL RETURN null AS x } "
                         "RETURN k, x IS NULL, count(*)",
                         {{"1", "false", present}, {"1", "true", "1"}, {"2", "true", "1"}});
        expectResultRows("UNWIND [1, 2] AS k CALL { WITH k " + guarded + " UNION " + yielded + " } RETURN k, count(*)",
                         {{"1", distinct}, {"2", distinct}});

        expectResultRows("WHEN 1 = 2 THEN " + yielded + " ELSE RETURN null AS x", {{"null"}});
        expectResultRows("WHEN 1 = 1 THEN " + yielded + " ELSE RETURN null AS x", everyValue);
        expectResultRows("UNWIND [1, 2] AS k CALL (k) { WHEN k = 1 THEN " + yielded + " ELSE RETURN null AS x } "
                         "RETURN k, x IS NULL, count(*)",
                         {{"1", "false", present}, {"2", "true", "1"}});
        expectResultRows("UNWIND [1, 2] AS k CALL (k) { WHEN k = 1 THEN RETURN null AS x ELSE " + yielded + " } "
                         "RETURN k, x IS NULL, count(*)",
                         {{"1", "true", "1"}, {"2", "false", present}});

        const std::string selected = "UNWIND [1, 2] AS k " + calling + " RETURN CASE WHEN k = 1 THEN " + value + " END AS x";

        expectResultRows(selected + " UNION " + yielded, eachValueAndNull);
        expectResultRows(yielded + " UNION " + selected, eachValueAndNull);
        expectResultRows("CALL { " + selected + " UNION ALL " + yielded + " } RETURN x IS NULL, count(*)",
                         {{"false", bothRows}, {"true", present}});
    }

    const std::vector<size_t> _chunkSizes {2, 7, ChunkConfig::CHUNK_SIZE};
};

TEST_F(UnionSchemaIDNullBranchTest, holdsANullBesideALabelID) {
    expectNullBranchHeld("CALL db.labels() YIELD id", "id", 9, 9);
}

TEST_F(UnionSchemaIDNullBranchTest, holdsANullBesideAnEdgeTypeID) {
    expectNullBranchHeld("CALL db.edgeTypes() YIELD id", "id", 2, 2);
}

TEST_F(UnionSchemaIDNullBranchTest, holdsANullBesideAPropertyTypeID) {
    expectNullBranchHeld("CALL db.propertyTypes() YIELD id", "id", 8, 8);
}

TEST_F(UnionSchemaIDNullBranchTest, holdsANullBesideAValueType) {
    expectNullBranchHeld("CALL db.propertyTypes() YIELD valueType", "valueType", 8, 3);
}

TEST_F(UnionSchemaIDNullBranchTest, holdsANullBesideALabelName) {
    expectNullBranchHeld("CALL db.labels() YIELD label", "label", 9, 9);
}
