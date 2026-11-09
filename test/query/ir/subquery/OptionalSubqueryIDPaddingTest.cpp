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

// OPTIONAL CALL over a body returning a label, edge type or property type ID, or a value
// type, as db.labels(), db.edgeTypes() and db.propertyTypes() yield them. SimpleGraph has 9
// labels, 2 edge types and 8 property types of 3 value types. The body yields for k = 1
// only, so k = 2 is the one row it misses, and that row reads null in the body's column.
class OptionalSubqueryIDPaddingTest : public WriteQueryTest {
protected:
    void expectPaddedRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        for (const size_t chunkSize : _chunkSizes) {
            _interpreter->setChunkSize(chunkSize);

            StringRowSink sink;
            const QueryStatus status = runQuery(query, &sink);
            ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

            std::vector<StringRowSink::Row> actual;
            sink.sortedRows(actual);

            EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\nchunk size: " << chunkSize;
        }
    }

    void expectPaddedColumn(std::string_view body, size_t yieldedRows, size_t distinctValues) {
        const std::string optionalCall = "UNWIND [1, 2] AS k OPTIONAL CALL { WITH k " + std::string(body) + " } ";
        const std::string yielded = std::to_string(yieldedRows);

        expectPaddedRows(optionalCall + "RETURN count(*), count(x)", {{std::to_string(yieldedRows + 1), yielded}});
        expectPaddedRows(optionalCall + "RETURN count(DISTINCT x)", {{std::to_string(distinctValues)}});
        expectPaddedRows(optionalCall + "RETURN k, x IS NULL, count(*)",
                         {{"1", "false", yielded}, {"2", "true", "1"}});
        expectPaddedRows(optionalCall + "WITH k, x WHERE k = 2 RETURN k, x, x IS NOT NULL",
                         {{"2", "null", "false"}});
    }

    const std::vector<size_t> _chunkSizes {2, 7, ChunkConfig::CHUNK_SIZE};
};

TEST_F(OptionalSubqueryIDPaddingTest, padsALabelIDWithNull) {
    expectPaddedColumn("CALL db.labels() YIELD id WITH id WHERE k = 1 RETURN id AS x", 9, 9);
}

TEST_F(OptionalSubqueryIDPaddingTest, padsAnEdgeTypeIDWithNull) {
    expectPaddedColumn("CALL db.edgeTypes() YIELD id WITH id WHERE k = 1 RETURN id AS x", 2, 2);
}

TEST_F(OptionalSubqueryIDPaddingTest, padsAPropertyTypeIDWithNull) {
    expectPaddedColumn("CALL db.propertyTypes() YIELD id WITH id WHERE k = 1 RETURN id AS x", 8, 8);
}

TEST_F(OptionalSubqueryIDPaddingTest, padsAValueTypeWithNull) {
    expectPaddedColumn("CALL db.propertyTypes() YIELD valueType WITH valueType WHERE k = 1 RETURN valueType AS x", 8, 3);
}

TEST_F(OptionalSubqueryIDPaddingTest, padsALabelIDOfABodyCarryingTheScope) {
    expectPaddedRows("UNWIND [1, 2] AS k "
                     "OPTIONAL CALL (k) { CALL db.labels() YIELD id WHERE k = 1 RETURN id } "
                     "RETURN k, id IS NULL, count(*)",
                     {{"1", "false", "9"}, {"2", "true", "1"}});
}

TEST_F(OptionalSubqueryIDPaddingTest, padsAStringAndANodeWithNull) {
    expectPaddedColumn("CALL db.labels() YIELD label WITH label WHERE k = 1 RETURN label AS x", 9, 9);
    expectPaddedColumn("MATCH (n:Person) WHERE k = 1 RETURN n AS x", 8, 8);
}
