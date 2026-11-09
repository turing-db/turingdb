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

// count(x) and count(DISTINCT x) over a label, edge type or property type ID, or a value
// type, that is null where k = 2. SimpleGraph has 9 labels, 2 edge types and 8 property
// types of 3 value types. A CASE or a coalesce leaves every row of k = 2 null, and an
// OPTIONAL CALL pads the one row of k = 2 its body misses.
class CountOverNullableSchemaIDTest : public WriteQueryTest {
protected:
    void expectCountedRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
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

    void expectCounts(const std::string& over, size_t presentRows, size_t distinctValues, size_t nullRows) {
        const std::string present = std::to_string(presentRows);
        const std::string distinct = std::to_string(distinctValues);
        const std::string missed = std::to_string(nullRows);
        const std::string allRows = std::to_string(presentRows + nullRows);

        expectCountedRows(over + "RETURN k, count(x)", {{"1", present}, {"2", "0"}});
        expectCountedRows(over + "RETURN k, count(DISTINCT x)", {{"1", distinct}, {"2", "0"}});
        expectCountedRows(over + "RETURN k, count(*), count(x), count(DISTINCT x)",
                          {{"1", present, present, distinct}, {"2", missed, "0", "0"}});
        expectCountedRows(over + "RETURN x IS NULL, count(x), count(DISTINCT x)",
                          {{"false", present, distinct}, {"true", "0", "0"}});

        expectCountedRows(over + "RETURN k, size(collect(k)), count(x), count(DISTINCT x)",
                          {{"1", present, present, distinct}, {"2", missed, "0", "0"}});
        expectCountedRows(over + "RETURN size(collect(k)), count(x), count(DISTINCT x)",
                          {{allRows, present, distinct}});
        expectCountedRows(over + "RETURN count(x), count(DISTINCT x)", {{present, distinct}});
    }

    void expectCountsOverEveryProducer(std::string_view call, std::string_view column, size_t rows, size_t distinctValues) {
        const std::string calling(call);
        const std::string yielded(column);
        const std::string selected = "CASE WHEN k = 1 THEN " + yielded + " END";
        const std::string unwound = "UNWIND [1, 2] AS k " + calling;
        const std::string optionalCall = "UNWIND [1, 2] AS k OPTIONAL CALL { WITH k " + calling + " WITH " + yielded
                                       + " WHERE k = 1 RETURN " + yielded + " AS x } ";

        expectCounts(unwound + " WITH k, " + selected + " AS x ", rows, distinctValues, rows);
        expectCounts(unwound + " WITH k, coalesce(" + selected + ", null) AS x ", rows, distinctValues, rows);
        expectCounts(optionalCall, rows, distinctValues, 1);
    }

    const std::vector<size_t> _chunkSizes {2, 7, ChunkConfig::CHUNK_SIZE};
};

TEST_F(CountOverNullableSchemaIDTest, countsALabelID) {
    expectCountsOverEveryProducer("CALL db.labels() YIELD id", "id", 9, 9);
}

TEST_F(CountOverNullableSchemaIDTest, countsAnEdgeTypeID) {
    expectCountsOverEveryProducer("CALL db.edgeTypes() YIELD id", "id", 2, 2);
}

TEST_F(CountOverNullableSchemaIDTest, countsAPropertyTypeID) {
    expectCountsOverEveryProducer("CALL db.propertyTypes() YIELD id", "id", 8, 8);
}

TEST_F(CountOverNullableSchemaIDTest, countsAValueType) {
    expectCountsOverEveryProducer("CALL db.propertyTypes() YIELD valueType", "valueType", 8, 3);
}

TEST_F(CountOverNullableSchemaIDTest, countsASelectionInsideTheAggregate) {
    expectCountedRows("UNWIND [1, 2] AS k CALL db.labels() YIELD id RETURN k, count(CASE WHEN k = 1 THEN id END)",
                      {{"1", "9"}, {"2", "0"}});
    expectCountedRows("UNWIND [1, 2] AS k CALL db.labels() YIELD id "
                      "RETURN size(collect(k)), count(CASE WHEN k = 1 THEN id END)",
                      {{"18", "9"}});
}
