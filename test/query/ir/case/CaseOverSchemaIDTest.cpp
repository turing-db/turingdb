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

// A CASE or a coalesce over a label, edge type or property type ID, or a value type, as
// db.labels(), db.edgeTypes() and db.propertyTypes() yield them. SimpleGraph has 9 labels,
// 2 edge types and 8 property types of 3 value types. Every query unwinds k over [1, 2],
// so each yielded row comes once with k = 1 and once with k = 2.
class CaseOverSchemaIDTest : public WriteQueryTest {
protected:
    void selectedRows(std::string_view query, size_t chunkSize, std::vector<StringRowSink::Row>& rows) {
        _interpreter->setChunkSize(chunkSize);

        StringRowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        sink.sortedRows(rows);
    }

    void expectSelectedRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        for (const size_t chunkSize : _chunkSizes) {
            std::vector<StringRowSink::Row> actual;
            selectedRows(query, chunkSize, actual);

            EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\nchunk size: " << chunkSize;
        }
    }

    // A selection hands every row the value it was given: each row reads its column twice,
    // once as it is and once through @param selection
    void expectSelectedValues(std::string_view call, std::string_view column, std::string_view selection) {
        const std::string plain = std::string(call) + " RETURN " + std::string(column) + " AS v, "
                                + std::string(column) + " AS x";

        std::vector<StringRowSink::Row> expected;
        selectedRows(plain, ChunkConfig::CHUNK_SIZE, expected);

        const std::vector<StringRowSink::Row> doubled = expected;
        expected.insert(expected.end(), doubled.begin(), doubled.end());

        expectSelectedRows("UNWIND [1, 2] AS k " + std::string(call) + " RETURN "
                               + std::string(column) + " AS v, " + std::string(selection) + " AS x",
                           expected);
    }

    void expectSelectedColumn(std::string_view call, std::string_view column, size_t rows, size_t distinctValues) {
        const std::string over = "UNWIND [1, 2] AS k " + std::string(call) + " ";
        const std::string id(column);
        const std::string yielded = std::to_string(rows);
        const std::string bothRows = std::to_string(2 * rows);

        expectSelectedRows(over + "RETURN count(CASE WHEN k = 1 THEN " + id + " END)", {{yielded}});
        expectSelectedRows(over + "RETURN count(CASE k WHEN 1 THEN " + id + " END)", {{yielded}});
        expectSelectedRows(over + "RETURN count(CASE WHEN k = 1 THEN null ELSE " + id + " END)", {{yielded}});
        expectSelectedRows(over + "RETURN count(CASE WHEN k = 1 THEN " + id + " ELSE " + id + " END)", {{bothRows}});
        expectSelectedRows(over + "RETURN count(coalesce(" + id + ", " + id + "))", {{bothRows}});
        expectSelectedRows(over + "RETURN count(coalesce(null, " + id + "))", {{bothRows}});
        expectSelectedRows(over + "RETURN count(coalesce(CASE WHEN k = 1 THEN " + id + " END, " + id + "))",
                           {{bothRows}});

        expectSelectedRows(over + "RETURN count(DISTINCT CASE WHEN k = 1 THEN " + id + " END)",
                           {{std::to_string(distinctValues)}});
        expectSelectedRows(over + "WITH k, CASE WHEN k = 1 THEN " + id + " END AS x RETURN k, x IS NULL, count(*)",
                           {{"1", "false", yielded}, {"2", "true", yielded}});
        expectSelectedRows(over + "WITH k, CASE WHEN k = 1 THEN " + id + " END AS x "
                                  "WHERE k = 2 RETURN DISTINCT x, x IS NOT NULL",
                           {{"null", "false"}});

        expectSelectedValues(call, column, "coalesce(CASE WHEN k = 1 THEN " + id + " END, " + id + ")");
        expectSelectedValues(call, column, "CASE WHEN k = 1 THEN " + id + " ELSE " + id + " END");
        expectSelectedValues(call, column, "coalesce(" + id + ", " + id + ")");
    }

    // A row with k = 0 takes the first branch and never reaches the second, which divides by k
    void expectGuardedSelection(std::string_view call, std::string_view column, size_t rows) {
        const std::string id(column);

        expectSelectedRows("UNWIND [0, 1] AS k " + std::string(call)
                               + " RETURN count(CASE WHEN k = 0 THEN null WHEN 10 / k > 1 THEN " + id + " END)",
                           {{std::to_string(rows)}});
    }

    const std::vector<size_t> _chunkSizes {2, 7, ChunkConfig::CHUNK_SIZE};
};

TEST_F(CaseOverSchemaIDTest, selectsALabelID) {
    expectSelectedColumn("CALL db.labels() YIELD id", "id", 9, 9);
    expectGuardedSelection("CALL db.labels() YIELD id", "id", 9);
}

TEST_F(CaseOverSchemaIDTest, selectsAnEdgeTypeID) {
    expectSelectedColumn("CALL db.edgeTypes() YIELD id", "id", 2, 2);
    expectGuardedSelection("CALL db.edgeTypes() YIELD id", "id", 2);
}

TEST_F(CaseOverSchemaIDTest, selectsAPropertyTypeID) {
    expectSelectedColumn("CALL db.propertyTypes() YIELD id", "id", 8, 8);
    expectGuardedSelection("CALL db.propertyTypes() YIELD id", "id", 8);
}

TEST_F(CaseOverSchemaIDTest, selectsAValueType) {
    expectSelectedColumn("CALL db.propertyTypes() YIELD valueType", "valueType", 8, 3);
    expectGuardedSelection("CALL db.propertyTypes() YIELD valueType", "valueType", 8);
}

TEST_F(CaseOverSchemaIDTest, selectsALabelAsAString) {
    expectSelectedColumn("CALL db.labels() YIELD label", "label", 9, 9);
}
