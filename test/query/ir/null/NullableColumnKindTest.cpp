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

// A label, edge type or property type ID, a value type or an embedding, null on some rows,
// carried through LIMIT, SKIP, DISTINCT, ORDER BY and UNWIND. SimpleGraph has 9 labels, 2
// edge types and 8 property types of 3 value types. A CASE leaves every row of k = 2 null,
// and an OPTIONAL CALL pads the one row of k = 2 its body misses.
class NullableColumnKindTest : public WriteQueryTest {
protected:
    void expectCarriedRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        for (const size_t chunkSize : _chunkSizes) {
            _interpreter->setChunkSize(chunkSize);

            StringRowSink sink;
            const QueryStatus status = runQuery(query, &sink);
            ASSERT_TRUE(status.isOk()) << "query: " << query << "\nchunk size: " << chunkSize
                                       << "\nerror: " << status.getError();

            std::vector<StringRowSink::Row> actual;
            sink.sortedRows(actual);

            EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\nchunk size: " << chunkSize;
        }
    }

    // @param over binds x, present on @param presentRows rows and null on @param nullRows
    void expectRowsCarried(const std::string& over, size_t presentRows, size_t nullRows) {
        const std::string present = std::to_string(presentRows);
        const std::string allRows = std::to_string(presentRows + nullRows);
        const std::string skippedRows = std::to_string(presentRows + nullRows - 1);

        expectCarriedRows(over + "WITH x LIMIT 20 RETURN count(*), count(x)", {{allRows, present}});
        expectCarriedRows(over + "WITH x LIMIT 2 RETURN count(*)", {{"2"}});
        expectCarriedRows(over + "WITH x SKIP 1 LIMIT 20 RETURN count(*)", {{skippedRows}});
        expectCarriedRows(over + "WITH x SKIP 1 RETURN count(*)", {{skippedRows}});
    }

    // @param over binds x, which holds one of @param distinctValues values on @param presentRows
    // rows of k = 1 and is null on the @param nullRows rows of k = 2
    void expectKeyCarried(const std::string& over, size_t presentRows, size_t nullRows, size_t distinctValues) {
        const std::string present = std::to_string(presentRows);
        const std::string distinct = std::to_string(distinctValues);
        const std::string distinctRows = std::to_string(distinctValues + 1);

        expectRowsCarried(over, presentRows, nullRows);

        expectCarriedRows(over + "WITH k, x LIMIT 20 RETURN k, count(x)", {{"1", present}, {"2", "0"}});
        expectCarriedRows(over + "WITH DISTINCT x LIMIT 20 RETURN count(*), count(x)", {{distinctRows, distinct}});
        expectCarriedRows(over + "WITH DISTINCT k, x LIMIT 20 RETURN k, count(x)", {{"1", distinct}, {"2", "0"}});
        expectCarriedRows(over + "WITH x ORDER BY x LIMIT 2 RETURN count(x)", {{"2"}});
        expectCarriedRows(over + "UNWIND x AS y RETURN count(*), count(DISTINCT y)", {{present, distinct}});
    }

    void expectKeyCarriedOverEveryProducer(std::string_view call, std::string_view column, size_t rows, size_t distinctValues) {
        const std::string calling(call);
        const std::string yielded(column);

        expectKeyCarried("UNWIND [1, 2] AS k " + calling + " WITH k, CASE WHEN k = 1 THEN " + yielded + " END AS x ",
                         rows,
                         rows,
                         distinctValues);

        expectKeyCarried("UNWIND [1, 2] AS k OPTIONAL CALL { WITH k " + calling + " WITH " + yielded
                             + " WHERE k = 1 RETURN " + yielded + " AS x } ",
                         rows,
                         1,
                         distinctValues);
    }

    const std::vector<size_t> _chunkSizes {2, 7, ChunkConfig::CHUNK_SIZE};
};

TEST_F(NullableColumnKindTest, carriesALabelID) {
    expectKeyCarriedOverEveryProducer("CALL db.labels() YIELD id", "id", 9, 9);
}

TEST_F(NullableColumnKindTest, carriesAnEdgeTypeID) {
    expectKeyCarriedOverEveryProducer("CALL db.edgeTypes() YIELD id", "id", 2, 2);
}

TEST_F(NullableColumnKindTest, carriesAPropertyTypeID) {
    expectKeyCarriedOverEveryProducer("CALL db.propertyTypes() YIELD id", "id", 8, 8);
}

TEST_F(NullableColumnKindTest, carriesAValueType) {
    expectKeyCarriedOverEveryProducer("CALL db.propertyTypes() YIELD valueType", "valueType", 8, 3);
}

// An embedding has no order, so it is never a sort key
TEST_F(NullableColumnKindTest, carriesAnEmbedding) {
    applyWrite("CREATE (:Doc {name: 'Alpha', vec: (1.0, 0.0, 0.0)}), "
               "(:Doc {name: 'Beta', vec: (0.0, 1.0, 0.0)}), "
               "(:Doc {name: 'Plain'})");

    expectRowsCarried("MATCH (d:Doc) WITH d.vec AS x ", 2, 1);
    expectRowsCarried("MATCH (d:Doc) WITH CASE WHEN d.name = 'Alpha' THEN d.vec END AS x ", 1, 2);
    expectRowsCarried("MATCH (d:Doc) OPTIONAL CALL { WITH d WITH d WHERE d.name = 'Alpha' RETURN d.vec AS x } ", 1, 2);

    expectCarriedRows("MATCH (d:Doc) WITH d.vec AS x MATCH (e:Doc) RETURN count(*), count(x)", {{"9", "6"}});
    expectCarriedRows("MATCH (d:Doc) WITH d.name AS name, d.vec AS x ORDER BY name LIMIT 2 RETURN count(x)", {{"2"}});
}
