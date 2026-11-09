#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <array>
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

namespace {

// A column of one schema kind, as a procedure yields it. SimpleGraph has 9 labels, 2 edge
// types and 8 property types, whose 3 value types are held by 2, 3 and 3 of them, and 8
// Person nodes with 15 INTERESTED_IN edges.
struct SchemaColumn {
    std::string_view _call;
    std::string_view _column;
    size_t _rows {0};
    size_t _equalPairs {0};
};

constexpr std::array<SchemaColumn, 4> SCHEMA_COLUMNS {{
    {"CALL db.labels() YIELD id", "id", 9, 9},
    {"CALL db.edgeTypes() YIELD id", "id", 2, 2},
    {"CALL db.propertyTypes() YIELD id", "id", 8, 8},
    {"CALL db.propertyTypes() YIELD valueType", "valueType", 8, 22},
}};

}

// A label, edge type or property type ID, or a value type, imported by a body run once per
// row, which reads it as the constant it is for the run
class PerRunImportSchemaIDTest : public WriteQueryTest {
protected:
    void expectImportRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
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

    static void splitRows(std::string_view presentValue,
                          std::string_view nullValue,
                          size_t presentRows,
                          size_t nullRows,
                          std::vector<StringRowSink::Row>& rows) {
        rows.push_back({std::string(presentValue), std::to_string(presentRows)});

        if (nullRows > 0) {
            rows.push_back({std::string(nullValue), std::to_string(nullRows)});
        }
    }

    // @param over binds x to a value of @param schema's column, null on @param nullRows rows
    void expectImported(const SchemaColumn& schema, const std::string& over, size_t nullRows) {
        const size_t presentRows = schema._rows;
        const std::string column(schema._column);

        const std::string allRows = std::to_string(presentRows + nullRows);
        const std::string personRows = std::to_string(8 * (presentRows + nullRows));
        const std::string presentPersonRows = std::to_string(8 * presentRows);

        std::vector<StringRowSink::Row> people;
        splitRows("8", "0", presentRows, nullRows, people);

        std::vector<StringRowSink::Row> interests;
        splitRows("15", "0", presentRows, nullRows, interests);

        std::vector<StringRowSink::Row> one;
        splitRows("1", "0", presentRows, nullRows, one);

        std::vector<StringRowSink::Row> same;
        splitRows("true", "null", presentRows, nullRows, same);

        std::vector<StringRowSink::Row> found;
        splitRows("true", "false", presentRows, nullRows, found);

        std::vector<StringRowSink::Row> headIsNull;
        splitRows("false", "true", presentRows, nullRows, headIsNull);

        expectImportRows(over + "CALL { WITH x MATCH (n:Person) RETURN count(n) AS c } RETURN c, count(*)",
                         {{"8", allRows}});
        expectImportRows(over + "CALL (x) { MATCH (n:Person) RETURN count(n) AS c } RETURN c, count(*)",
                         {{"8", allRows}});
        expectImportRows(over + "CALL { WITH x MATCH (n:Person) RETURN count(x) AS c } RETURN c, count(*)", people);
        expectImportRows(over + "CALL { WITH x RETURN collect(x) AS c } RETURN size(c), count(*)", one);
        expectImportRows(over + "CALL (x) { RETURN collect(x) AS c } RETURN size(c), count(*)", one);
        expectImportRows(over + "CALL { WITH x WITH x LIMIT 1 RETURN x AS y } RETURN x = y, count(*)", same);
        expectImportRows(over + "CALL (x) { MATCH (n:Person) WITH n LIMIT 20 RETURN x AS y } RETURN count(*), count(y)",
                         {{personRows, presentPersonRows}});
        expectImportRows(over + "CALL (x) { UNWIND [1, 2, 3] AS n WITH count(n) AS c "
                                "RETURN CASE WHEN c = 3 THEN x END AS y } RETURN x = y, count(*)",
                         same);
        expectImportRows(over + "CALL (x) { UNWIND [1, 2, 3] AS n WITH count(n) AS c RETURN x AS y } RETURN x = y, count(*)",
                         same);
        expectImportRows(over + "CALL (x) { UNWIND [1, 2, 3] AS n WITH DISTINCT x RETURN x AS y } RETURN x = y, count(*)",
                         same);
        expectImportRows(over + "CALL (x) { UNWIND [1, 2, 3] AS n WITH collect(x) AS all RETURN x IN all AS found } "
                                "RETURN found, count(*)",
                         found);
        expectImportRows(over + "CALL (x) { UNWIND [1, 2, 3] AS n WITH count(n) AS c RETURN [x, x] AS pair } "
                                "RETURN size(pair), count(*)",
                         {{"2", allRows}});
        expectImportRows(over + "CALL { WITH x " + std::string(schema._call) + " WITH " + column + " WHERE " + column
                             + " = x RETURN count(*) AS c } RETURN sum(c)",
                         {{std::to_string(schema._equalPairs)}});

        expectImportRows(over + "WITH x, COUNT { MATCH (n:Person) WITH n LIMIT 20 WITH n WHERE x IS NOT NULL RETURN n } AS c "
                                "RETURN c, count(*)",
                         people);
        expectImportRows(over + "WITH x, EXISTS { MATCH (n:Person) WITH n LIMIT 20 WITH n WHERE x IS NOT NULL RETURN n } AS found "
                                "RETURN found, count(*)",
                         found);
        expectImportRows(over + "WITH x, COLLECT { MATCH (n:Person) WITH n LIMIT 20 RETURN x } AS c RETURN size(c), count(*)",
                         {{"8", allRows}});
        expectImportRows(over + "WITH x, COLLECT { UNWIND [1, 2, 3] AS n WITH count(n) AS c RETURN x } AS c "
                                "RETURN head(c) IS NULL, count(*)",
                         headIsNull);
        expectImportRows(over + "WITH x, [(n:Person)-[:INTERESTED_IN]->(i) WHERE x IS NOT NULL | x] AS c "
                                "RETURN size(c), count(*)",
                         interests);
    }

    const std::vector<size_t> _chunkSizes {2, 7, ChunkConfig::CHUNK_SIZE};
};

TEST_F(PerRunImportSchemaIDTest, countsPeopleOncePerImportedLabelID) {
    expectImportRows("CALL db.labels() YIELD id WITH id LIMIT 3 "
                     "CALL { WITH id MATCH (n:Person) RETURN count(n) AS c } RETURN c",
                     {{"8"}, {"8"}, {"8"}});
    expectImportRows("CALL db.labels() YIELD id WITH id LIMIT 3 CALL { WITH id RETURN collect(id) AS c } RETURN size(c)",
                     {{"1"}, {"1"}, {"1"}});
}

TEST_F(PerRunImportSchemaIDTest, importsASchemaID) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);

        expectImported(schema, std::string(schema._call) + " WITH " + column + " AS x ", 0);
    }
}

TEST_F(PerRunImportSchemaIDTest, importsASchemaIDACaseLeavesNull) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);

        expectImported(schema,
                       "UNWIND [1, 2] AS k " + std::string(schema._call) + " WITH CASE WHEN k = 1 THEN " + column
                           + " END AS x ",
                       schema._rows);
    }
}

TEST_F(PerRunImportSchemaIDTest, importsASchemaIDAnOptionalCallPadsWithNull) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);

        expectImported(schema,
                       "UNWIND [1, 2] AS k OPTIONAL CALL { WITH k " + std::string(schema._call) + " WITH " + column
                           + " WHERE k = 1 RETURN " + column + " AS x } ",
                       1);
    }
}
