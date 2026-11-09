#include <gtest/gtest.h>

#include <stddef.h>

#include <array>
#include <string>
#include <string_view>

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

namespace {

// A column of one schema kind, as a procedure yields it. SimpleGraph has 9 labels, 2 edge
// types and 8 property types.
struct SchemaColumn {
    std::string_view _call;
    std::string_view _column;
    size_t _rows {0};
};

constexpr std::array<SchemaColumn, 4> SCHEMA_COLUMNS {{
    {"CALL db.labels() YIELD id", "id", 9},
    {"CALL db.edgeTypes() YIELD id", "id", 2},
    {"CALL db.propertyTypes() YIELD id", "id", 8},
    {"CALL db.propertyTypes() YIELD valueType", "valueType", 8},
}};

}

class SchemaIDEmptyOrNullListTest : public WriteQueryTest {
};

TEST_F(SchemaIDEmptyOrNullListTest, filtersOnAnEmptyOrNullList) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string call(schema._call);
        const std::string column(schema._column);

        expectRows(call + " WHERE " + column + " IN [] RETURN " + column, {});
        expectRows(call + " WHERE " + column + " IN [null] RETURN " + column, {});
        expectRows(call + " WHERE NOT " + column + " IN [] RETURN count(*)", {{std::to_string(schema._rows)}});
        expectRows(call + " WHERE NOT " + column + " IN [null] RETURN count(*)", {{"0"}});
    }
}

TEST_F(SchemaIDEmptyOrNullListTest, projectsFalseOverAnEmptyListAndNullOverANullList) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);

        expectRows(std::string(schema._call) + " RETURN " + column + " IN [], " + column + " IN [null]",
                   Rows(schema._rows, Row {"false", "null"}));
        expectRows(std::string(schema._call) + " RETURN " + column + " IN [null, null], count(*)",
                   {{"null", std::to_string(schema._rows)}});
    }
}

TEST_F(SchemaIDEmptyOrNullListTest, searchesWithANullableSide) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string call(schema._call);
        const std::string column(schema._column);
        const std::string rows = std::to_string(schema._rows);

        expectRows("UNWIND [1, 2] AS k " + call + " WITH k, CASE WHEN k = 1 THEN " + column
                       + " END AS x RETURN k, x IN [], x IN [null], count(*)",
                   {{"1", "false", "null", rows}, {"2", "false", "null", rows}});
        expectRows("UNWIND [1, 2] AS k OPTIONAL CALL { WITH k " + call + " WITH " + column
                       + " WHERE k = 1 RETURN " + column + " AS x } RETURN k, x IN [], x IN [null], count(*)",
                   {{"1", "false", "null", rows}, {"2", "false", "null", "1"}});
    }
}

TEST_F(SchemaIDEmptyOrNullListTest, searchesAListColumn) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string call(schema._call);
        const std::string column(schema._column);
        const std::string rows = std::to_string(schema._rows);

        expectRows(call + " WITH " + column + ", [] AS empty, [null] AS nulls RETURN " + column + " IN empty, "
                       + column + " IN nulls, count(*)",
                   {{"false", "null", rows}});
        expectRows("UNWIND [0, 1] AS k " + call + " RETURN k, " + column
                       + " IN [i IN range(1, k) | null], count(*)",
                   {{"0", "false", rows}, {"1", "null", rows}});
        expectRows("UNWIND [1, 2] AS k " + call + " RETURN k, " + column
                       + " IN CASE WHEN k = 1 THEN [] END, count(*)",
                   {{"1", "false", rows}, {"2", "null", rows}});
    }
}
