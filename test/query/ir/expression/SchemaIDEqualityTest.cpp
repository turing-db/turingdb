#include <gtest/gtest.h>

#include <stddef.h>

#include <array>
#include <string>
#include <string_view>

#include "HashJoinQueryTest.h"
#include "IRTestRows.h"
#include "StringRowSink.h"
#include "WriteQueryTest.h"

using namespace turing::test;

namespace {

// A column of one schema kind, as a procedure yields it. SimpleGraph has 9 labels, 2 edge
// types and 8 property types, whose 3 value types are held by 2, 3 and 3 of them.
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

void pairsOf(const SchemaColumn& schema, std::string& query) {
    const std::string call(schema._call);
    const std::string column(schema._column);

    query = call + " WITH " + column + " AS a " + call + " WITH a, " + column + " ";
}

// The column's rows under k = 1 and again under k = 2, with x holding the value for k = 1
// and null for k = 2
void nullableHalfOf(const SchemaColumn& schema, std::string& query) {
    const std::string column(schema._column);

    query = "UNWIND [1, 2] AS k " + std::string(schema._call) + " WITH k, " + column
          + ", CASE WHEN k = 1 THEN " + column + " END AS x ";
}

}

class SchemaIDEqualityTest : public WriteQueryTest {
};

TEST_F(SchemaIDEqualityTest, filtersEqualPairs) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);
        const size_t unequalPairs = schema._rows * schema._rows - schema._equalPairs;

        std::string pairs;
        pairsOf(schema, pairs);

        expectRows(pairs + "WHERE a = " + column + " RETURN count(*)", {{std::to_string(schema._equalPairs)}});
        expectRows(pairs + "WHERE a <> " + column + " RETURN count(*)", {{std::to_string(unequalPairs)}});
    }
}

TEST_F(SchemaIDEqualityTest, projectsEquality) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);
        const size_t unequalPairs = schema._rows * schema._rows - schema._equalPairs;

        std::string pairs;
        pairsOf(schema, pairs);

        expectRows(pairs + "RETURN a = " + column + ", a <> " + column + ", count(*)",
                   {
                       {"true", "false", std::to_string(schema._equalPairs)},
                       {"false", "true", std::to_string(unequalPairs)},
                   });
    }
}

TEST_F(SchemaIDEqualityTest, comparesANullableSideAsNull) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);
        const std::string rows = std::to_string(schema._rows);

        std::string nullableHalf;
        nullableHalfOf(schema, nullableHalf);

        expectRows(nullableHalf + "RETURN k, x = x, x <> x, count(*)",
                   {{"1", "true", "false", rows}, {"2", "null", "null", rows}});
        expectRows(nullableHalf + "RETURN k, x = " + column + ", " + column + " <> x, count(*)",
                   {{"1", "true", "false", rows}, {"2", "null", "null", rows}});
        expectRows(nullableHalf + "WITH x, " + column + " WHERE " + column + " = x RETURN count(*)", {{rows}});
    }
}

TEST_F(SchemaIDEqualityTest, comparesAnOptionalCallPaddingAsNull) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);

        expectRows("UNWIND [1, 2] AS k OPTIONAL CALL { WITH k " + std::string(schema._call) + " WITH " + column
                       + " WHERE k = 1 RETURN " + column + " AS x } RETURN k, x = x, x <> x, count(*)",
                   {{"1", "true", "false", std::to_string(schema._rows)}, {"2", "null", "null", "1"}});
    }
}

TEST_F(SchemaIDEqualityTest, comparesANullLiteralAsNull) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);

        expectRows(std::string(schema._call) + " RETURN " + column + " = null, null <> " + column + ", count(*)",
                   {{"null", "null", std::to_string(schema._rows)}});
    }
}

TEST_F(SchemaIDEqualityTest, selectsACaseBranchByEquality) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);
        const size_t unequalPairs = schema._rows * schema._rows - schema._equalPairs;
        const std::string rows = std::to_string(schema._rows);

        std::string pairs;
        pairsOf(schema, pairs);

        std::string nullableHalf;
        nullableHalfOf(schema, nullableHalf);

        expectRows(pairs + "RETURN CASE a WHEN " + column + " THEN 'same' ELSE 'other' END, count(*)",
                   {{"same", std::to_string(schema._equalPairs)}, {"other", std::to_string(unequalPairs)}});
        expectRows(nullableHalf + "RETURN k, CASE x WHEN " + column + " THEN 'same' ELSE 'other' END, count(*)",
                   {{"1", "same", rows}, {"2", "other", rows}});
    }
}

TEST_F(SchemaIDEqualityTest, rejectsIDsOfTwoKinds) {
    expectError("CALL db.edgeTypes() YIELD id WITH id AS a CALL db.labels() YIELD id RETURN a = id",
                "Operands are not valid or compatible types: 'EdgeType' and 'Label'");
    expectError("CALL db.propertyTypes() YIELD id, valueType RETURN id <> valueType",
                "Operands are not valid or compatible types: 'PropertyType' and 'ValueType'");
}

class SchemaIDJoinKeyTest : public HashJoinQueryTest {
protected:
    void expectJoinedCount(std::string_view query, size_t expected) {
        std::string program;
        dbProgram(query, program);
        EXPECT_TRUE(contains(program, "db.hash_join")) << program;

        expectCount(query, expected);
    }
};

TEST_F(SchemaIDJoinKeyTest, joinsOnAnID) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);

        std::string pairs;
        pairsOf(schema, pairs);

        expectJoinedCount(pairs + "WHERE a = " + column + " RETURN count(*)", schema._equalPairs);
    }
}

TEST_F(SchemaIDJoinKeyTest, joinsANullableKeyOnAnID) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string call(schema._call);
        const std::string column(schema._column);

        expectJoinedCount("UNWIND [1, 2] AS k " + call + " WITH CASE WHEN k = 1 THEN " + column + " END AS x "
                              + call + " WITH x, " + column + " WHERE x = " + column + " RETURN count(*)",
                          schema._equalPairs);
    }
}
