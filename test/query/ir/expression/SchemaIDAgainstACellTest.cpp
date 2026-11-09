#include <gtest/gtest.h>

#include <stddef.h>

#include <array>
#include <string>
#include <string_view>

#include "HashJoinQueryTest.h"
#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace turing::test;

namespace {

// A column of one schema kind, as a procedure yields it. SimpleGraph has 9 labels, 2 edge
// types and 8 property types, whose 3 value types are held by 2, 3 and 3 of them.
struct SchemaColumn {
    std::string_view _call;
    std::string_view _column;
    std::string_view _type;
    size_t _rows {0};
    size_t _equalPairs {0};
};

constexpr std::array<SchemaColumn, 4> SCHEMA_COLUMNS {{
    {"CALL db.labels() YIELD id", "id", "Label", 9, 9},
    {"CALL db.edgeTypes() YIELD id", "id", "EdgeType", 2, 2},
    {"CALL db.propertyTypes() YIELD id", "id", "PropertyType", 8, 8},
    {"CALL db.propertyTypes() YIELD valueType", "valueType", "ValueType", 8, 22},
}};

// A tagged cell holding a value: a map value, read as a cell, and the element of a list
// holding an integer too, read as a nullable cell
struct CellShape {
    std::string_view _prefix;
    std::string_view _suffix;
};

constexpr std::array<CellShape, 2> CELL_SHAPES {{
    {"{a: ", "}.a"},
    {"([", "] + [1])[0]"},
}};

// fuse_hash_join takes no key computed through a concatenation
constexpr std::array<CellShape, 2> JOIN_KEY_CELL_SHAPES {{
    {"{a: ", "}.a"},
    {"[", ", 1][0]"},
}};

void cellOf(const CellShape& shape, std::string_view value, std::string& cell) {
    cell = std::string(shape._prefix) + std::string(value) + std::string(shape._suffix);
}

void pairsOf(const SchemaColumn& schema, std::string& query) {
    const std::string call(schema._call);
    const std::string column(schema._column);

    query = call + " WITH " + column + " AS a " + call + " WITH a, " + column + " ";
}

// The column's rows under k = 1 and again under k = 2, with y holding the value for k = 1
// and null for k = 2
void nullableHalfOf(const SchemaColumn& schema, std::string& query) {
    const std::string column(schema._column);

    query = "UNWIND [1, 2] AS k " + std::string(schema._call) + " WITH k, " + column
          + ", CASE WHEN k = 1 THEN " + column + " END AS y ";
}

size_t unequalPairsOf(const SchemaColumn& schema) {
    return schema._rows * schema._rows - schema._equalPairs;
}

}

class SchemaIDAgainstACellTest : public WriteQueryTest {
};

TEST_F(SchemaIDAgainstACellTest, equatesACellHoldingTheID) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);

        for (const CellShape& shape : CELL_SHAPES) {
            std::string cell;
            cellOf(shape, column, cell);

            expectRows(std::string(schema._call) + " RETURN " + cell + " = " + column + ", " + cell + " <> " + column
                           + ", " + column + " = " + cell + ", " + column + " <> " + cell + ", count(*)",
                       {{"true", "false", "true", "false", std::to_string(schema._rows)}});
        }
    }
}

TEST_F(SchemaIDAgainstACellTest, comparesEveryPair) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);
        const std::string equalPairs = std::to_string(schema._equalPairs);
        const std::string unequalPairs = std::to_string(unequalPairsOf(schema));

        std::string pairs;
        pairsOf(schema, pairs);

        for (const CellShape& shape : CELL_SHAPES) {
            std::string cell;
            cellOf(shape, "a", cell);

            const std::string cellPairs = pairs + "WITH " + cell + " AS x, " + column + " ";

            expectRows(cellPairs + "RETURN x = " + column + ", " + column + " <> x, count(*)",
                       {{"true", "false", equalPairs}, {"false", "true", unequalPairs}});
            expectRows(cellPairs + "WHERE x = " + column + " RETURN count(*)", {{equalPairs}});
            expectRows(cellPairs + "WHERE " + column + " <> x RETURN count(*)", {{unequalPairs}});
        }
    }
}

TEST_F(SchemaIDAgainstACellTest, comparesACellHoldingAnotherValue) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);
        const std::array<std::string, 5> cells {
            "{a: 1}.a",
            "{a: 'Person'}.a",
            "{a: [" + column + "]}.a",
            "{a: {b: " + column + "}}.a",
            "([1, " + column + "])[0]",
        };

        for (const std::string& cell : cells) {
            expectRows(std::string(schema._call) + " RETURN " + cell + " = " + column + ", " + column + " <> " + cell
                           + ", count(*)",
                       {{"false", "true", std::to_string(schema._rows)}});
        }
    }
}

TEST_F(SchemaIDAgainstACellTest, comparesACellHoldingAnIDOfAnotherKind) {
    for (const SchemaColumn& held : SCHEMA_COLUMNS) {
        for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
            if (held._type == schema._type) {
                continue;
            }

            const std::string column(schema._column);

            expectRows(std::string(held._call) + " WITH {a: " + std::string(held._column) + "} AS m "
                           + std::string(schema._call) + " RETURN m.a = " + column + ", " + column + " <> m.a, count(*)",
                       {{"false", "true", std::to_string(held._rows * schema._rows)}});
        }
    }
}

TEST_F(SchemaIDAgainstACellTest, comparesANullCellAsNull) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);
        const std::string rows = std::to_string(schema._rows);

        expectRows(std::string(schema._call) + " RETURN {a: null}.a = " + column + ", " + column
                       + " <> {a: null}.a, count(*)",
                   {{"null", "null", rows}});

        std::string nullableHalf;
        nullableHalfOf(schema, nullableHalf);

        for (const CellShape& shape : CELL_SHAPES) {
            std::string cell;
            cellOf(shape, "y", cell);

            expectRows(nullableHalf + "RETURN k, " + cell + " = " + column + ", " + column + " <> " + cell + ", count(*)",
                       {{"1", "true", "false", rows}, {"2", "null", "null", rows}});
        }
    }
}

TEST_F(SchemaIDAgainstACellTest, comparesANullableIDAsNull) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);
        const std::string rows = std::to_string(schema._rows);

        std::string nullableHalf;
        nullableHalfOf(schema, nullableHalf);

        for (const CellShape& shape : CELL_SHAPES) {
            std::string cell;
            cellOf(shape, column, cell);

            std::string nullableCell;
            cellOf(shape, "y", nullableCell);

            expectRows(nullableHalf + "RETURN k, " + cell + " = y, y <> " + cell + ", count(*)",
                       {{"1", "true", "false", rows}, {"2", "null", "null", rows}});
            expectRows(nullableHalf + "RETURN k, " + nullableCell + " = y, y <> " + nullableCell + ", count(*)",
                       {{"1", "true", "false", rows}, {"2", "null", "null", rows}});
        }
    }
}

TEST_F(SchemaIDAgainstACellTest, comparesAnOptionalCallPaddingAsNull) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);
        const std::string rows = std::to_string(schema._rows);

        for (const CellShape& shape : CELL_SHAPES) {
            std::string cell;
            cellOf(shape, column, cell);

            expectRows("UNWIND [1, 2] AS k " + std::string(schema._call) + " OPTIONAL CALL { WITH k, " + column
                           + " WITH " + column + " WHERE k = 1 RETURN " + cell + " AS x } RETURN k, x = " + column
                           + ", " + column + " <> x, count(*)",
                       {{"1", "true", "false", rows}, {"2", "null", "null", rows}});
        }
    }
}

TEST_F(SchemaIDAgainstACellTest, searchesAListOfCells) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string call(schema._call);
        const std::string column(schema._column);
        const std::string rows = std::to_string(schema._rows);

        expectRows(call + " RETURN " + column + " IN [" + column + ", 'x'], " + column + " IN [0, 'Person'], "
                       + column + " IN [0, 'x', null], count(*)",
                   {{"true", "false", "null", rows}});
        expectRows(call + " RETURN coll.indexOf([1, " + column + "], " + column + "), coll.indexOf([1, 'x'], " + column
                       + "), coll.indexOf([" + column + "], {a: " + column + "}.a), count(*)",
                   {{"1", "-1", "0", rows}});

        std::string nullableHalf;
        nullableHalfOf(schema, nullableHalf);

        expectRows(nullableHalf + "RETURN k, y IN [" + column + ", 'x'], count(*)",
                   {{"1", "true", rows}, {"2", "null", rows}});

        std::string pairs;
        pairsOf(schema, pairs);

        expectRows(pairs + "WHERE a IN [" + column + ", 'x'] RETURN count(*)", {{std::to_string(schema._equalPairs)}});

        for (const CellShape& shape : CELL_SHAPES) {
            std::string cell;
            cellOf(shape, column, cell);

            std::string pairCell;
            cellOf(shape, "a", pairCell);

            expectRows(call + " RETURN " + cell + " IN [" + column + "], count(*)", {{"true", rows}});
            expectRows(pairs + "WHERE " + pairCell + " IN [" + column + "] RETURN count(*)",
                       {{std::to_string(schema._equalPairs)}});
        }
    }
}

TEST_F(SchemaIDAgainstACellTest, selectsACaseBranchAgainstACell) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string call(schema._call);
        const std::string column(schema._column);
        const std::string rows = std::to_string(schema._rows);

        std::string pairs;
        pairsOf(schema, pairs);

        std::string nullableHalf;
        nullableHalfOf(schema, nullableHalf);

        for (const CellShape& shape : CELL_SHAPES) {
            std::string cell;
            cellOf(shape, column, cell);

            std::string pairCell;
            cellOf(shape, "a", pairCell);

            expectRows(call + " RETURN CASE " + cell + " WHEN " + column + " THEN 'same' ELSE 'other' END, CASE "
                           + column + " WHEN {a: 1}.a THEN 'one' WHEN " + cell + " THEN 'same' END, count(*)",
                       {{"same", "same", rows}});
            expectRows(pairs + "RETURN CASE " + pairCell + " WHEN " + column + " THEN 'same' ELSE 'other' END, count(*)",
                       {{"same", std::to_string(schema._equalPairs)}, {"other", std::to_string(unequalPairsOf(schema))}});
            expectRows(nullableHalf + "RETURN k, CASE y WHEN " + cell + " THEN 'same' ELSE 'other' END, count(*)",
                       {{"1", "same", rows}, {"2", "other", rows}});
        }
    }
}

TEST_F(SchemaIDAgainstACellTest, ordersACellAgainstAnIDAsNull) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);

        for (const CellShape& shape : CELL_SHAPES) {
            std::string cell;
            cellOf(shape, column, cell);

            expectRows(std::string(schema._call) + " RETURN " + cell + " < " + column + ", " + column + " >= " + cell
                           + ", count(*)",
                       {{"null", "null", std::to_string(schema._rows)}});
        }
    }
}

TEST_F(SchemaIDAgainstACellTest, keepsRejectingKnownTypes) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);
        const std::string compared = "Operands are not valid or compatible types: '" + std::string(schema._type) + "' and ";
        const std::string ordered = "Operands are not valid or compatible types for an ordering: '" + std::string(schema._type) + "' and ";

        expectError(std::string(schema._call) + " RETURN " + column + " = 0", compared + "'Integer'");
        expectError(std::string(schema._call) + " RETURN " + column + " <> 'Person'", compared + "'String'");
        expectError(std::string(schema._call) + " RETURN " + column + " < 1", ordered + "'Integer'");
        expectError(std::string(schema._call) + " RETURN " + column + " < " + column,
                    ordered + "'" + std::string(schema._type) + "'");
    }
}

class SchemaIDCellJoinKeyTest : public HashJoinQueryTest {
protected:
    void expectJoinedCount(std::string_view query, size_t expected) {
        std::string program;
        dbProgram(query, program);
        EXPECT_TRUE(contains(program, "db.hash_join")) << program;

        expectCount(query, expected);
    }
};

TEST_F(SchemaIDCellJoinKeyTest, joinsACellOnAnID) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string column(schema._column);

        std::string pairs;
        pairsOf(schema, pairs);

        for (const CellShape& shape : JOIN_KEY_CELL_SHAPES) {
            std::string cell;
            cellOf(shape, "a", cell);

            expectJoinedCount(pairs + "WHERE " + cell + " = " + column + " RETURN count(*)", schema._equalPairs);
            expectJoinedCount(pairs + "WHERE " + column + " = " + cell + " RETURN count(*)", schema._equalPairs);
        }
    }
}

TEST_F(SchemaIDCellJoinKeyTest, joinsANullableKeyOnACell) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string call(schema._call);
        const std::string column(schema._column);

        for (const CellShape& shape : JOIN_KEY_CELL_SHAPES) {
            std::string cell;
            cellOf(shape, column, cell);

            std::string nullableCell;
            cellOf(shape, "y", nullableCell);

            const std::string nullableKeys = "UNWIND [1, 2] AS k " + call + " WITH CASE WHEN k = 1 THEN " + column
                                           + " END AS y " + call + " WITH y, " + column + " ";

            expectJoinedCount(nullableKeys + "WHERE y = " + cell + " RETURN count(*)", schema._equalPairs);
            expectJoinedCount(nullableKeys + "WHERE " + nullableCell + " = " + column + " RETURN count(*)",
                              schema._equalPairs);
        }
    }
}
