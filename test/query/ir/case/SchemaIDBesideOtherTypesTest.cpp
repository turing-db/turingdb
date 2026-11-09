#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <array>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iterators/ChunkConfig.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "IRTestRows.h"
#include "StringRowSink.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

// The values of one schema column of SimpleGraph in ascending order: 9 label IDs, 2 edge
// type IDs, 8 property type IDs, and the value types of the 8 property types
constexpr std::string_view LABEL_IDS[] = {"0", "1", "2", "3", "4", "5", "6", "7", "8"};
constexpr std::string_view EDGE_TYPE_IDS[] = {"0", "1"};
constexpr std::string_view PROPERTY_TYPE_IDS[] = {"0", "1", "2", "3", "4", "5", "6", "7"};
constexpr std::string_view VALUE_TYPES[] = {"Int64", "Int64", "String", "String", "String", "Bool", "Bool", "Bool"};

constexpr std::string_view LABEL_NAMES[] = {
    "Person",
    "SoftwareEngineering",
    "Founder",
    "Bioinformatics",
    "Interest",
    "Exotic",
    "Supernatural",
    "SleepDisturber",
    "Sales",
};

struct SchemaColumn {
    std::string_view _yield;
    std::string_view _type;
    std::span<const std::string_view> _ascending;
};

constexpr std::array<SchemaColumn, 4> SCHEMA_COLUMNS {{
    {"CALL db.labels() YIELD id", "Label", LABEL_IDS},
    {"CALL db.edgeTypes() YIELD id", "EdgeType", EDGE_TYPE_IDS},
    {"CALL db.propertyTypes() YIELD id", "PropertyType", PROPERTY_TYPE_IDS},
    {"CALL db.propertyTypes() YIELD valueType", "ValueType", VALUE_TYPES},
}};

// A value of another type, as a query spells it and as a row renders it
struct OtherValue {
    std::string_view _expression;
    std::string_view _rendered;
};

constexpr std::array<OtherValue, 6> OTHER_VALUES {{
    {"0", "0"},
    {"'a'", "a"},
    {"{a: 1}.a", "1"},
    {"[1]", "[1]"},
    {"{b: 1}", "{b: 1}"},
    {"null", "null"},
}};

constexpr std::array<size_t, 3> CHUNK_SIZES {2, 7, ChunkConfig::CHUNK_SIZE};

std::string callAs(const SchemaColumn& schema, std::string_view name) {
    return std::string(schema._yield) + " AS " + std::string(name) + " ";
}

std::string rowCountOf(const SchemaColumn& schema) {
    return std::to_string(schema._ascending.size());
}

bool isNull(const OtherValue& other) {
    return other._expression == "null";
}

// What = answers between the plain ID and a cell holding @param other
std::string equalityWithTheID(const OtherValue& other) {
    return isNull(other) ? "null" : "false";
}

void distinctValuesOf(const SchemaColumn& schema, std::vector<std::string_view>& values) {
    values.assign(schema._ascending.begin(), schema._ascending.end());
    values.erase(std::unique(values.begin(), values.end()), values.end());
}

// One row per value of the column under k = 1 and one under k = 2, holding @param first or
// @param second, or the value itself where that is empty
void keyedRows(const SchemaColumn& schema, std::string_view first, std::string_view second, Rows& rows) {
    rows.clear();

    for (const std::string_view value : schema._ascending) {
        rows.push_back({"1", std::string(first.empty() ? value : first)});
        rows.push_back({"2", std::string(second.empty() ? value : second)});
    }
}

void valueRows(std::span<const std::string_view> values, Rows& rows) {
    for (const std::string_view value : values) {
        rows.push_back({std::string(value)});
    }
}

}

// A CASE, a coalesce, a UNION or a WHEN that fills one column with a label, edge type or
// property type ID, or a value type, beside a value of another type: the column holds each
// row's value as a tagged cell, as it does an integer beside a string
class SchemaIDBesideOtherTypesTest : public WriteQueryTest {
protected:
    void resultRows(std::string_view query, size_t chunkSize, Rows& rows) {
        _interpreter->setChunkSize(chunkSize);

        StringRowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nchunk size: " << chunkSize
                                   << "\nerror: " << status.getError();

        rows = sink.getRows();
    }

    void expectRowsAtEveryChunkSize(std::string_view query, const Rows& expected) {
        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        for (const size_t chunkSize : CHUNK_SIZES) {
            Rows actual;
            resultRows(query, chunkSize, actual);
            std::sort(actual.begin(), actual.end());

            EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\nchunk size: " << chunkSize;
        }
    }

    void expectRowsInOrderAtEveryChunkSize(std::string_view query, const Rows& expected) {
        for (const size_t chunkSize : CHUNK_SIZES) {
            Rows actual;
            resultRows(query, chunkSize, actual);

            EXPECT_EQ(actual, expected) << "query: " << query << "\nchunk size: " << chunkSize;
        }
    }
};

TEST_F(SchemaIDBesideOtherTypesTest, selectsAnIDBesideAValueOfAnotherType) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string call = callAs(schema, "v");
        const std::string over = "UNWIND [1, 2] AS k " + call;
        const std::string rows = rowCountOf(schema);

        for (const OtherValue& other : OTHER_VALUES) {
            const std::string value(other._expression);

            Rows expected;
            keyedRows(schema, "", other._rendered, expected);

            const std::array<std::string, 4> selections {
                "CASE WHEN k = 1 THEN v ELSE " + value + " END",
                "CASE WHEN k = 2 THEN " + value + " ELSE v END",
                "CASE k WHEN 1 THEN v WHEN 2 THEN " + value + " END",
                "CASE k WHEN 2 THEN " + value + " ELSE v END",
            };

            for (const std::string& selection : selections) {
                expectRowsAtEveryChunkSize(over + "RETURN k, " + selection, expected);
                expectRowsAtEveryChunkSize(over + "WITH k, v, " + selection + " AS x RETURN k, x = v, v = x, count(*)",
                                           {{"1", "true", "true", rows},
                                            {"2", equalityWithTheID(other), equalityWithTheID(other), rows}});
                expectRowsAtEveryChunkSize("UNWIND [1, 2] AS k CALL (k) { " + call + "RETURN " + selection + " AS x } "
                                           "RETURN k, x",
                                           expected);
            }
        }
    }
}

TEST_F(SchemaIDBesideOtherTypesTest, coalescesAnIDBesideAValueOfAnotherType) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string call = callAs(schema, "v");
        const std::string over = "UNWIND [1, 2] AS k " + call;

        for (const OtherValue& other : OTHER_VALUES) {
            const std::string value(other._expression);

            const std::string_view otherOrID = isNull(other) ? "" : other._rendered;

            Rows everyID;
            keyedRows(schema, "", "", everyID);

            Rows everyOtherOrID;
            keyedRows(schema, otherOrID, otherOrID, everyOtherOrID);

            Rows idThenOther;
            keyedRows(schema, "", other._rendered, idThenOther);

            Rows otherThenID;
            keyedRows(schema, "", otherOrID, otherThenID);

            expectRowsAtEveryChunkSize(over + "RETURN k, coalesce(v, " + value + ")", everyID);
            expectRowsAtEveryChunkSize(over + "RETURN k, coalesce(" + value + ", v)", everyOtherOrID);
            expectRowsAtEveryChunkSize(over + "RETURN k, coalesce(CASE WHEN k = 1 THEN v END, " + value + ")",
                                       idThenOther);
            expectRowsAtEveryChunkSize(over + "RETURN k, coalesce(CASE WHEN k = 2 THEN " + value + " END, v)",
                                       otherThenID);
            expectRowsAtEveryChunkSize("UNWIND [1, 2] AS k CALL (k) { " + call + "RETURN coalesce(CASE WHEN k = 1 THEN v END, "
                                           + value + ") AS x } RETURN k, x",
                                       idThenOther);
        }
    }
}

TEST_F(SchemaIDBesideOtherTypesTest, unitesAnIDWithAValueOfAnotherType) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string yielded = callAs(schema, "x") + "RETURN x";

        std::vector<std::string_view> distinctValues;
        distinctValuesOf(schema, distinctValues);

        for (const OtherValue& other : OTHER_VALUES) {
            const std::string returned = "RETURN " + std::string(other._expression) + " AS x";
            const Row otherRow {std::string(other._rendered)};

            Rows everyValue;
            valueRows(schema._ascending, everyValue);
            everyValue.push_back(otherRow);

            Rows eachValue;
            valueRows(distinctValues, eachValue);
            eachValue.push_back(otherRow);

            Rows everyValueTwice;
            for (const Row& row : everyValue) {
                everyValueTwice.push_back({"1", row[0]});
                everyValueTwice.push_back({"2", row[0]});
            }

            expectRowsAtEveryChunkSize(yielded + " UNION ALL " + returned, everyValue);
            expectRowsAtEveryChunkSize(returned + " UNION ALL " + yielded, everyValue);
            expectRowsAtEveryChunkSize(yielded + " UNION " + returned, eachValue);
            expectRowsAtEveryChunkSize(returned + " UNION " + yielded, eachValue);
            expectRowsAtEveryChunkSize("CALL { " + yielded + " UNION ALL " + returned + " } RETURN x", everyValue);
            expectRowsAtEveryChunkSize("CALL { " + returned + " UNION " + yielded + " } RETURN x", eachValue);
            expectRowsAtEveryChunkSize("UNWIND [1, 2] AS k CALL { " + yielded + " UNION ALL " + returned + " } RETURN k, x",
                                       everyValueTwice);
        }
    }
}

TEST_F(SchemaIDBesideOtherTypesTest, unitesALabelIDWithItsName) {
    Rows expected;
    valueRows(LABEL_IDS, expected);
    valueRows(LABEL_NAMES, expected);

    expectRowsAtEveryChunkSize("CALL db.labels() YIELD id AS x RETURN x "
                               "UNION ALL "
                               "CALL db.labels() YIELD label AS x RETURN x",
                               expected);
    expectRowsAtEveryChunkSize("CALL db.labels() YIELD label AS x RETURN x "
                               "UNION "
                               "CALL db.labels() YIELD id AS x RETURN x",
                               expected);
}

TEST_F(SchemaIDBesideOtherTypesTest, selectsAWhenBranchOfAnotherType) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string yielded = callAs(schema, "x") + "RETURN x";

        Rows everyValue;
        valueRows(schema._ascending, everyValue);

        for (const OtherValue& other : OTHER_VALUES) {
            const std::string returned = "RETURN " + std::string(other._expression) + " AS x";
            const Rows otherRows {{std::string(other._rendered)}};

            Rows perRow;
            for (const Row& row : everyValue) {
                perRow.push_back({"1", row[0]});
            }
            perRow.push_back({"2", std::string(other._rendered)});

            expectRowsAtEveryChunkSize("WHEN 1 = 1 THEN " + yielded + " ELSE " + returned, everyValue);
            expectRowsAtEveryChunkSize("WHEN 1 = 2 THEN " + yielded + " ELSE " + returned, otherRows);
            expectRowsAtEveryChunkSize("WHEN 1 = 1 THEN " + returned + " ELSE " + yielded, otherRows);
            expectRowsAtEveryChunkSize("WHEN 1 = 2 THEN " + returned + " ELSE " + yielded, everyValue);
            expectRowsAtEveryChunkSize("UNWIND [1, 2] AS k CALL (k) { WHEN k = 1 THEN " + yielded + " ELSE " + returned
                                           + " } RETURN k, x",
                                       perRow);
            expectRowsAtEveryChunkSize("UNWIND [1, 2] AS k CALL (k) { WHEN k = 2 THEN " + returned + " ELSE " + yielded
                                           + " } RETURN k, x",
                                       perRow);
        }
    }
}

TEST_F(SchemaIDBesideOtherTypesTest, mixesIDsOfTwoKinds) {
    for (const SchemaColumn& first : SCHEMA_COLUMNS) {
        for (const SchemaColumn& second : SCHEMA_COLUMNS) {
            if (first._type == second._type) {
                continue;
            }

            const std::string pairs = callAs(first, "a") + callAs(second, "b") + "UNWIND [1, 2] AS k ";
            const std::string pairCount = std::to_string(first._ascending.size() * second._ascending.size());

            expectRowsAtEveryChunkSize(pairs + "WITH k, a, b, CASE WHEN k = 1 THEN a ELSE b END AS x "
                                               "RETURN k, x = a, x = b, count(*)",
                                       {{"1", "true", "false", pairCount}, {"2", "false", "true", pairCount}});
            expectRowsAtEveryChunkSize(pairs + "WITH k, a, b, coalesce(CASE WHEN k = 1 THEN a END, b) AS x "
                                               "RETURN k, x = a, x = b, count(*)",
                                       {{"1", "true", "false", pairCount}, {"2", "false", "true", pairCount}});

            std::vector<std::string_view> firstDistinct;
            distinctValuesOf(first, firstDistinct);

            std::vector<std::string_view> secondDistinct;
            distinctValuesOf(second, secondDistinct);

            Rows everyValue;
            valueRows(first._ascending, everyValue);
            valueRows(second._ascending, everyValue);

            Rows eachValue;
            valueRows(firstDistinct, eachValue);
            valueRows(secondDistinct, eachValue);

            Rows perRow;
            for (const std::string_view value : first._ascending) {
                perRow.push_back({"1", std::string(value)});
            }
            for (const std::string_view value : second._ascending) {
                perRow.push_back({"2", std::string(value)});
            }

            const std::string firstYielded = callAs(first, "x") + "RETURN x";
            const std::string secondYielded = callAs(second, "x") + "RETURN x";

            expectRowsAtEveryChunkSize(firstYielded + " UNION ALL " + secondYielded, everyValue);
            expectRowsAtEveryChunkSize(firstYielded + " UNION " + secondYielded, eachValue);
            expectRowsAtEveryChunkSize("UNWIND [1, 2] AS k CALL (k) { WHEN k = 1 THEN " + firstYielded + " ELSE "
                                           + secondYielded + " } RETURN k, x",
                                       perRow);
        }
    }
}

TEST_F(SchemaIDBesideOtherTypesTest, keepsAnIDInACellThroughDistinctGroupingAndOrder) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string call = callAs(schema, "v");
        const std::string mixed = "UNWIND [1, 2] AS k " + call + "WITH v, CASE WHEN k = 0 THEN 0 ELSE v END AS x ";
        const std::string bothRows = std::to_string(2 * schema._ascending.size());

        std::vector<std::string_view> distinctValues;
        distinctValuesOf(schema, distinctValues);

        Rows eachValue;
        valueRows(distinctValues, eachValue);

        Rows grouped;
        Rows groupedBesideAnInteger {{"0", "1"}};
        for (const std::string_view value : distinctValues) {
            const size_t rows = static_cast<size_t>(std::ranges::count(schema._ascending, value));
            grouped.push_back({std::string(value), std::to_string(2 * rows)});
            groupedBesideAnInteger.push_back({std::string(value), std::to_string(2 * rows)});
        }

        Rows ascendingTwice;
        Rows ascendingThenNulls;
        for (const std::string_view value : schema._ascending) {
            ascendingTwice.push_back({std::string(value)});
            ascendingTwice.push_back({std::string(value)});
            ascendingThenNulls.push_back({std::string(value)});
        }

        for (size_t row = 0; row < schema._ascending.size(); row++) {
            ascendingThenNulls.push_back({"null"});
        }

        Rows descendingTwice(ascendingTwice.rbegin(), ascendingTwice.rend());

        expectRowsAtEveryChunkSize(mixed + "RETURN DISTINCT x", eachValue);
        expectRowsAtEveryChunkSize(mixed + "RETURN count(DISTINCT x)", {{std::to_string(distinctValues.size())}});
        expectRowsAtEveryChunkSize(mixed + "RETURN x, count(*)", grouped);
        expectRowsAtEveryChunkSize(mixed + "RETURN x = v, count(*)", {{"true", bothRows}});
        expectRowsInOrderAtEveryChunkSize(mixed + "RETURN x ORDER BY x", ascendingTwice);
        expectRowsInOrderAtEveryChunkSize(mixed + "RETURN x ORDER BY x DESC", descendingTwice);
        expectRowsInOrderAtEveryChunkSize("UNWIND [1, 2] AS k " + call + "WITH CASE WHEN k = 1 THEN v ELSE {a: null}.a END AS x "
                                          "RETURN x ORDER BY x",
                                          ascendingThenNulls);

        const std::string yielded = call + "RETURN v AS x";
        expectRowsAtEveryChunkSize("CALL { " + yielded + " UNION ALL " + yielded + " UNION ALL RETURN 0 AS x } "
                                   "RETURN x, count(*)",
                                   groupedBesideAnInteger);
    }
}

TEST_F(SchemaIDBesideOtherTypesTest, rejectsWritingAnIDThroughACell) {
    size_t written = 0;

    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string remy = "MATCH (n:Person {name: 'Remy'}) " + callAs(schema, "v") + "WITH n, v LIMIT 1 ";
        const std::string type(schema._type);
        const std::string unsupported = "Cannot evaluate property: unsupported type '" + type + "'";

        expectWriteError(remy + "SET n.p" + std::to_string(written++) + " = CASE WHEN n.name = 'Remy' THEN v ELSE 0 END",
                         unsupported);
        expectWriteError(remy + "SET n.p" + std::to_string(written++) + " = coalesce(v, 'a')", unsupported);
        expectWriteError(remy + "SET n.age = CASE WHEN n.name = 'Remy' THEN v ELSE 0 END",
                         "Cannot set a property of type 'Int64' to a value of type '" + type + "'");
    }
}

TEST_F(SchemaIDBesideOtherTypesTest, keepsRejectingAComparisonOfKnownTypes) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string call = callAs(schema, "v");
        const std::string type(schema._type);
        const std::string compared = "Operands are not valid or compatible types: '" + type + "' and ";
        const std::string ordered = "Operands are not valid or compatible types for an ordering: '" + type + "' and ";

        expectError(call + "RETURN CASE v WHEN 0 THEN 'zero' END", compared + "'Integer'");
        expectError(call + "RETURN CASE v WHEN 'Person' THEN 1 END", compared + "'String'");
        expectError(call + "RETURN v = 0", compared + "'Integer'");
        expectError(call + "RETURN v <> 'Person'", compared + "'String'");
        expectError(call + "RETURN v < v", ordered + "'" + type + "'");
        expectError(call + "RETURN min(v)", "Invalid arguments for function 'min'");
        expectError(call + "RETURN max(v)", "Invalid arguments for function 'max'");
    }
}
