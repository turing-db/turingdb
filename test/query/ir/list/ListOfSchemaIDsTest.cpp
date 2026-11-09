#include <gtest/gtest.h>

#include <stddef.h>

#include <array>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "QueryStatus.h"
#include "versioning/ChangeID.h"

#include "IRTestRows.h"
#include "StringRowSink.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

// The values of one schema column of SimpleGraph in ascending order, as ORDER BY sorts the
// column: 9 label IDs, 2 edge type IDs, 8 property type IDs, and the value types of the 8
// property types, ordered as their ordinals are
constexpr std::string_view LABEL_IDS[] = {"0", "1", "2", "3", "4", "5", "6", "7", "8"};
constexpr std::string_view EDGE_TYPE_IDS[] = {"0", "1"};
constexpr std::string_view PROPERTY_TYPE_IDS[] = {"0", "1", "2", "3", "4", "5", "6", "7"};
constexpr std::string_view VALUE_TYPES[] = {"Int64", "Int64", "String", "String", "String", "Bool", "Bool", "Bool"};

struct SchemaKind {
    std::string_view _call;
    std::string_view _type;
    std::string_view _unstorable;
    std::span<const std::string_view> _ascending;
};

constexpr std::array<SchemaKind, 4> SCHEMA_KINDS {{
    {"CALL db.labels() YIELD id AS x ", "Label", "a label ID", LABEL_IDS},
    {"CALL db.edgeTypes() YIELD id AS x ", "EdgeType", "an edge type ID", EDGE_TYPE_IDS},
    {"CALL db.propertyTypes() YIELD id AS x ", "PropertyType", "a property type ID", PROPERTY_TYPE_IDS},
    {"CALL db.propertyTypes() YIELD valueType AS x ", "ValueType", "a value type", VALUE_TYPES},
}};

// Each value of the column once, in ascending order, with the number of rows holding it
void distinctValues(const SchemaKind& kind, std::vector<std::pair<std::string, size_t>>& values) {
    values.clear();

    for (const std::string_view value : kind._ascending) {
        if (!values.empty() && values.back().first == value) {
            values.back().second++;
        } else {
            values.emplace_back(value, 1);
        }
    }
}

size_t equalPairCount(const SchemaKind& kind) {
    std::vector<std::pair<std::string, size_t>> values;
    distinctValues(kind, values);

    size_t pairs = 0;
    for (const auto& [value, rows] : values) {
        pairs += rows * rows;
    }

    return pairs;
}

std::string rowCountOf(const SchemaKind& kind) {
    return std::to_string(kind._ascending.size());
}

}

class ListOfSchemaIDsTest : public WriteQueryTest {
protected:
    void stringRows(std::string_view query, std::vector<StringRowSink::Row>& rows) {
        StringRowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        rows = sink.getRows();
    }

    // The write of @param property fails, and the change it ran in is submitted all the same,
    // so the read that follows would see a value the failed write staged
    void expectWriteRejected(std::string_view query, std::string_view property, std::string_view expectedError) {
        ChangeID changeID;
        openChange(changeID);

        const QueryStatus status = runWrite(query, changeID);
        expectFailure(status, query, expectedError);

        submit(changeID);

        expectRows("MATCH (n) WHERE n." + std::string(property) + " IS NOT NULL RETURN count(*)", {{"0"}});
    }
};

TEST_F(ListOfSchemaIDsTest, holdsAnIDInAListAndAMap) {
    for (const SchemaKind& kind : SCHEMA_KINDS) {
        Rows expected;
        for (const std::string_view value : kind._ascending) {
            const std::string text(value);
            expected.push_back({"[" + text + "]", "{a: " + text + "}", "[[" + text + "], {b: " + text + "}]"});
        }

        expectRows(std::string(kind._call) + "RETURN [x], {a: x}, [[x], {b: x}]", expected);
    }
}

TEST_F(ListOfSchemaIDsTest, rendersAHeldIDAsItsColumnRendersIt) {
    for (const SchemaKind& kind : SCHEMA_KINDS) {
        const std::string query = std::string(kind._call) + "RETURN x, [[x]], {a: x}";

        std::vector<StringRowSink::Row> rows;
        stringRows(query, rows);
        ASSERT_EQ(rows.size(), kind._ascending.size()) << query;

        for (const StringRowSink::Row& row : rows) {
            ASSERT_EQ(row.size(), 3u) << query;
            EXPECT_EQ(row[1], "[" + row[0] + "]") << query;
            EXPECT_EQ(row[2], "{a: " + row[0] + "}") << query;
        }
    }
}

TEST_F(ListOfSchemaIDsTest, collectsIDs) {
    for (const SchemaKind& kind : SCHEMA_KINDS) {
        const std::string call(kind._call);

        std::string collected = "[";
        for (size_t index = 0; index < kind._ascending.size(); index++) {
            collected += index == 0 ? "" : ", ";
            collected += kind._ascending[index];
        }
        collected += "]";

        std::vector<std::pair<std::string, size_t>> values;
        distinctValues(kind, values);

        expectRows(call + "WITH x ORDER BY x RETURN collect(x)", {{collected}});
        expectRows(call + "RETURN size(collect(x)), size(collect(DISTINCT x))",
                   {{rowCountOf(kind), std::to_string(values.size())}});
    }
}

TEST_F(ListOfSchemaIDsTest, readsAHeldIDBackAsAnEqualID) {
    for (const SchemaKind& kind : SCHEMA_KINDS) {
        const std::string call(kind._call);
        const std::string rows = rowCountOf(kind);

        std::vector<std::pair<std::string, size_t>> values;
        distinctValues(kind, values);

        const size_t firstRows = values.front().second;
        const std::string otherRows = std::to_string(kind._ascending.size() - firstRows);

        expectRows(call + "RETURN [x][0] = x, head([x]) = x, last([null, x]) = x, count(*)", {{"true", "true", "true", rows}});
        expectRows(call + "WITH x ORDER BY x WITH collect(x) AS xs " + call + "RETURN xs[0] = x, count(*)",
                   {{"true", std::to_string(firstRows)}, {"false", otherRows}});
        expectRows(call + "WITH x ORDER BY x WITH collect(x) AS xs " + call + "RETURN head(xs) = x, count(*)",
                   {{"true", std::to_string(firstRows)}, {"false", otherRows}});
    }
}

TEST_F(ListOfSchemaIDsTest, searchesAListOfIDs) {
    for (const SchemaKind& kind : SCHEMA_KINDS) {
        const std::string call(kind._call);
        const std::string rows = rowCountOf(kind);

        expectRows(call + "WITH collect(x) AS xs " + call + "RETURN x IN xs, count(*)", {{"true", rows}});
        expectRows(call + "WITH collect(x) AS xs " + call + "RETURN x IN [y IN xs WHERE y <> x], count(*)",
                   {{"false", rows}});
        expectRows(call + "RETURN x IN [x], x IN [x, null], count(*)", {{"true", "true", rows}});
    }
}

TEST_F(ListOfSchemaIDsTest, unwindsAListOfIDs) {
    for (const SchemaKind& kind : SCHEMA_KINDS) {
        const std::string call(kind._call);

        expectRows(call + "WITH collect(x) AS xs UNWIND xs AS y " + call + "WITH x, y WHERE y = x RETURN count(*)",
                   {{std::to_string(equalPairCount(kind))}});
        expectRows(call + "UNWIND [x, x] AS y WITH x, y WHERE y = x RETURN count(*)",
                   {{std::to_string(2 * kind._ascending.size())}});
    }
}

TEST_F(ListOfSchemaIDsTest, holdsANullBesideAnID) {
    for (const SchemaKind& kind : SCHEMA_KINDS) {
        const std::string call(kind._call);
        const std::string rows = rowCountOf(kind);
        const std::string first(kind._ascending.front());

        expectRows(call + "WITH x ORDER BY x LIMIT 1 RETURN [k IN [1, 2] | CASE WHEN k = 1 THEN x END]",
                   {{"[" + first + ", null]"}});
        expectRows("UNWIND [1, 2] AS k " + call
                       + "RETURN k, [i IN [1, 2] | CASE WHEN i = k THEN x END][k - 1] = x, count(*)",
                   {{"1", "true", rows}, {"2", "true", rows}});
        expectRows("UNWIND [1, 2] AS k " + call
                       + "WITH k, CASE WHEN k = 1 THEN x END AS y RETURN k, [y][0] IS NULL, {a: y}.a IS NULL, count(*)",
                   {{"1", "false", "false", rows}, {"2", "true", "true", rows}});
        expectRows("UNWIND [1, 2] AS k " + call + "WITH CASE WHEN k = 1 THEN x END AS y RETURN size(collect(y))",
                   {{rows}});
    }
}

TEST_F(ListOfSchemaIDsTest, appendsAnIDToAList) {
    for (const SchemaKind& kind : SCHEMA_KINDS) {
        expectRows(std::string(kind._call) + "RETURN size([x] + x), size(x + [x]), ([x] + x)[1] = x, count(*)",
                   {{"2", "2", "true", rowCountOf(kind)}});
    }
}

TEST_F(ListOfSchemaIDsTest, deduplicatesAndGroupsListsOfIDs) {
    for (const SchemaKind& kind : SCHEMA_KINDS) {
        const std::string call(kind._call);

        std::vector<std::pair<std::string, size_t>> values;
        distinctValues(kind, values);

        const std::string distinctCount = std::to_string(values.size());

        Rows grouped;
        for (const auto& [value, rows] : values) {
            grouped.push_back({"[" + value + "]", "{a: " + value + "}", std::to_string(2 * rows)});
        }

        expectRows("UNWIND [1, 2] AS k " + call + "WITH DISTINCT [x] AS l RETURN count(*)", {{distinctCount}});
        expectRows("UNWIND [1, 2] AS k " + call + "WITH DISTINCT {a: x} AS m RETURN count(*)", {{distinctCount}});
        expectRows("UNWIND [1, 2] AS k " + call + "RETURN [x] AS l, {a: x} AS m, count(*)", grouped);
    }
}

TEST_F(ListOfSchemaIDsTest, ordersListsOfIDsAsTheIDsSort) {
    for (const SchemaKind& kind : SCHEMA_KINDS) {
        const std::string call(kind._call);

        Rows ascending;
        for (const std::string_view value : kind._ascending) {
            ascending.push_back({"[" + std::string(value) + "]"});
        }

        const Rows descending(ascending.rbegin(), ascending.rend());

        expectRowsInOrder(call + "RETURN [x] AS l ORDER BY l", ascending);
        expectRowsInOrder(call + "RETURN [x] AS l ORDER BY l DESC", descending);
        expectRowsInOrder(call + "RETURN [x] AS l ORDER BY x DESC", descending);
    }
}

TEST_F(ListOfSchemaIDsTest, findsThePositionOfAnID) {
    for (const SchemaKind& kind : SCHEMA_KINDS) {
        const std::string call(kind._call);

        std::vector<std::pair<std::string, size_t>> values;
        distinctValues(kind, values);

        Rows positions;
        size_t position = 0;
        for (const auto& [value, rows] : values) {
            positions.push_back({std::to_string(position), std::to_string(rows)});
            position += rows;
        }

        expectRows(call + "WITH x ORDER BY x WITH collect(x) AS xs " + call + "RETURN coll.indexOf(xs, x), count(*)",
                   positions);
        expectRows(call + "RETURN coll.indexOf([], x), coll.indexOf([null, x], x), count(*)",
                   {{"-1", "1", rowCountOf(kind)}});
    }
}

TEST_F(ListOfSchemaIDsTest, rejectsWhatInRejects) {
    for (const SchemaKind& kind : SCHEMA_KINDS) {
        const std::string call(kind._call);
        const std::string compared = "cannot compare '" + std::string(kind._type) + "' with an element of type ";

        expectError(call + "RETURN x IN [0, 1]", compared + "'Integer'");
        expectError(call + "RETURN coll.indexOf([0, 1], x)", compared + "'Integer'");
        expectError(call + "RETURN coll.indexOf(['a'], x)", compared + "'String'");
    }
}

TEST_F(ListOfSchemaIDsTest, keepsIDsUnordered) {
    for (const SchemaKind& kind : SCHEMA_KINDS) {
        const std::string call(kind._call);
        const std::string type(kind._type);

        expectError(call + "RETURN [x][0] < x",
                    "Operands are not valid or compatible types for an ordering: '" + type + "' and '" + type + "'");
        expectError(call + "RETURN min([x][0])", "Invalid arguments for function 'min'");
        expectError(call + "RETURN max(head([x]))", "Invalid arguments for function 'max'");
        expectRows(call + "RETURN [x] < [x], count(*)", {{"null", rowCountOf(kind)}});
    }
}

// Label 0, edge type 0 and property type 0 are three values, none of them the integer 0
TEST_F(ListOfSchemaIDsTest, tellsIDsOfTwoKindsApart) {
    const std::string firstOfEach = "CALL db.labels() YIELD id AS l WITH l ORDER BY l LIMIT 1 "
                                    "CALL db.edgeTypes() YIELD id AS e WITH l, e ORDER BY e LIMIT 1 "
                                    "CALL db.propertyTypes() YIELD id AS p WITH l, e, p ORDER BY p LIMIT 1 ";

    expectRows(firstOfEach + "RETURN [l] = [l], [l] = [e], [e] = [p], [l] = [0], {a: l} = {a: e}",
               {{"true", "false", "false", "false", "false"}});
    expectRows(firstOfEach + "RETURN size([l, e, p, 0]), [l, e, p, 0][1] IS NULL", {{"4", "false"}});
    expectRows(firstOfEach + "UNWIND [[l], [e], [p], [0], [l]] AS y RETURN count(DISTINCT y)", {{"4"}});
}

TEST_F(ListOfSchemaIDsTest, turnsAwayAPropertyHoldingAnID) {
    size_t written = 0;

    for (const SchemaKind& kind : SCHEMA_KINDS) {
        const std::string call(kind._call);
        const std::string rejection = "A property value cannot hold " + std::string(kind._unstorable);
        const std::string remy = "MATCH (n:Person {name: 'Remy'}) " + call + "WITH n, x LIMIT 1 ";

        for (const std::string_view value : {"[x]", "{a: x}", "[[1], [x]]", "[{a: [x]}]", "{a: {b: x}}"}) {
            const std::string property = "p" + std::to_string(written++);
            expectWriteRejected(remy + "SET n." + property + " = " + std::string(value), property, rejection);
        }

        const std::string property = "p" + std::to_string(written++);
        expectWriteRejected(call + "WITH x LIMIT 1 CREATE (:Person {" + property + ": [x]})", property, rejection);
    }

    expectRows("MATCH (n) RETURN count(*)", {{"18"}});
}
