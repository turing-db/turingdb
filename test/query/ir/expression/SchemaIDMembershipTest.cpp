#include <gtest/gtest.h>

#include <stddef.h>

#include <array>
#include <string>
#include <string_view>

#include "QueryStatus.h"

#include "IRTestRows.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

// A column of one schema kind, as a procedure yields it. SimpleGraph has 9 labels, 2 edge
// types and 8 property types.
struct SchemaColumn {
    std::string_view _call;
    std::string_view _column;
    std::string_view _type;
    size_t _rows {0};
};

constexpr std::array<SchemaColumn, 4> SCHEMA_COLUMNS {{
    {"CALL db.labels() YIELD id", "id", "Label", 9},
    {"CALL db.edgeTypes() YIELD id", "id", "EdgeType", 2},
    {"CALL db.propertyTypes() YIELD id", "id", "PropertyType", 8},
    {"CALL db.propertyTypes() YIELD valueType", "valueType", "ValueType", 8},
}};

}

class SchemaIDMembershipTest : public WriteQueryTest {
protected:
    void expectRejected(std::string_view query, std::string_view expectedError) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR)
            << "query: " << query << "\nerror: " << status.getError();
        EXPECT_NE(status.getError().find(expectedError), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }
};

TEST_F(SchemaIDMembershipTest, rejectsElementsAnIDCannotEqual) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string search = std::string(schema._call) + " WHERE " + std::string(schema._column) + " IN ";
        const std::string compared = "IN cannot compare '" + std::string(schema._type) + "' with an element of type ";

        expectRejected(search + "[0, 1] RETURN count(*)", compared + "'Integer'");
        expectRejected(search + "['Person', 'String'] RETURN count(*)", compared + "'String'");
        expectRejected(search + "[[0]] RETURN count(*)", compared + "'List'");
        expectRejected(search + "[0, 'Person'] RETURN count(*)", compared + "'Any'");
    }
}

TEST_F(SchemaIDMembershipTest, searchesNullAsNull) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        expectRows(std::string(schema._call) + " RETURN " + std::string(schema._column) + " IN null, count(*)",
                   {{"null", std::to_string(schema._rows)}});
    }
}

TEST_F(SchemaIDMembershipTest, keepsSearchingListsOfOtherValues) {
    expectRows("MATCH (n:Person) WHERE n IN [0, 1] RETURN count(*)", {{"2"}});
    expectRows("RETURN 1.5 IN [1.5, 2.5], 'a' IN [1, 'a'], null IN [1]", {{"true", "true", "null"}});
}
