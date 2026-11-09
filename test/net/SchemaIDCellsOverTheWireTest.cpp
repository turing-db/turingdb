#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "ProtoServerTest.h"
#include "ProtoTestUtils.h"
#include "QueryStatus.h"
#include "TuringClient.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view SCHEMA_CALLS[] = {
    "CALL db.labels() YIELD id AS x ",
    "CALL db.edgeTypes() YIELD id AS x ",
    "CALL db.propertyTypes() YIELD id AS x ",
    "CALL db.propertyTypes() YIELD valueType AS x ",
};

}

// A client reads a schema ID or a value type held in a list or a map as it reads the column
// the value came from: an ID as its number, a value type as its name
class SchemaIDCellsOverTheWireTest : public ProtoServerTest {
};

TEST_F(SchemaIDCellsOverTheWireTest, readsAHeldIDAsItsColumn) {
    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");
    client.connect();

    for (const std::string_view call : SCHEMA_CALLS) {
        const std::string query = std::string(call) + "RETURN x, [x, x], {a: x}";

        ResultRows rows;
        QueryStatus status;
        runProtoQuery(&client, query, nullptr, rows, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        ASSERT_FALSE(rows.empty()) << query;

        for (const std::vector<std::string>& row : rows) {
            ASSERT_EQ(row.size(), 3u) << query;
            EXPECT_EQ(row[1], "[" + row[0] + ", " + row[0] + "]") << query;
            EXPECT_EQ(row[2], "{a: " + row[0] + "}") << query;
        }
    }

    client.disconnect();
}
