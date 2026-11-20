#include <gtest/gtest.h>

#include <stddef.h>
#include <string>
#include <vector>

#include "ProtoServerTest.h"
#include "ProtoTestUtils.h"
#include "QueryStatus.h"
#include "TuringClient.h"
#include "TuringValueStore.h"

using namespace db;
using namespace turing::test;

namespace {

const std::vector<std::string> COLUMN_NAMES = {"x", "name", "list", "map"};

std::string makeQuery(size_t rowCount) {
    return "UNWIND range(1, " + std::to_string(rowCount) + ") AS x "
           "RETURN x, 'name' + toString(x) AS name, [x, x + 1] AS list, {x: x} AS map";
}

}

// The template sendQuery decodes into a sink over a value store the caller owns
class TuringClientCallerStoreTest : public ProtoServerTest {
protected:
    void runOnStore(const std::string& query,
                    std::vector<std::string>& columnNames,
                    ResultRows& rows,
                    QueryStatus& status) {
        net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
        client.setGraphName("simpledb");
        net::proto::TuringValueStore values;

        client.connect();
        runProtoQueryOnStore(&client, &_env->getMem(), &values, query, columnNames, rows, status);
        client.disconnect();
    }
};

TEST_F(TuringClientCallerStoreTest, decodesAResultOfSeveralChunks) {
    std::vector<std::string> columnNames;
    ResultRows rows;
    QueryStatus status;
    runOnStore(makeQuery(TWO_CHUNK_ROW_COUNT), columnNames, rows, status);

    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(columnNames, COLUMN_NAMES);
    ASSERT_EQ(rows.size(), TWO_CHUNK_ROW_COUNT);

    for (size_t row = 0; row < TWO_CHUNK_ROW_COUNT; ++row) {
        const std::string x = std::to_string(row + 1);
        const std::string next = std::to_string(row + 2);
        const std::vector<std::string> expected = {x, "name" + x, "[" + x + ", " + next + "]", "{x: " + x + "}"};
        ASSERT_EQ(rows[row], expected) << "row " << row;
    }
}

TEST_F(TuringClientCallerStoreTest, handsOverTheColumnsOfAnEmptyResult) {
    std::vector<std::string> columnNames;
    ResultRows rows;
    QueryStatus status;
    runOnStore(makeQuery(0), columnNames, rows, status);

    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(columnNames, COLUMN_NAMES);
    EXPECT_TRUE(rows.empty());
}
