#include <gtest/gtest.h>

#include <stddef.h>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "ProtoServerTest.h"
#include "ProtoTestUtils.h"
#include "TuringAsyncClient.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

using namespace db;
using namespace turing::test;

namespace {

std::string makeQuery(std::string_view prefix, size_t rowCount) {
    return "UNWIND range(1, " + std::to_string(rowCount) + ") AS x RETURN '" + std::string(prefix) + "' + toString(x) AS name";
}

void appendStrings(const Dataframe* dataframe, std::vector<std::string_view>& strings) {
    const Dataframe::NamedColumns& columns = dataframe->cols();
    ASSERT_EQ(columns.size(), 1u);

    const NamedColumn* column = columns.front();
    const ColumnVector<std::string_view>* plainStrings = column->as<ColumnVector<std::string_view>>();
    const ColumnOptVector<std::string_view>* nullableStrings = column->as<ColumnOptVector<std::string_view>>();

    if (plainStrings != nullptr) {
        strings.insert(strings.end(), plainStrings->begin(), plainStrings->end());
    } else if (nullableStrings != nullptr) {
        for (size_t row = 0; row < nullableStrings->size(); ++row) {
            ASSERT_TRUE(nullableStrings->isValid(row));
            strings.push_back(nullableStrings->getValue(row));
        }
    } else {
        FAIL() << "the name column holds no strings";
    }
}

void expectStrings(std::span<const std::string_view> strings, std::string_view prefix) {
    for (size_t row = 0; row < strings.size(); ++row) {
        const std::string expected = std::string(prefix) + std::to_string(row + 1);
        ASSERT_EQ(strings[row], expected) << "row " << row;
    }
}

}

// The async client keeps the strings of a query valid until the query ends, and returns the
// rows of the next query on the same client
class TuringAsyncClientQueriesTest : public ProtoServerTest {
};

TEST_F(TuringAsyncClientQueriesTest, keepsTheStringsOfAQueryUntilItEnds) {
    net::proto::TuringAsyncClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");
    client.connect();

    std::vector<std::string_view> strings;
    size_t chunkCount = 0;
    ASSERT_NO_FATAL_FAILURE(runAsyncProtoQuery(&client, makeQuery("first", TWO_CHUNK_ROW_COUNT), [&strings, &chunkCount](const Dataframe* dataframe) {
        appendStrings(dataframe, strings);
        expectStrings(strings, "first");
        chunkCount++;
    }));

    client.disconnect();

    EXPECT_GE(chunkCount, 2u);
    EXPECT_EQ(strings.size(), TWO_CHUNK_ROW_COUNT);
}

TEST_F(TuringAsyncClientQueriesTest, returnsTheRowsOfTheNextQueryOnTheSameClient) {
    net::proto::TuringAsyncClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");
    client.connect();

    std::vector<std::string_view> firstStrings;
    ASSERT_NO_FATAL_FAILURE(runAsyncProtoQuery(&client, makeQuery("first", TWO_CHUNK_ROW_COUNT), [&firstStrings](const Dataframe* dataframe) {
        appendStrings(dataframe, firstStrings);
    }));
    EXPECT_EQ(firstStrings.size(), TWO_CHUNK_ROW_COUNT);

    std::vector<std::string_view> secondStrings;
    ASSERT_NO_FATAL_FAILURE(runAsyncProtoQuery(&client, makeQuery("second", 3), [&secondStrings](const Dataframe* dataframe) {
        appendStrings(dataframe, secondStrings);
    }));

    client.disconnect();

    ASSERT_EQ(secondStrings.size(), 3u);
    expectStrings(secondStrings, "second");
}
