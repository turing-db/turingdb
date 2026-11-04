#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <chrono>
#include <optional>
#include <string>
#include <string_view>

#include "ProtoServerTest.h"
#include "QueryStatus.h"
#include "TuringAsyncClient.h"
#include "TuringClient.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

using namespace db;
using namespace turing::test;

namespace {

// Far below the server's packets, which carry up to its 1 MiB buffer each
constexpr size_t SMALL_BUFFER_CAPACITY = 64;

constexpr std::string_view QUERY = "UNWIND range(1, 100000) AS x RETURN x";

struct Total {
    size_t _rowCount {0};
    int64_t _sum {0};
};

void addRows(const Dataframe* dataframe, Total& total) {
    const auto* values = dataframe->cols().front()->as<ColumnVector<std::optional<int64_t>>>();
    ASSERT_NE(values, nullptr);

    for (const std::optional<int64_t>& value : values->getRaw()) {
        ASSERT_TRUE(value.has_value());
        total._rowCount++;
        total._sum += *value;
    }
}

}

// A client's input buffer is only where it starts: a packet larger than the buffer grows it
class TuringClientBufferTest : public ProtoServerTest {
protected:
    void expectEveryRow(const Total& total) {
        EXPECT_EQ(total._rowCount, 100000u);
        EXPECT_EQ(total._sum, int64_t {5000050000});
    }
};

TEST_F(TuringClientBufferTest, growsTheBufferOfTheBlockingClient) {
    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem(), SMALL_BUFFER_CAPACITY);
    client.setGraphName("simpledb");
    client.connect();

    Total total;
    const QueryStatus status = client.sendQuery(std::string(QUERY), [&total](const Dataframe* dataframe) {
        addRows(dataframe, total);
    });
    client.disconnect();

    ASSERT_TRUE(status.isOk()) << status.getError();
    expectEveryRow(total);
}

TEST_F(TuringClientBufferTest, growsTheBufferOfTheAsyncClient) {
    net::proto::TuringAsyncClient client("127.0.0.1", std::to_string(_port), &_env->getMem(), SMALL_BUFFER_CAPACITY);
    client.setGraphName("simpledb");
    client.connect();

    Total total;
    client.sendQuery(std::string(QUERY), [&total](const Dataframe* dataframe) {
        addRows(dataframe, total);
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!client.isRecvComplete() && std::chrono::steady_clock::now() < deadline) {
        client.send();
        client.recv();
    }
    client.disconnect();

    ASSERT_TRUE(client.isRecvComplete());
    ASSERT_TRUE(client.getQueryStatus().isOk()) << client.getQueryStatus().getError();
    expectEveryRow(total);
}
