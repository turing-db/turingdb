#include <gtest/gtest.h>

#include <stddef.h>
#include <string>
#include <string_view>
#include <vector>

#include "ProtoServerTest.h"
#include "ProtoTestUtils.h"
#include "QueryStatus.h"
#include "QueryResultFormatter.h"
#include "TruncatingProxy.h"
#include "TuringAsyncClient.h"
#include "TuringClient.h"
#include "TuringException.h"
#include "TuringProtoHeaders.h"
#include "TuringValueStore.h"
#include "dataframe/Dataframe.h"

using namespace db;
using namespace turing::test;

namespace {

// Far below the lists below, so the server writes each one over several CHUNK packets
constexpr size_t SMALL_BUFFER_CAPACITY = 256;

// Past the first CHUNK packets of the list, well before its end
constexpr size_t TRUNCATED_RESPONSE_SIZE = 2048;

constexpr std::string_view TRUNCATED_RESPONSE_ERROR = "Connection closed mid-chunk";

constexpr std::string_view LIST_QUERY = "RETURN range(1, 1000) AS list";
constexpr std::string_view MAP_QUERY = "RETURN {values: range(1, 1000)} AS map";
constexpr std::string_view NEXT_QUERY = "UNWIND range(1, 3) AS x RETURN x, [x, x * 10] AS list, {x: x} AS map";

const ResultRows NEXT_ROWS = {
    {"1", "[1, 10]", "{x: 1}"},
    {"2", "[2, 20]", "{x: 2}"},
    {"3", "[3, 30]", "{x: 3}"},
};

}

// A query that throws while a list or map row is still open leaves no container behind for
// the next query on the same value store
class ValueStoreAfterThrownQueryTest : public ProtoServerTest {
public:
    ValueStoreAfterThrownQueryTest()
    {
        _serverConfig.setProtoBufferCapacity(SMALL_BUFFER_CAPACITY);
    }

protected:
    void initialize() override {
        ProtoServerTest::initialize();
        ASSERT_NO_FATAL_FAILURE(_proxy.start(_port, TRUNCATED_RESPONSE_SIZE));
    }

    void terminate() override {
        _proxy.stop();
        ProtoServerTest::terminate();
    }

    void expectCutInsideValue() {
        TruncatedPacket packet;
        _proxy.findTruncatedPacket(packet);

        ASSERT_TRUE(packet._found);
        EXPECT_EQ(packet._type, net::proto::MessageTypes::CHUNK);
        EXPECT_GE(packet._chunkPacketsBefore, 1u);
    }

    void expectNextQueryOnStore(std::string_view thrownQuery) {
        net::proto::TuringClient client("127.0.0.1", std::to_string(_proxy.getPort()), &_env->getMem());
        client.setGraphName("simpledb");
        net::proto::TuringValueStore values;

        client.connect();
        std::string error;
        try {
            std::vector<std::string> columnNames;
            ResultRows rows;
            QueryStatus status;
            runProtoQueryOnStore(&client, &_env->getMem(), &values, std::string(thrownQuery), columnNames, rows, status);
        } catch (const TuringException& exception) {
            error = exception.what();
        }
        ASSERT_EQ(error, TRUNCATED_RESPONSE_ERROR);

        client.connect();
        std::vector<std::string> columnNames;
        ResultRows rows;
        QueryStatus status;
        runProtoQueryOnStore(&client, &_env->getMem(), &values, std::string(NEXT_QUERY), columnNames, rows, status);
        client.disconnect();

        ASSERT_TRUE(status.isOk()) << status.getError();
        EXPECT_EQ(rows, NEXT_ROWS);
        expectCutInsideValue();
    }

    void expectNextQueryOnClient(std::string_view thrownQuery) {
        net::proto::TuringClient client("127.0.0.1", std::to_string(_proxy.getPort()), &_env->getMem());
        client.setGraphName("simpledb");

        client.connect();
        std::string error;
        try {
            ResultRows rows;
            QueryStatus status;
            runProtoQuery(&client, std::string(thrownQuery), nullptr, rows, status);
        } catch (const TuringException& exception) {
            error = exception.what();
        }
        ASSERT_EQ(error, TRUNCATED_RESPONSE_ERROR);

        client.connect();
        ResultRows rows;
        QueryStatus status;
        runProtoQuery(&client, std::string(NEXT_QUERY), nullptr, rows, status);
        client.disconnect();

        ASSERT_TRUE(status.isOk()) << status.getError();
        EXPECT_EQ(rows, NEXT_ROWS);
        expectCutInsideValue();
    }

    void expectNextQueryOnAsyncClient(std::string_view thrownQuery) {
        net::proto::TuringAsyncClient client("127.0.0.1", std::to_string(_proxy.getPort()), &_env->getMem());
        client.setGraphName("simpledb");

        client.connect();
        std::string error;
        try {
            runAsyncProtoQuery(&client, std::string(thrownQuery), [](const Dataframe* dataframe) {});
        } catch (const TuringException& exception) {
            error = exception.what();
        }
        ASSERT_EQ(error, TRUNCATED_RESPONSE_ERROR);

        client.connect();
        ResultRows rows;
        std::vector<std::string> values;
        ASSERT_NO_FATAL_FAILURE(runAsyncProtoQuery(&client, std::string(NEXT_QUERY), [&rows, &values](const Dataframe* dataframe) {
            QueryResultFormatter::appendRows(rows, values, dataframe);
        }));
        client.disconnect();

        EXPECT_EQ(rows, NEXT_ROWS);
        expectCutInsideValue();
    }

    TruncatingProxy _proxy;
};

TEST_F(ValueStoreAfterThrownQueryTest, aCallerStoreAfterAnOpenList) {
    expectNextQueryOnStore(LIST_QUERY);
}

TEST_F(ValueStoreAfterThrownQueryTest, aCallerStoreAfterAnOpenMap) {
    expectNextQueryOnStore(MAP_QUERY);
}

TEST_F(ValueStoreAfterThrownQueryTest, theClientStoreAfterAnOpenList) {
    expectNextQueryOnClient(LIST_QUERY);
}

TEST_F(ValueStoreAfterThrownQueryTest, theClientStoreAfterAnOpenMap) {
    expectNextQueryOnClient(MAP_QUERY);
}

TEST_F(ValueStoreAfterThrownQueryTest, theAsyncClientAfterAnOpenList) {
    expectNextQueryOnAsyncClient(LIST_QUERY);
}

TEST_F(ValueStoreAfterThrownQueryTest, theAsyncClientAfterAnOpenMap) {
    expectNextQueryOnAsyncClient(MAP_QUERY);
}
