#include <gtest/gtest.h>

#include <stddef.h>
#include <chrono>
#include <string>
#include <string_view>

#include "Authenticator.h"
#include "ProtoServerTest.h"
#include "QueryStatus.h"
#include "TuringAsyncClient.h"
#include "TuringClient.h"
#include "TuringDB.h"
#include "TuringException.h"
#include "dataframe/Dataframe.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view TOKEN = "client-after-protocol-error";
constexpr std::string_view QUERY = "MATCH (n) RETURN n";
constexpr std::string_view UNAUTHORIZED = "Unauthorized: missing or invalid authentication token";
constexpr std::string_view NOT_CONNECTED = "The client is not connected";
constexpr size_t SIMPLEDB_NODE_COUNT = 18;
constexpr size_t ATTEMPT_COUNT = 5;

}

// The server closes the connection after a protocol error, so a client reports the error,
// closes its end too, and answers again once it reconnects
class ClientAfterProtocolErrorTest : public ProtoServerTest {
protected:
    void initialize() override {
        ProtoServerTest::initialize();
        _env->getDB().setAuthenticator(&_authenticator);
    }

    void asyncQueryError(net::proto::TuringAsyncClient& client, std::string& error) {
        try {
            client.sendQuery(std::string(QUERY), [](const Dataframe*) {});

            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
            while (!client.isRecvComplete() && std::chrono::steady_clock::now() < deadline) {
                client.send();
                client.recv();
            }
        } catch (const TuringException& e) {
            error = e.what();
            return;
        }

        error = client.isRecvComplete() ? "no error" : "no answer within 2 s";
    }

    Authenticator _authenticator {std::string(TOKEN)};
};

TEST_F(ClientAfterProtocolErrorTest, reconnectsTheBlockingClientAfterAProtocolError) {
    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");
    client.connect();

    for (size_t attempt = 0; attempt < ATTEMPT_COUNT; ++attempt) {
        SCOPED_TRACE(::testing::Message() << "attempt=" << attempt);

        client.setAuthToken("wrong-token");
        try {
            client.sendQuery(std::string(QUERY), [](const Dataframe*) {});
            FAIL() << "the query with the wrong token was answered";
        } catch (const TuringException& e) {
            EXPECT_NE(std::string_view(e.what()).find(UNAUTHORIZED), std::string_view::npos) << e.what();
        }
        ASSERT_FALSE(client.isConnected());

        try {
            client.sendQuery(std::string(QUERY), [](const Dataframe*) {});
            FAIL() << "a closed client sent a query";
        } catch (const TuringException& e) {
            EXPECT_NE(std::string_view(e.what()).find(NOT_CONNECTED), std::string_view::npos) << e.what();
        }

        client.connect();
        client.setAuthToken(std::string(TOKEN));
        size_t rowCount = 0;
        QueryStatus status;
        ASSERT_NO_THROW(status = client.sendQuery(std::string(QUERY), [&rowCount](const Dataframe* dataframe) {
            rowCount += dataframe->getLogicalRowCount();
        }));

        ASSERT_TRUE(status.isOk()) << status.getError();
        EXPECT_EQ(rowCount, SIMPLEDB_NODE_COUNT);
    }

    client.disconnect();
}

TEST_F(ClientAfterProtocolErrorTest, closesTheAsyncClientAfterAProtocolError) {
    net::proto::TuringAsyncClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");

    for (size_t attempt = 0; attempt < ATTEMPT_COUNT; ++attempt) {
        SCOPED_TRACE(::testing::Message() << "attempt=" << attempt);

        client.connect();

        std::string error;
        asyncQueryError(client, error);
        ASSERT_NE(error.find(UNAUTHORIZED), std::string::npos) << error;
        ASSERT_FALSE(client.isConnected());

        asyncQueryError(client, error);
        EXPECT_NE(error.find(NOT_CONNECTED), std::string::npos) << error;
    }
}
