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

// Far below the messages below, so the server writes each one as several packets
constexpr size_t SMALL_BUFFER_CAPACITY = 16;

constexpr std::string_view TOKEN = "split-error-message";
constexpr std::string_view UNAUTHORIZED = "Unauthorized: missing or invalid authentication token";
constexpr std::string_view UNKNOWN_VARIABLE_QUERY = "RETURN aVariableWhoseNameSpansSeveralPackets";
constexpr std::string_view UNKNOWN_VARIABLE = "Variable 'aVariableWhoseNameSpansSeveralPackets' not found";
constexpr size_t ATTEMPT_COUNT = 3;

}

// The server splits a message longer than its buffer over several ERROR or PROTOCOL_ERROR
// packets, and a client reports the message whole
class SplitErrorMessageTest : public ProtoServerTest {
public:
    SplitErrorMessageTest()
    {
        _serverConfig.setProtoBufferCapacity(SMALL_BUFFER_CAPACITY);
    }

protected:
    void runAsync(net::proto::TuringAsyncClient& client, std::string_view query, std::string& error) {
        try {
            client.sendQuery(std::string(query), [](const Dataframe*) {});

            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
            while (!client.isRecvComplete() && std::chrono::steady_clock::now() < deadline) {
                client.send();
                client.recv();
            }
        } catch (const TuringException& e) {
            error = e.what();
            return;
        }

        error = client.isRecvComplete() ? client.getQueryStatus().getError() : "no answer within 2 s";
    }

    Authenticator _authenticator {std::string(TOKEN)};
};

TEST_F(SplitErrorMessageTest, reportsASplitProtocolErrorOfTheBlockingClient) {
    _env->getDB().setAuthenticator(&_authenticator);

    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");
    client.setAuthToken("wrong-token");

    for (size_t attempt = 0; attempt < ATTEMPT_COUNT; ++attempt) {
        SCOPED_TRACE(::testing::Message() << "attempt=" << attempt);

        client.connect();

        std::string error;
        try {
            client.sendQuery("MATCH (n) RETURN n", [](const Dataframe*) {});
        } catch (const TuringException& e) {
            error = e.what();
        }

        ASSERT_NE(error.find(UNAUTHORIZED), std::string::npos) << error;
    }

    client.disconnect();
}

TEST_F(SplitErrorMessageTest, reportsASplitProtocolErrorOfTheAsyncClient) {
    _env->getDB().setAuthenticator(&_authenticator);

    net::proto::TuringAsyncClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");

    for (size_t attempt = 0; attempt < ATTEMPT_COUNT; ++attempt) {
        SCOPED_TRACE(::testing::Message() << "attempt=" << attempt);

        client.connect();

        std::string error;
        runAsync(client, "MATCH (n) RETURN n", error);
        ASSERT_NE(error.find(UNAUTHORIZED), std::string::npos) << error;
    }

    client.disconnect();
}

TEST_F(SplitErrorMessageTest, reportsASplitQueryErrorOfTheBlockingClient) {
    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");
    client.connect();

    for (size_t attempt = 0; attempt < ATTEMPT_COUNT; ++attempt) {
        SCOPED_TRACE(::testing::Message() << "attempt=" << attempt);

        QueryStatus status;
        ASSERT_NO_THROW(status = client.sendQuery(std::string(UNKNOWN_VARIABLE_QUERY), [](const Dataframe*) {}));

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR);
        ASSERT_NE(status.getError().find(UNKNOWN_VARIABLE), std::string::npos) << status.getError();
    }

    client.disconnect();
}

TEST_F(SplitErrorMessageTest, reportsASplitQueryErrorOfTheAsyncClient) {
    net::proto::TuringAsyncClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");
    client.connect();

    for (size_t attempt = 0; attempt < ATTEMPT_COUNT; ++attempt) {
        SCOPED_TRACE(::testing::Message() << "attempt=" << attempt);

        std::string error;
        runAsync(client, UNKNOWN_VARIABLE_QUERY, error);

        EXPECT_EQ(client.getQueryStatus().getStatus(), QueryStatus::Status::ANALYZE_ERROR);
        ASSERT_NE(error.find(UNKNOWN_VARIABLE), std::string::npos) << error;
    }

    client.disconnect();
}
