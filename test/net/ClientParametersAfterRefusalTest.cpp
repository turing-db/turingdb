#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <string_view>
#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"
#include "ProtoServerTest.h"
#include "ProtoTestUtils.h"
#include "QueryResultFormatter.h"
#include "QueryStatus.h"
#include "TuringAsyncClient.h"
#include "TuringClient.h"
#include "TuringException.h"
#include "dataframe/Dataframe.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view TOO_LARGE_QUERY = "UNWIND $p AS p RETURN count(*) AS c";
constexpr std::string_view SCALAR_QUERY = "RETURN $x AS x";
constexpr std::string_view LIST_QUERY = "UNWIND $ids AS x RETURN x";
constexpr std::string_view NO_PARAMETER_QUERY = "MATCH (n:Founder) RETURN n.name ORDER BY n.name";

void makeScalarParameters(ParameterMap& parameters) {
    ParameterValue x;
    x.setInt64(5);
    parameters.set("x", x);
}

void makeListParameters(ParameterMap& parameters) {
    ParameterValue ids;
    ParameterValue::List& idList = ids.setList();
    idList.resize(3);
    idList[0].setInt64(1);
    idList[1].setInt64(2);
    idList[2].setInt64(3);
    parameters.set("ids", ids);
}

}

// A client that refuses parameters too large to send is left as it was: the next query, with
// or without parameters, is answered on the same client, and after it reconnects
class ClientParametersAfterRefusalTest : public ProtoServerTest {
protected:
    void initialize() override {
        ProtoServerTest::initialize();

        makeTooLargeParameters(_tooLarge);
        makeScalarParameters(_scalar);
        makeListParameters(_list);
    }

    template <typename Client>
    static void refuse(Client& client, const ParameterMap& parameters) {
        try {
            client.sendQuery(std::string(TOO_LARGE_QUERY), [](const Dataframe*) {}, &parameters);
            ADD_FAILURE() << "Parameters larger than the buffer were sent";
        } catch (const TuringException& e) {
            std::string message;
            makeTooLargeParametersMessage(message);
            EXPECT_EQ(std::string_view(e.what()), message);
        }
    }

    static void expectRows(net::proto::TuringClient& client,
                           std::string_view query,
                           const ParameterMap* parameters,
                           const ResultRows& expected) {
        ResultRows rows;
        QueryStatus status;
        ASSERT_NO_THROW(runProtoQuery(&client, std::string(query), parameters, rows, status)) << query;

        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
        EXPECT_EQ(rows, expected) << query;
    }

    void expectAnswers(net::proto::TuringClient& client) {
        expectRows(client, SCALAR_QUERY, &_scalar, {{"5"}});
        expectRows(client, LIST_QUERY, &_list, {{"1"}, {"2"}, {"3"}});
        expectRows(client, NO_PARAMETER_QUERY, nullptr, {{"Adam"}, {"Remy"}});
    }

    ParameterMap _tooLarge;
    ParameterMap _scalar;
    ParameterMap _list;
};

TEST_F(ClientParametersAfterRefusalTest, answersQueriesWithParametersOnTheSameClient) {
    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");
    client.connect();

    expectRows(client, SCALAR_QUERY, &_scalar, {{"5"}});

    refuse(client, _tooLarge);
    ASSERT_TRUE(client.isConnected());

    expectAnswers(client);

    client.disconnect();
}

TEST_F(ClientParametersAfterRefusalTest, answersQueriesWithParametersAfterAReconnect) {
    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");
    client.connect();

    refuse(client, _tooLarge);

    client.disconnect();
    client.connect();

    expectAnswers(client);

    client.disconnect();
}

TEST_F(ClientParametersAfterRefusalTest, answersQueriesWithParametersAfterTwoRefusals) {
    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");
    client.connect();

    refuse(client, _tooLarge);
    expectRows(client, NO_PARAMETER_QUERY, nullptr, {{"Adam"}, {"Remy"}});
    refuse(client, _tooLarge);
    ASSERT_TRUE(client.isConnected());

    expectAnswers(client);

    client.disconnect();
}

TEST_F(ClientParametersAfterRefusalTest, theAsyncClientAnswersQueriesWithParametersAfterARefusal) {
    net::proto::TuringAsyncClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");
    client.connect();

    refuse(client, _tooLarge);
    ASSERT_TRUE(client.isConnected());

    ResultRows rows;
    std::vector<std::string> values;
    ASSERT_NO_THROW(client.sendQuery(std::string(LIST_QUERY), [&rows, &values](const Dataframe* dataframe) {
        QueryResultFormatter::appendRows(rows, values, dataframe);
    }, &_list));

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!client.isRecvComplete() && std::chrono::steady_clock::now() < deadline) {
        client.send();
        client.recv();
    }

    ASSERT_TRUE(client.isRecvComplete());
    const QueryStatus& status = client.getQueryStatus();
    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(rows, (ResultRows {{"1"}, {"2"}, {"3"}}));

    client.disconnect();
}
