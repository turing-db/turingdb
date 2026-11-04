#include <gtest/gtest.h>

#include <stddef.h>
#include <string>
#include <string_view>

#include "ParameterMap.h"
#include "ProtoServerTest.h"
#include "QueryStatus.h"
#include "TuringClient.h"
#include "TuringException.h"
#include "TuringProtoHeaders.h"
#include "dataframe/Dataframe.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view QUERY = "MATCH (n) RETURN n";
constexpr size_t SIMPLEDB_NODE_COUNT = 18;

}

// A request the client cannot build fails before a byte is sent, so the connection stays
// open and answers the next query
class UnsentRequestKeepsConnectionTest : public ProtoServerTest {
};

TEST_F(UnsentRequestKeepsConnectionTest, answersAfterParametersTooLargeToSend) {
    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");
    client.connect();

    ParameterMap parameters;
    ParameterValue text;
    text.setString(std::string(net::proto::DEFAULT_BUFFER_CAPACITY + 1, 'x'));
    parameters.set("text", text);

    EXPECT_THROW(client.sendQuery("RETURN $text", [](const Dataframe*) {}, &parameters), TuringException);
    ASSERT_TRUE(client.isConnected());

    size_t rowCount = 0;
    QueryStatus status;
    ASSERT_NO_THROW(status = client.sendQuery(std::string(QUERY), [&rowCount](const Dataframe* dataframe) {
        rowCount += dataframe->getLogicalRowCount();
    }));

    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(rowCount, SIMPLEDB_NODE_COUNT);

    client.disconnect();
}
