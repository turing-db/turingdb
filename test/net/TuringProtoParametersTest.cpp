#include <gtest/gtest.h>

#include <stdint.h>
#include <string.h>
#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "DBServerConfig.h"
#include "Graph.h"
#include "NetBuffer.h"
#include "ParameterMap.h"
#include "ParameterValue.h"
#include "QueryResultFormatter.h"
#include "QueryStatus.h"
#include "RemoteTestUtils.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringAsyncClient.h"
#include "TuringClient.h"
#include "TuringException.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoNestedWriter.h"
#include "TuringProtoOutBuf.h"
#include "TuringServer.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"
#include "dataframe/Dataframe.h"
#include "map/MapBuffer.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<std::vector<std::string>>;

void setString(ParameterMap& parameters, std::string_view name, std::string_view text) {
    ParameterValue value;
    value.setString(text);
    parameters.set(name, value);
}

void appendLength(std::string& body, size_t length) {
    const net::proto::WireSize wireLength = static_cast<net::proto::WireSize>(length);
    body.append(reinterpret_cast<const char*>(&wireLength), sizeof(wireLength));
}

void makeEnvelope(std::string_view query, std::string_view parameterBytes, std::string& body) {
    appendLength(body, query.size());
    body += query;
    appendLength(body, parameterBytes.size());
    body += parameterBytes;
}

}

class TuringProtoParametersTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        Graph* graph = nullptr;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            system.createGraph("default");
            graph = system.createGraph("simpledb");
        }

        SimpleGraph::createSimpleGraph(graph);

        _port = reserveFreePort();

        _serverConfig.setAddress("127.0.0.1");
        _serverConfig.setPort(_port);
        _serverConfig.setWorkerCount(1);
        _serverConfig.setMaxConnections(16);

        _server = std::make_unique<TuringServer>(_serverConfig, _env->getDB());
        _server->start();

        ASSERT_TRUE(waitUntilListening(_port, std::chrono::milliseconds(2000)));
    }

    void terminate() override {
        if (_server) {
            _server->stop();
            _server->wait();
        }
    }

    void runQuery(const std::string& query, const ParameterMap* parameters, Rows& rows, QueryStatus& status) {
        net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
        client.setGraphName("simpledb");
        client.connect();

        std::vector<std::string> values;
        status = client.sendQuery(query, [&rows, &values](const Dataframe* dataframe) {
            QueryResultFormatter::appendRows(rows, values, dataframe);
        }, parameters);

        client.disconnect();
    }

    void expectRows(const std::string& query, const ParameterMap* parameters, const Rows& expected) {
        Rows rows;
        QueryStatus status;
        runQuery(query, parameters, rows, status);

        ASSERT_TRUE(status.isOk()) << status.getError();
        EXPECT_EQ(rows, expected) << query;
    }

    void postEnvelope(const std::string& body, net::proto::MessageTypes expectedType, std::string_view expectedMessage) {
        HttpResponse response;
        postHttpBody(_port, "/query?graph=simpledb", body, response);

        ASSERT_GE(response.body.size(), net::proto::ProtoHeader::wireSize());
        const net::proto::ProtoHeader header = net::proto::ProtoHeader::decode(response.body.data(),
                                                                              response.body.size());
        EXPECT_EQ(header._type, expectedType);
        EXPECT_NE(response.body.find(expectedMessage), std::string::npos) << response.body;
    }

    ProtoEnvScope _protoScope;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<TuringServer> _server;
    DBServerConfig _serverConfig;
    uint16_t _port {0};
};

TEST_F(TuringProtoParametersTest, aStringParameterNamesTheLabel) {
    ParameterMap parameters;
    setString(parameters, "label", "Founder");

    expectRows("MATCH (n:$label) RETURN n.name ORDER BY n.name", &parameters, {{"Adam"}, {"Remy"}});
}

TEST_F(TuringProtoParametersTest, aListParameterNamesEveryLabel) {
    ParameterValue labels;
    ParameterValue::List& list = labels.setList();
    list.resize(2);
    list[0].setString("Interest");
    list[1].setString("Exotic");

    ParameterMap parameters;
    parameters.set("labels", labels);

    expectRows("MATCH (n:$labels) RETURN n.name ORDER BY n.name", &parameters, {{"Eighties"}, {"Ghosts"}});
}

TEST_F(TuringProtoParametersTest, aParameterNamesTheEdgeType) {
    ParameterMap parameters;
    setString(parameters, "type", "KNOWS_WELL");

    expectRows("MATCH ()-[e:$type]->() RETURN count(e)", &parameters, {{"3"}});
}

TEST_F(TuringProtoParametersTest, noParametersAndAnEmptyMapRunTheQuery) {
    expectRows("MATCH (n) RETURN count(n)", nullptr, {{"18"}});

    const ParameterMap parameters;
    expectRows("MATCH (n) RETURN count(n)", &parameters, {{"18"}});
}

TEST_F(TuringProtoParametersTest, aMissingParameterIsReported) {
    Rows rows;
    QueryStatus status;
    runQuery("MATCH (n:$label) RETURN n", nullptr, rows, status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::EXEC_ERROR);
    EXPECT_EQ(status.getError(), "Parameter $label is not defined");
}

TEST_F(TuringProtoParametersTest, theAsyncClientSendsParameters) {
    ParameterMap parameters;
    setString(parameters, "label", "Founder");

    net::proto::TuringAsyncClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("simpledb");
    client.connect();

    Rows rows;
    std::vector<std::string> values;
    client.sendQuery("MATCH (n:$label) RETURN n.name ORDER BY n.name", [&rows, &values](const Dataframe* dataframe) {
        QueryResultFormatter::appendRows(rows, values, dataframe);
    }, &parameters);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!client.isRecvComplete() && std::chrono::steady_clock::now() < deadline) {
        client.send();
        client.recv();
    }

    ASSERT_TRUE(client.isRecvComplete());
    ASSERT_TRUE(client.getQueryStatus().isOk()) << client.getQueryStatus().getError();
    EXPECT_EQ(rows, (Rows {{"Adam"}, {"Remy"}}));

    client.disconnect();
}

TEST_F(TuringProtoParametersTest, aMalformedEnvelopeIsAProtocolError) {
    using net::proto::MessageTypes;

    postEnvelope("MA", MessageTypes::PROTOCOL_ERROR, "The request body ends before the query length");
    postEnvelope("MATCH (n) RETURN n", MessageTypes::PROTOCOL_ERROR, "The request body ends before the end of the query");

    std::string noParameterLength;
    appendLength(noParameterLength, 8);
    noParameterLength += "RETURN 1";
    postEnvelope(noParameterLength, MessageTypes::PROTOCOL_ERROR, "The request body ends before the parameter map length");

    std::string wrongParameterLength;
    makeEnvelope("RETURN 1", "", wrongParameterLength);
    wrongParameterLength += "xyz";
    postEnvelope(wrongParameterLength, MessageTypes::PROTOCOL_ERROR, "The parameter map length does not match the rest of the request body");

    std::string truncatedMap;
    makeEnvelope("RETURN 1", std::string(6, '\0'), truncatedMap);
    postEnvelope(truncatedMap, MessageTypes::PROTOCOL_ERROR, "The parameter map ends before its last value");
}

TEST_F(TuringProtoParametersTest, aTypeAParameterCannotHoldIsAQueryError) {
    MapBuffer<> mapBuffer;
    const std::vector<MapBuffer<>::MapKeyValuePair> entries = {{"count", types::UInt64::Primitive {7}}};

    net::proto::TuringProtoOutBuf parameterBuffer(net::proto::DEFAULT_BUFFER_CAPACITY);
    net::proto::NestedContainerWriter writer(&parameterBuffer);
    writer.writeMapView(mapBuffer.insert(entries));

    std::string body;
    makeEnvelope("RETURN 1", std::string_view(parameterBuffer.data(), parameterBuffer.size()), body);
    postEnvelope(body, net::proto::MessageTypes::ERROR, "A parameter cannot hold a UInt64 value");
}

TEST_F(TuringProtoParametersTest, aQueryAndParametersUnderTheLimitRun) {
    std::string query = "MATCH (n:$label) RETURN n.name ORDER BY n.name";
    query.resize(400000, ' ');

    ParameterMap parameters;
    setString(parameters, "label", "Founder");
    setString(parameters, "padding", std::string(400000, 'x'));

    expectRows(query, &parameters, {{"Adam"}, {"Remy"}});
}

TEST_F(TuringProtoParametersTest, aQueryAndParametersOverTheLimitGetTheServerError) {
    std::string query = "RETURN 1";
    query.resize(600000, ' ');

    ParameterMap parameters;
    setString(parameters, "padding", std::string(600000, 'x'));

    std::string message;
    try {
        Rows rows;
        QueryStatus status;
        runQuery(query, &parameters, rows, status);
    } catch (const TuringException& e) {
        message = e.what();
    }

    const std::string limit = std::to_string(net::NetBuffer::BUFFER_SIZE);
    EXPECT_NE(message.find(limit), std::string::npos) << message;
}
