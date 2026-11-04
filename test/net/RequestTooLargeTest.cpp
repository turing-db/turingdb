#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <chrono>
#include <memory>
#include <string>
#include <string_view>

#include "DBServerConfig.h"
#include "Graph.h"
#include "NetBuffer.h"
#include "ProtoTestUtils.h"
#include "QueryStatus.h"
#include "RemoteTestUtils.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringClient.h"
#include "TuringException.h"
#include "TuringProtoHeaders.h"
#include "TuringServer.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr const char* GRAPH_NAME = "simpledb";
constexpr const char* REMY_QUERY = "MATCH (n) WHERE n.name = 'Remy' RETURN n.name";

constexpr std::string_view LIMIT_MESSAGE = "The request is over the server limit of 1048576 bytes for the request "
                                           "line, headers and body together. Split the query, or load bulk data "
                                           "from a file with LOAD JSONL or LOAD PARQUET.";

void makeCreateQuery(size_t minimumSize, std::string& query) {
    query = "CREATE ";
    size_t nodeIndex = 0;

    while (query.size() < minimumSize) {
        if (nodeIndex != 0) {
            query += ", ";
        }

        query += "(:Detection {id: " + std::to_string(nodeIndex) + ", sensor: 'radar'})";
        nodeIndex++;
    }
}

}

class RequestTooLargeTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        Graph* graph = nullptr;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            system.createGraph("default");
            graph = system.createGraph(GRAPH_NAME);
        }
        SimpleGraph::createSimpleGraph(graph);

        _port = reserveFreePort();

        DBServerConfig serverConfig;
        serverConfig.setAddress("127.0.0.1");
        serverConfig.setPort(_port);
        serverConfig.setWorkerCount(1);
        serverConfig.setMaxConnections(16);

        _server = std::make_unique<TuringServer>(serverConfig, _env->getDB());
        _server->start();

        ASSERT_TRUE(waitUntilListening(_port, std::chrono::milliseconds(2000)));
    }

    void terminate() override {
        if (_server) {
            _server->stop();
            _server->wait();
        }
    }

    void expectRequestTooLarge(const std::string& query) {
        std::string body;
        makeProtoRequestBody(query, body);

        HttpResponse response;
        postHttpBody(_port, "/query?graph=simpledb", body, response);

        EXPECT_EQ(response.statusLine, "HTTP/1.1 413 Content Too Large");

        const size_t headerSize = net::proto::ProtoHeader::wireSize();
        ASSERT_GE(response.body.size(), headerSize);

        const net::proto::ProtoHeader header = net::proto::ProtoHeader::decode(response.body.data(),
                                                                              response.body.size());
        EXPECT_EQ(header._type, net::proto::MessageTypes::PROTOCOL_ERROR);

        EXPECT_EQ(response.body.substr(headerSize), LIMIT_MESSAGE);
    }

    void expectRemy(const std::string& query) {
        net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
        client.setGraphName(GRAPH_NAME);
        client.connect();

        ResultRows rows;
        QueryStatus status;
        runProtoQuery(&client, query, nullptr, rows, status);

        client.disconnect();

        ASSERT_TRUE(status.isOk()) << status.getError();
        EXPECT_EQ(rows, (ResultRows {{"Remy"}}));
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<TuringServer> _server;
    uint16_t _port {0};
};

TEST_F(RequestTooLargeTest, BodyOverTheLimitIsAProtocolError) {
    std::string query;
    makeCreateQuery(1177786, query);
    expectRequestTooLarge(query);
}

TEST_F(RequestTooLargeTest, BodyFarOverTheLimitIsReadBeforeTheError) {
    std::string query;
    makeCreateQuery(16 * net::NetBuffer::BUFFER_SIZE, query);
    expectRequestTooLarge(query);
}

TEST_F(RequestTooLargeTest, ServerAnswersTheNextRequestAfterTheError) {
    std::string query;
    makeCreateQuery(2 * net::NetBuffer::BUFFER_SIZE, query);
    expectRequestTooLarge(query);

    expectRemy(REMY_QUERY);
}

TEST_F(RequestTooLargeTest, BodyUnderTheLimitRuns) {
    std::string query = REMY_QUERY;
    query.resize(1000000, ' ');

    expectRemy(query);
}

TEST_F(RequestTooLargeTest, TheClientReportsTheLimit) {
    std::string query;
    makeCreateQuery(2 * net::NetBuffer::BUFFER_SIZE, query);

    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName(GRAPH_NAME);
    client.connect();

    std::string message;
    try {
        ResultRows rows;
        QueryStatus status;
        runProtoQuery(&client, query, nullptr, rows, status);
    } catch (const TuringException& e) {
        message = e.what();
    }

    client.disconnect();

    EXPECT_NE(message.find(LIMIT_MESSAGE), std::string::npos) << message;
}
