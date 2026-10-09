#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <chrono>
#include <memory>
#include <string>

#include <nlohmann/json.hpp>

#include "DBServerConfig.h"
#include "Graph.h"
#include "NetBuffer.h"
#include "RemoteTestUtils.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringClient.h"
#include "TuringException.h"
#include "TuringServer.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr const char* GRAPH_NAME = "simpledb";

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

        _serverConfig.setAddress("127.0.0.1");
        _serverConfig.setPort(_port);
        _serverConfig.setWorkerCount(1);
        _serverConfig.setMaxConnections(16);
    }

    void terminate() override {
        if (_server) {
            _server->stop();
            _server->wait();
        }
    }

    void startServer() {
        _server = std::make_unique<TuringServer>(_serverConfig, _env->getDB());
        _server->start();

        ASSERT_TRUE(waitUntilListening(_port, std::chrono::milliseconds(2000)));
    }

    void expectRequestTooLarge(const std::string& query) {
        HttpResponse response;
        postHttpQuery(_port, "/query?graph=simpledb", query, response);

        EXPECT_EQ(response.statusLine, "HTTP/1.1 413 Content Too Large");

        const nlohmann::json json = nlohmann::json::parse(response.body);
        ASSERT_TRUE(json.contains("error")) << response.body;
        ASSERT_TRUE(json.contains("error_details")) << response.body;
        EXPECT_EQ(json["error"], "REQUEST_TOO_BIG");

        const std::string details = json["error_details"];
        const std::string limit = std::to_string(net::NetBuffer::BUFFER_SIZE);
        EXPECT_NE(details.find(limit), std::string::npos) << details;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<TuringServer> _server;
    DBServerConfig _serverConfig;
    uint16_t _port {0};
};

TEST_F(RequestTooLargeTest, BodyOverTheLimitGetsAJsonError) {
    startServer();

    std::string query;
    makeCreateQuery(1177786, query);
    expectRequestTooLarge(query);
}

TEST_F(RequestTooLargeTest, BodyFarOverTheLimitIsReadBeforeTheError) {
    startServer();

    std::string query;
    makeCreateQuery(16 * net::NetBuffer::BUFFER_SIZE, query);
    expectRequestTooLarge(query);
}

TEST_F(RequestTooLargeTest, ServerAnswersTheNextRequestAfterTheError) {
    startServer();

    std::string query;
    makeCreateQuery(2 * net::NetBuffer::BUFFER_SIZE, query);
    expectRequestTooLarge(query);

    HttpResponse response;
    postHttpQuery(_port, "/query?graph=simpledb", "MATCH (n) WHERE n.name = 'Remy' RETURN n.name", response);

    EXPECT_EQ(response.statusLine, "HTTP/1.1 200 OK");
    const nlohmann::json json = nlohmann::json::parse(response.body);
    EXPECT_FALSE(json.contains("error")) << response.body;
}

TEST_F(RequestTooLargeTest, BodyUnderTheLimitRuns) {
    startServer();

    std::string query = "MATCH (n) WHERE n.name = 'Remy' RETURN n.name";
    query.resize(1000000, ' ');

    HttpResponse response;
    postHttpQuery(_port, "/query?graph=simpledb", query, response);

    EXPECT_EQ(response.statusLine, "HTTP/1.1 200 OK");
    const nlohmann::json json = nlohmann::json::parse(response.body);
    EXPECT_FALSE(json.contains("error")) << response.body;
    EXPECT_EQ(json["data"][0][0][0], "Remy") << response.body;
}

TEST_F(RequestTooLargeTest, BinaryProtocolNamesTheLimit) {
    ProtoEnvScope protoScope;
    startServer();

    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName(GRAPH_NAME);
    client.connect();

    std::string query;
    makeCreateQuery(1177786, query);

    std::string message;
    try {
        client.sendQuery(query, [](const Dataframe*) {});
    } catch (const TuringException& exception) {
        message = exception.what();
    }

    const std::string limit = std::to_string(net::NetBuffer::BUFFER_SIZE);
    EXPECT_NE(message.find(limit), std::string::npos) << message;
}
