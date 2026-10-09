#include <gtest/gtest.h>

#include <errno.h>
#include <netinet/in.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
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

struct HttpResponse {
    std::string statusLine;
    std::string body;
};

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

int connectTo(uint16_t port) {
    const int socket = ::socket(AF_INET, SOCK_STREAM, 0);
    if (socket < 0) {
        throw TuringException("Could not create the client socket");
    }

    timeval timeout {};
    timeout.tv_sec = 30;
    setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);

    if (::connect(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
        ::close(socket);
        throw TuringException("Could not connect to the test server");
    }

    return socket;
}

void sendAll(int socket, const std::string& data) {
    size_t sent = 0;

    while (sent < data.size()) {
        const ssize_t bytes = ::send(socket, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (bytes <= 0) {
            throw TuringException("Request send failed after " + std::to_string(sent)
                                  + " of " + std::to_string(data.size())
                                  + " bytes: " + strerror(errno));
        }

        sent += bytes;
    }
}

void receiveUntil(int socket, std::string& received, size_t size) {
    char buffer[4096];

    while (received.size() < size) {
        const ssize_t bytes = ::recv(socket, buffer, sizeof(buffer), 0);
        if (bytes <= 0) {
            throw TuringException("Response ended after " + std::to_string(received.size())
                                  + " bytes: " + received);
        }

        received.append(buffer, bytes);
    }
}

size_t receiveLine(int socket, std::string& received, size_t position) {
    while (true) {
        const size_t lineEnd = received.find("\r\n", position);
        if (lineEnd != std::string::npos) {
            return lineEnd;
        }

        receiveUntil(socket, received, received.size() + 1);
    }
}

void postQuery(uint16_t port, const std::string& query, HttpResponse& response) {
    const int socket = connectTo(port);

    std::string request = "POST /query?graph=";
    request += GRAPH_NAME;
    request += " HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: ";
    request += std::to_string(query.size());
    request += "\r\n\r\n";
    request += query;

    try {
        sendAll(socket, request);

        std::string received;
        const size_t statusLineEnd = receiveLine(socket, received, 0);
        response.statusLine = received.substr(0, statusLineEnd);

        size_t headerEnd = received.find("\r\n\r\n");
        while (headerEnd == std::string::npos) {
            receiveUntil(socket, received, received.size() + 1);
            headerEnd = received.find("\r\n\r\n");
        }

        size_t position = headerEnd + 4;
        while (true) {
            const size_t sizeLineEnd = receiveLine(socket, received, position);
            const size_t chunkSize = std::stoul(received.substr(position, sizeLineEnd - position), nullptr, 16);
            const size_t chunkBegin = sizeLineEnd + 2;
            receiveUntil(socket, received, chunkBegin + chunkSize + 2);

            if (chunkSize == 0) {
                break;
            }

            response.body.append(received, chunkBegin, chunkSize);
            position = chunkBegin + chunkSize + 2;
        }
    } catch (...) {
        ::close(socket);
        throw;
    }

    ::close(socket);
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
        postQuery(_port, query, response);

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
    postQuery(_port, "MATCH (n) WHERE n.name = 'Remy' RETURN n.name", response);

    EXPECT_EQ(response.statusLine, "HTTP/1.1 200 OK");
    const nlohmann::json json = nlohmann::json::parse(response.body);
    EXPECT_FALSE(json.contains("error")) << response.body;
}

TEST_F(RequestTooLargeTest, BodyUnderTheLimitRuns) {
    startServer();

    std::string query = "MATCH (n) WHERE n.name = 'Remy' RETURN n.name";
    query.resize(1000000, ' ');

    HttpResponse response;
    postQuery(_port, query, response);

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
