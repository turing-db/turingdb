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
#include <spdlog/fmt/fmt.h>

#include "DBServerConfig.h"
#include "Graph.h"
#include "RemoteTestUtils.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringException.h"
#include "TuringProtoHeaders.h"
#include "TuringServer.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr const char* GRAPH_NAME = "simpledb";
constexpr const char* COUNT_QUERY = "MATCH (n) RETURN count(n)";
constexpr uint64_t SIMPLEDB_NODE_COUNT = 18;

struct HttpResponse {
    std::string statusLine;
    std::string body;
};

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
            throw TuringException(std::string("Request send failed: ") + strerror(errno));
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

void postQuery(uint16_t port, const std::string& uri, const std::string& query, HttpResponse& response) {
    const int socket = connectTo(port);

    std::string request = "POST " + uri + " HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: ";
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

std::string toHex(CommitHash hash) {
    return fmt::format("{:x}", hash.get());
}

std::string commitError(std::string_view value) {
    return fmt::format("The commit parameter '{}' is not a hexadecimal commit hash or 'head'", value);
}

std::string changeError(std::string_view value) {
    return fmt::format("The change parameter '{}' is not a hexadecimal change ID or 'head'", value);
}

}

class VersionParamTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        Graph* graph = nullptr;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            system.createGraph("default");
            graph = system.createGraph(GRAPH_NAME);
        }

        _emptyCommit = toHex(graph->getHeadHash());
        SimpleGraph::createSimpleGraph(graph);
        _headCommit = toHex(graph->getHeadHash());

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

    void expectCount(const std::string& params, uint64_t expectedCount) {
        HttpResponse response;
        postQuery(_port, "/query?" + params, COUNT_QUERY, response);

        EXPECT_EQ(response.statusLine, "HTTP/1.1 200 OK") << params;
        const nlohmann::json json = nlohmann::json::parse(response.body);
        ASSERT_FALSE(json.contains("error")) << params << ": " << response.body;
        EXPECT_EQ(json["data"][0][0][0], expectedCount) << params << ": " << response.body;
    }

    void expectError(const std::string& params,
                     const std::string& expectedError,
                     const std::string& expectedDetails) {
        HttpResponse response;
        postQuery(_port, "/query?" + params, COUNT_QUERY, response);

        const nlohmann::json json = nlohmann::json::parse(response.body);
        ASSERT_TRUE(json.contains("error")) << params << ": " << response.body;
        EXPECT_FALSE(json.contains("data")) << params << ": " << response.body;
        EXPECT_EQ(json["error"], expectedError) << params;
        EXPECT_EQ(json["error_details"], expectedDetails) << params;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<TuringServer> _server;
    DBServerConfig _serverConfig;
    std::string _emptyCommit;
    std::string _headCommit;
    uint16_t _port {0};
};

TEST_F(VersionParamTest, wellFormedCommitsReadTheirVersion) {
    startServer();

    expectCount("graph=simpledb", SIMPLEDB_NODE_COUNT);
    expectCount("graph=simpledb&commit=head&change=head", SIMPLEDB_NODE_COUNT);
    expectCount("graph=simpledb&commit=" + _headCommit, SIMPLEDB_NODE_COUNT);
    expectCount("graph=simpledb&commit=" + _emptyCommit, 0);
}

TEST_F(VersionParamTest, unparseableCommitIsRejected) {
    startServer();

    expectError("graph=simpledb&commit=zzz", "COMMIT_NOT_FOUND", commitError("zzz"));
    expectError("graph=simpledb&commit=xyz123", "COMMIT_NOT_FOUND", commitError("xyz123"));
    expectError("graph=simpledb&commit=" + _emptyCommit + "zz",
                "COMMIT_NOT_FOUND",
                commitError(_emptyCommit + "zz"));
    expectError("graph=simpledb&commit=" + _emptyCommit + "=1",
                "COMMIT_NOT_FOUND",
                commitError(_emptyCommit + "=1"));
}

TEST_F(VersionParamTest, emptyCommitIsRejected) {
    startServer();

    expectError("graph=simpledb&commit=", "COMMIT_NOT_FOUND", commitError(""));
    expectError("commit=&graph=simpledb", "COMMIT_NOT_FOUND", commitError(""));
    expectError("graph=simpledb&commit", "COMMIT_NOT_FOUND", commitError(""));
}

TEST_F(VersionParamTest, unparseableChangeIsRejected) {
    startServer();

    expectError("graph=simpledb&change=zzz", "CHANGE_NOT_FOUND", changeError("zzz"));
    expectError("graph=simpledb&change=", "CHANGE_NOT_FOUND", changeError(""));
    expectError("change=&graph=simpledb", "CHANGE_NOT_FOUND", changeError(""));
}

TEST_F(VersionParamTest, headSuffixGetsAJsonError) {
    startServer();

    HttpResponse response;
    postQuery(_port, "/query?graph=simpledb&commit=" + _headCommit + "(HEAD)", COUNT_QUERY, response);

    EXPECT_EQ(response.statusLine, "HTTP/1.1 400 Bad Request");
    const nlohmann::json json = nlohmann::json::parse(response.body);
    EXPECT_EQ(json["error"], "INVALID_URI") << response.body;
    EXPECT_TRUE(json.contains("error_details")) << response.body;
}

TEST_F(VersionParamTest, binaryProtocolRejectsAnUnparseableCommit) {
    ProtoEnvScope protoScope;
    startServer();

    HttpResponse response;
    postQuery(_port, "/query?graph=simpledb&commit=zzz", COUNT_QUERY, response);

    EXPECT_EQ(response.statusLine, "HTTP/1.1 200 OK");
    ASSERT_GE(response.body.size(), net::proto::ProtoHeader::wireSize());

    const net::proto::ProtoHeader header = net::proto::ProtoHeader::decode(response.body.data(),
                                                                          response.body.size());
    EXPECT_EQ(header._type, net::proto::MessageTypes::ERROR);
    EXPECT_NE(response.body.find(commitError("zzz")), std::string::npos) << response.body;
}
