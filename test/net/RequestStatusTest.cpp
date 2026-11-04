#include <gtest/gtest.h>

#include <stdint.h>
#include <chrono>
#include <memory>
#include <string>
#include <string_view>

#include "Authenticator.h"
#include "DBServerConfig.h"
#include "ProtoTestUtils.h"
#include "QueryStatus.h"
#include "RemoteTestUtils.h"
#include "TuringClient.h"
#include "TuringDB.h"
#include "TuringException.h"
#include "TuringProtoHeaders.h"
#include "TuringServer.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr const char* TOKEN = "secret-key";
constexpr std::string_view UNAUTHORIZED_MESSAGE = "Unauthorized: missing or invalid authentication token";

}

class RequestStatusTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _env->getDB().setAuthenticator(&_authenticator);

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

    void sendRequest(std::string_view method,
                     std::string_view path,
                     std::string_view token,
                     const std::string& body,
                     HttpResponse& response) {
        std::string request = std::string(method) + " " + std::string(path) + " HTTP/1.1\r\nHost: 127.0.0.1\r\n";
        if (!token.empty()) {
            request += "Authorization: Bearer " + std::string(token) + "\r\n";
        }

        request += "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n";
        request += body;

        sendHttpRequest(_port, request, response);
    }

    void expectRejected(const HttpResponse& response,
                        std::string_view expectedStatusLine,
                        std::string_view expectedMessage) {
        EXPECT_EQ(response.statusLine, expectedStatusLine);

        const size_t headerSize = net::proto::ProtoHeader::wireSize();
        ASSERT_GE(response.body.size(), headerSize);

        const net::proto::ProtoHeader header = net::proto::ProtoHeader::decode(response.body.data(),
                                                                              response.body.size());
        EXPECT_EQ(header._type, net::proto::MessageTypes::PROTOCOL_ERROR);
        EXPECT_EQ(response.body.substr(headerSize), expectedMessage);
    }

    Authenticator _authenticator {TOKEN};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<TuringServer> _server;
    uint16_t _port {0};
};

TEST_F(RequestStatusTest, aValidQueryIsOk) {
    std::string body;
    makeProtoRequestBody("RETURN 1", body);

    HttpResponse response;
    sendRequest("POST", "/query?graph=default", TOKEN, body, response);

    EXPECT_EQ(response.statusLine, "HTTP/1.1 200 OK");
}

TEST_F(RequestStatusTest, aMissingTokenIsUnauthorized) {
    std::string body;
    makeProtoRequestBody("RETURN 1", body);

    HttpResponse response;
    sendRequest("POST", "/query?graph=default", "", body, response);

    expectRejected(response, "HTTP/1.1 401 Unauthorized", UNAUTHORIZED_MESSAGE);
}

TEST_F(RequestStatusTest, aWrongTokenIsUnauthorized) {
    std::string body;
    makeProtoRequestBody("RETURN 1", body);

    HttpResponse response;
    sendRequest("POST", "/query?graph=default", "wrong-key", body, response);

    expectRejected(response, "HTTP/1.1 401 Unauthorized", UNAUTHORIZED_MESSAGE);
}

TEST_F(RequestStatusTest, aGetIsNotAllowed) {
    HttpResponse response;
    sendRequest("GET", "/query?graph=default", TOKEN, "", response);

    expectRejected(response, "HTTP/1.1 405 Method Not Allowed", "Only POST is supported by the binary protocol");
}

TEST_F(RequestStatusTest, anUnknownEndpointIsNotFound) {
    std::string body;
    makeProtoRequestBody("RETURN 1", body);

    HttpResponse response;
    sendRequest("POST", "/graphs", TOKEN, body, response);

    expectRejected(response, "HTTP/1.1 404 Not Found", "Unsupported endpoint for the binary protocol");
}

TEST_F(RequestStatusTest, aMalformedBodyIsABadRequest) {
    HttpResponse response;
    sendRequest("POST", "/query?graph=default", TOKEN, "MA", response);

    expectRejected(response, "HTTP/1.1 400 Bad Request", "The request body ends before the query length");
}

TEST_F(RequestStatusTest, aQueryErrorStaysInTheStream) {
    std::string body;
    makeProtoRequestBody("MATCH (", body);

    HttpResponse response;
    sendRequest("POST", "/query?graph=default", TOKEN, body, response);

    EXPECT_EQ(response.statusLine, "HTTP/1.1 200 OK");

    const net::proto::ProtoHeader header = net::proto::ProtoHeader::decode(response.body.data(),
                                                                          response.body.size());
    EXPECT_EQ(header._type, net::proto::MessageTypes::ERROR);
}

TEST_F(RequestStatusTest, theClientReportsTheRejection) {
    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("default");
    client.connect();

    std::string message;
    try {
        ResultRows rows;
        QueryStatus status;
        runProtoQuery(&client, "RETURN 1", nullptr, rows, status);
    } catch (const TuringException& e) {
        message = e.what();
    }

    client.disconnect();

    EXPECT_NE(message.find(UNAUTHORIZED_MESSAGE), std::string::npos) << message;
}

TEST_F(RequestStatusTest, theClientWithATokenRunsTheQuery) {
    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName("default");
    client.setAuthToken(TOKEN);
    client.connect();

    ResultRows rows;
    QueryStatus status;
    runProtoQuery(&client, "RETURN 1", nullptr, rows, status);

    client.disconnect();

    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(rows, (ResultRows {{"1"}}));
}
