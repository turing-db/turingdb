#include <gtest/gtest.h>

#include <stdint.h>
#include <chrono>
#include <memory>
#include <thread>

#include "AbstractThreadContext.h"
#include "HTTPParser.h"
#include "HTTPWriter.h"
#include "RemoteTestUtils.h"
#include "TCPServer.h"
#include "UriParser.h"

using namespace turing::test;

TEST(TCPServerBoundPortTest, aServerStartedOnPortZeroReportsThePortItIsBoundTo) {
    net::TCPServer::Functions functions;
    functions._processor = [](net::AbstractThreadContext* threadContext, net::TCPConnection& connection) {};
    functions._createThreadContext = [] {
        return std::make_unique<net::AbstractThreadContext>();
    };
    functions._createParser = [](net::NetBuffer* inputBuffer) {
        return std::unique_ptr<net::AbstractTCPParser>(new net::HTTPParser<net::URIParser>(inputBuffer));
    };
    functions._createWriter = [] {
        return std::make_unique<net::HTTPWriter>();
    };

    net::TCPServer server(std::move(functions));
    server.setPort(0);
    server.setWorkerCount(1);
    server.setMaxConnections(4);
    server.initialize();

    std::thread serverThread([&server] {
        server.start();
    });

    const uint16_t port = server.getPort();
    const bool listening = waitUntilListening(port, std::chrono::milliseconds(2000));

    server.terminate();
    serverThread.join();

    EXPECT_NE(port, 0);
    EXPECT_TRUE(listening);
}
