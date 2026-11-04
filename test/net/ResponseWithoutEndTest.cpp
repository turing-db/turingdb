#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <string>
#include <string_view>
#include <thread>

#include "LocalMemory.h"
#include "TuringAsyncClient.h"
#include "TuringClient.h"
#include "TuringException.h"
#include "TuringProtoHeaders.h"
#include "dataframe/Dataframe.h"

using namespace db;

namespace {

constexpr std::string_view WITHOUT_END = "The response ended before the END packet";

// The CHUNK_HEADER of a result with no columns, then the terminator: no END packet
void makeResponseWithoutEnd(std::string& response) {
    response = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: Keep-Alive\r\n\r\n";

    const net::proto::WireSize columnCount = 0;
    const net::proto::WireSize dataLength = sizeof(columnCount);

    std::string packet;
    packet.push_back(static_cast<char>(net::proto::MessageTypes::CHUNK_HEADER));
    packet.append(reinterpret_cast<const char*>(&dataLength), sizeof(dataLength));
    packet.append(reinterpret_cast<const char*>(&columnCount), sizeof(columnCount));

    char sizeLine[16];
    snprintf(sizeLine, sizeof(sizeLine), "%08zx\r\n", packet.size());
    response += sizeLine;
    response += packet;
    response += "\r\n00000000\r\n\r\n";
}

// Accepts one connection, reads one request and sends the canned response
void answerOnce(int listener, const std::string& response) {
    const int connection = ::accept(listener, nullptr, nullptr);
    if (connection < 0) {
        return;
    }

    std::string received;
    char buffer[4096];
    size_t headerEnd = std::string::npos;
    size_t bodyLength = 0;
    while (headerEnd == std::string::npos || received.size() < headerEnd + 4 + bodyLength) {
        const ssize_t bytes = ::recv(connection, buffer, sizeof(buffer), 0);
        if (bytes <= 0) {
            ::close(connection);
            return;
        }
        received.append(buffer, bytes);

        if (headerEnd == std::string::npos) {
            headerEnd = received.find("\r\n\r\n");
            const size_t lengthField = received.find("Content-Length: ");
            if (headerEnd != std::string::npos && lengthField != std::string::npos) {
                bodyLength = std::stoul(received.substr(lengthField + 16));
            }
        }
    }

    ::send(connection, response.data(), response.size(), MSG_NOSIGNAL);
    ::close(connection);
}

}

// A response that reaches its terminator without an END packet was cut short, so a client
// reports it instead of returning a partial result as a success
class ResponseWithoutEndTest : public ::testing::Test {
protected:
    void SetUp() override {
        _listener = ::socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(_listener, 0);

        sockaddr_in address {};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        ASSERT_EQ(::bind(_listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
        ASSERT_EQ(::listen(_listener, 1), 0);

        socklen_t addressLength = sizeof(address);
        ASSERT_EQ(::getsockname(_listener, reinterpret_cast<sockaddr*>(&address), &addressLength), 0);
        _port = ntohs(address.sin_port);

        std::string response;
        makeResponseWithoutEnd(response);
        _server = std::thread(answerOnce, _listener, response);
    }

    void TearDown() override {
        ::shutdown(_listener, SHUT_RDWR);
        if (_server.joinable()) {
            _server.join();
        }
        ::close(_listener);
    }

    LocalMemory _memory;
    int _listener {-1};
    uint16_t _port {0};
    std::thread _server;
};

TEST_F(ResponseWithoutEndTest, rejectsItInTheBlockingClient) {
    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_memory);
    client.connect();

    try {
        client.sendQuery("MATCH (n) RETURN n", [](const Dataframe*) {});
        FAIL() << "the response without END was accepted";
    } catch (const TuringException& e) {
        EXPECT_NE(std::string_view(e.what()).find(WITHOUT_END), std::string_view::npos) << e.what();
    }
}

TEST_F(ResponseWithoutEndTest, rejectsItInTheAsyncClient) {
    net::proto::TuringAsyncClient client("127.0.0.1", std::to_string(_port), &_memory);
    client.connect();

    std::string error = "no error";
    try {
        client.sendQuery("MATCH (n) RETURN n", [](const Dataframe*) {});

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!client.isRecvComplete() && std::chrono::steady_clock::now() < deadline) {
            client.send();
            client.recv();
        }
    } catch (const TuringException& e) {
        error = e.what();
    }

    EXPECT_NE(error.find(WITHOUT_END), std::string::npos) << error;
}
