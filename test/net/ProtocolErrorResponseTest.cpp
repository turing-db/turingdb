#include <gtest/gtest.h>

#include <stddef.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string>
#include <string_view>

#include "TuringProtoWriter.h"

using namespace net::proto;

namespace {

constexpr std::string_view MESSAGE = "Unauthorized: missing or invalid authentication token";

void receiveAll(int socket, std::string& received) {
    char buffer[4096];
    while (true) {
        const ssize_t bytes = ::recv(socket, buffer, sizeof(buffer), 0);
        if (bytes <= 0) {
            return;
        }
        received.append(buffer, bytes);
    }
}

}

// A protocol error response opens with its status and Connection: close and holds one
// PROTOCOL_ERROR packet; a peer that already left makes it fail quietly, since the
// connection closes after it anyway
TEST(ProtocolErrorResponseTest, opensAResponseThatClosesTheConnection) {
    int sockets[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);

    TuringProtoWriter writer;
    writer.setSocket(sockets[0]);
    writer.writeProtocolErrorResponse(net::HTTP::Status::UNAUTHORIZED, MESSAGE);
    writer.flush();
    ::close(sockets[0]);

    std::string received;
    receiveAll(sockets[1], received);
    ::close(sockets[1]);

    EXPECT_TRUE(received.starts_with("HTTP/1.1 401 Unauthorized\r\n")) << received;
    EXPECT_NE(received.find("Connection: close"), std::string::npos) << received;
    EXPECT_NE(received.find(MESSAGE), std::string::npos) << received;
    EXPECT_TRUE(received.ends_with("\r\n00000000\r\n\r\n")) << received;
}

TEST(ProtocolErrorResponseTest, survivesAPeerThatClosed) {
    int sockets[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
    ::close(sockets[1]);

    TuringProtoWriter writer;
    writer.setSocket(sockets[0]);
    EXPECT_NO_THROW(writer.writeProtocolErrorResponse(net::HTTP::Status::UNAUTHORIZED, MESSAGE));
    EXPECT_TRUE(writer.errorOccured());

    ::close(sockets[0]);
}
