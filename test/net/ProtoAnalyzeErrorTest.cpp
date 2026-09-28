#include <gtest/gtest.h>

#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <string>

#include "HTTPParser.h"
#include "NetBuffer.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoWriter.h"
#include "UriParser.h"

namespace {

std::string drainSocket(int socket) {
    std::string received;
    char buffer[4096];

    while (true) {
        const ssize_t bytes = recv(socket, buffer, sizeof(buffer), 0);
        if (bytes <= 0) {
            return received;
        }

        received.append(buffer, bytes);
    }
}

}

// A malformed request to the binary-protocol server used to reach
// HTTPParser::handleAnalyzeError, which static-cast the TuringProtoWriter to an HTTPWriter
// and corrupted memory. handleAnalyzeError now dispatches on the writer, so the proto writer
// answers with a PROTOCOL_ERROR frame instead.
TEST(ProtoAnalyzeErrorTest, MalformedRequestGetsProtocolErrorNotCrash) {
    int sockets[2] {};
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);

    net::NetBuffer buffer;
    auto bufferWriter = buffer.getWriter();
    const std::string request = "garbage";
    bufferWriter.writeString(request.data(), request.size());

    net::HTTPParser<net::URIParser> parser(&buffer);
    const auto analyzeResult = parser.analyze();
    ASSERT_FALSE(analyzeResult.has_value());

    net::proto::TuringProtoWriter writer;
    writer.setSocket(sockets[0]);

    parser.handleAnalyzeError(analyzeResult.error(), writer);
    writer.reset();

    close(sockets[0]);
    const std::string response = drainSocket(sockets[1]);
    close(sockets[1]);

    ASSERT_TRUE(response.starts_with("HTTP/1.1 200 OK\r\n"));

    const size_t headerEnd = response.find("\r\n\r\n");
    ASSERT_NE(headerEnd, std::string::npos);

    const size_t chunkBegin = headerEnd + 4 + net::http::CHUNK_HEADER_LINE_SIZE;
    ASSERT_GE(response.size(), chunkBegin + net::proto::ProtoHeader::wireSize());

    const net::proto::ProtoHeader header = net::proto::ProtoHeader::decode(response.data() + chunkBegin,
                                                                          net::proto::ProtoHeader::wireSize());
    EXPECT_EQ(header._type, net::proto::MessageTypes::PROTOCOL_ERROR);
}
