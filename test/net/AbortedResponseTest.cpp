#include <gtest/gtest.h>

#include <netinet/in.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string>
#include <vector>

#include "ProtoServerTest.h"
#include "ProtoTestUtils.h"
#include "QueryStatus.h"
#include "TuringClient.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr size_t BYTES_READ_BEFORE_CLOSE = 64ul * 1024;

const std::string LONG_STRINGS = "[x IN range(1, 100000) | toString(x) + '" + std::string(200, 'a') + "']";
const std::string LIST_QUERY = "RETURN " + LONG_STRINGS + " AS list";
const std::string MAP_QUERY = "RETURN {list: " + LONG_STRINGS + "} AS map";
const std::string CHECK_QUERY = "UNWIND range(1, 3) AS x RETURN x, [x, x + 1] AS list, {x: x} AS map";

// Posts the query, reads the first bytes of its response and closes the socket while the
// server is still writing, so the server's next send fails in the middle of the result.
void abortResponse(uint16_t port, const std::string& query) {
    const int socket = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(socket, 0);

    sockaddr_in address {};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    ASSERT_EQ(::connect(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);

    std::string body;
    makeProtoRequestBody(query, body);

    std::string request = "POST /query?graph=simpledb HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: ";
    request += std::to_string(body.size());
    request += "\r\n\r\n";
    request += body;

    size_t sent = 0;
    while (sent < request.size()) {
        const ssize_t bytes = ::send(socket, request.data() + sent, request.size() - sent, MSG_NOSIGNAL);
        ASSERT_GT(bytes, 0);
        sent += static_cast<size_t>(bytes);
    }

    std::vector<char> buffer(BYTES_READ_BEFORE_CLOSE);
    size_t received = 0;
    while (received < BYTES_READ_BEFORE_CLOSE) {
        const ssize_t bytes = ::recv(socket, buffer.data() + received, buffer.size() - received, 0);
        ASSERT_GT(bytes, 0);
        received += static_cast<size_t>(bytes);
    }

    ::close(socket);
}

}

// A response cut short in the middle of a list or a map leaves nothing behind for the next
// response the server writes. The server has one worker and hands a closed connection's slot,
// with its writer, to the next connection it accepts.
class AbortedResponseTest : public ProtoServerTest {
protected:
    void expectCheckQueryRows(net::proto::TuringClient* client) {
        ResultRows rows;
        QueryStatus status;
        runProtoQuery(client, CHECK_QUERY, nullptr, rows, status);

        ASSERT_TRUE(status.isOk()) << status.getError();

        const ResultRows expected = {
            {"1", "[1, 2]", "{x: 1}"},
            {"2", "[2, 3]", "{x: 2}"},
            {"3", "[3, 4]", "{x: 3}"},
        };
        EXPECT_EQ(rows, expected);
    }

    // The first client is answered once the worker has closed the aborted connection. Either it
    // took that connection's slot, or the second one does, since the first stays open.
    void expectNextResponsesIntact() {
        net::proto::TuringClient first("127.0.0.1", std::to_string(_port), &_env->getMem());
        first.setGraphName("simpledb");
        first.connect();
        expectCheckQueryRows(&first);

        net::proto::TuringClient second("127.0.0.1", std::to_string(_port), &_env->getMem());
        second.setGraphName("simpledb");
        second.connect();
        expectCheckQueryRows(&second);

        second.disconnect();
        first.disconnect();
    }
};

TEST_F(AbortedResponseTest, answersAfterAResponseAbortedInAList) {
    ASSERT_NO_FATAL_FAILURE(abortResponse(_port, LIST_QUERY));
    expectNextResponsesIntact();
}

TEST_F(AbortedResponseTest, answersAfterAResponseAbortedInAMap) {
    ASSERT_NO_FATAL_FAILURE(abortResponse(_port, MAP_QUERY));
    expectNextResponsesIntact();
}
