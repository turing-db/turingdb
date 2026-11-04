#include <gtest/gtest.h>

#include <stdint.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <memory>
#include <string>

#include "ProtoTestUtils.h"
#include "QueryStatus.h"
#include "TuringClient.h"
#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class TuringClientTimeoutTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        _listener = ::socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(_listener, 0);

        sockaddr_in address {};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        ASSERT_EQ(::bind(_listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
        ASSERT_EQ(::listen(_listener, 4), 0);

        socklen_t addressSize = sizeof(address);
        ASSERT_EQ(::getsockname(_listener, reinterpret_cast<sockaddr*>(&address), &addressSize), 0);
        _port = ntohs(address.sin_port);
    }

    void terminate() override {
        if (_listener >= 0) {
            ::close(_listener);
        }
    }

    std::unique_ptr<TuringTestEnv> _env;
    int _listener {-1};
    uint16_t _port {0};
};

TEST_F(TuringClientTimeoutTest, aSilentServerTimesOut) {
    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setTimeout(std::chrono::milliseconds(200));
    client.connect();

    const auto start = std::chrono::steady_clock::now();

    std::string message;
    try {
        ResultRows rows;
        QueryStatus status;
        runProtoQuery(&client, "RETURN 1", nullptr, rows, status);
    } catch (const TuringException& e) {
        message = e.what();
    }

    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_EQ(message, "Failed to read HTTP response: timed out after 200 ms");
    EXPECT_LT(elapsed, std::chrono::seconds(5));
    EXPECT_FALSE(client.isConnected());
}
