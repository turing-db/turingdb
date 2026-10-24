#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string>

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "DBServerConfig.h"
#include "RemoteTestUtils.h"
#include "TuringException.h"
#include "TuringServer.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"
#include "VisualizerProxy.h"

using namespace db;
using namespace turing::test;

class TakenPortTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        _heldSocket = ::socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(_heldSocket, 0);

        sockaddr_in address {};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        ASSERT_EQ(::bind(_heldSocket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
        ASSERT_EQ(::listen(_heldSocket, 1), 0);

        sockaddr_in boundAddress {};
        socklen_t boundAddressLength = sizeof(boundAddress);
        ASSERT_EQ(::getsockname(_heldSocket, reinterpret_cast<sockaddr*>(&boundAddress), &boundAddressLength), 0);
        _heldPort = ntohs(boundAddress.sin_port);
    }

    void terminate() override {
        if (_heldSocket >= 0) {
            ::close(_heldSocket);
        }
    }

    void expectStartError(const std::function<void()>& start, const std::string& expected) {
        try {
            start();
        } catch (const TuringException& e) {
            EXPECT_EQ(std::string(e.what()), expected);
            return;
        }

        ADD_FAILURE() << "start() returned on a taken port";
    }

    std::unique_ptr<TuringTestEnv> _env;
    int _heldSocket {-1};
    uint16_t _heldPort {0};
};

TEST_F(TakenPortTest, theServerDoesNotStartOnATakenPort) {
    DBServerConfig config;
    config.setAddress("127.0.0.1");
    config.setPort(_heldPort);

    TuringServer server(config, _env->getDB());

    expectStartError([&server] { server.start(); },
                     "Could not start the server on 127.0.0.1:" + std::to_string(_heldPort)
                         + ": bind: Address already in use");
}

TEST_F(TakenPortTest, theVisualizerDoesNotStartOnATakenPort) {
    VisualizerProxy proxy("127.0.0.1", _heldPort, reserveFreePort());

    expectStartError([&proxy] { proxy.start(); },
                     "Could not start the visualizer on 127.0.0.1:" + std::to_string(_heldPort)
                         + ": bind: Address already in use");
}

TEST_F(TakenPortTest, aStartedServerClosesItsPortWhenDestroyed) {
    const uint16_t port = reserveFreePort();

    DBServerConfig config;
    config.setAddress("127.0.0.1");
    config.setPort(port);

    auto server = std::make_unique<TuringServer>(config, _env->getDB());
    server->start();
    server.reset();

    EXPECT_FALSE(waitUntilListening(port, std::chrono::milliseconds(200)));
}

TEST_F(TakenPortTest, aStartedVisualizerClosesItsPortWhenDestroyed) {
    const uint16_t port = reserveFreePort();

    auto proxy = std::make_unique<VisualizerProxy>("127.0.0.1", port, reserveFreePort());
    proxy->start();
    proxy.reset();

    EXPECT_FALSE(waitUntilListening(port, std::chrono::milliseconds(200)));
}
