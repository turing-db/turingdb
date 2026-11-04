#include <gtest/gtest.h>

#include <memory>

#include "DBServerConfig.h"
#include "RemoteTestUtils.h"
#include "TuringException.h"
#include "TuringProtoHeaders.h"
#include "TuringServer.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A binary protocol server whose proto buffer exceeds MAX_PAYLOAD_SIZE would send packets no
// client accepts, so it refuses to start rather than fail on every connection
class ServerBufferLimitTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
    }

    std::unique_ptr<TuringTestEnv> _env;
};

TEST_F(ServerBufferLimitTest, refusesToStartWithABufferAboveTheMaximum) {
    DBServerConfig config;
    config.setAddress("127.0.0.1");
    config.setPort(reserveFreePort());
    config.setWorkerCount(1);
    config.setProtoBufferCapacity(net::proto::MAX_PAYLOAD_SIZE + 1);

    TuringServer server(config, _env->getDB());

    bool refused = false;
    try {
        server.start();
    } catch (const TuringException&) {
        refused = true;
    }

    if (!refused) {
        server.stop();
        server.wait();
    }

    EXPECT_TRUE(refused);
}
