#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <chrono>
#include <memory>
#include <string>

#include "DBServerConfig.h"
#include "Graph.h"
#include "QueryStatus.h"
#include "RemoteTestUtils.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringClient.h"
#include "TuringServer.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"
#include "dataframe/Dataframe.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr const char* GRAPH_NAME = "simpledb";

struct ReceivedFrames {
    size_t frameCount {0};
    size_t rowCount {0};
    size_t columnCount {0};
};

}

class TuringProtoEmptyResultTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        Graph* graph = nullptr;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            system.createGraph("default");
            graph = system.createGraph(GRAPH_NAME);
        }
        SimpleGraph::createSimpleGraph(graph);

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

    void runQuery(const std::string& query, ReceivedFrames& received) {
        net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
        client.setGraphName(GRAPH_NAME);
        client.connect();

        const QueryStatus status = client.sendQuery(query, [&received](const Dataframe* dataframe) {
            received.frameCount++;
            received.rowCount += dataframe->getLogicalRowCount();
            received.columnCount = dataframe->size();
        });

        client.disconnect();

        ASSERT_TRUE(status.isOk()) << status.getError();
    }

    ProtoEnvScope _protoScope;
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<TuringServer> _server;
    uint16_t _port {0};
};

TEST_F(TuringProtoEmptyResultTest, NullLiteralNextToNode) {
    ReceivedFrames received;
    runQuery("MATCH (n) WHERE n.name = 'Nobody' RETURN n, null AS nothing", received);

    EXPECT_EQ(received.frameCount, 1u);
    EXPECT_EQ(received.columnCount, 2u);
    EXPECT_EQ(received.rowCount, 0u);
}

TEST_F(TuringProtoEmptyResultTest, NullLiteralAlone) {
    ReceivedFrames received;
    runQuery("MATCH (n) WHERE n.name = 'Nobody' RETURN null AS nothing", received);

    EXPECT_EQ(received.frameCount, 1u);
    EXPECT_EQ(received.columnCount, 1u);
    EXPECT_EQ(received.rowCount, 0u);
}

TEST_F(TuringProtoEmptyResultTest, MissingPropertyNextToNode) {
    ReceivedFrames received;
    runQuery("MATCH (n) WHERE n.name = 'Nobody' RETURN n, n.doesNotExist", received);

    EXPECT_EQ(received.frameCount, 1u);
    EXPECT_EQ(received.columnCount, 2u);
    EXPECT_EQ(received.rowCount, 0u);
}

TEST_F(TuringProtoEmptyResultTest, NullLiteralWithRows) {
    ReceivedFrames received;
    runQuery("MATCH (n) WHERE n.name = 'Remy' RETURN n, null AS nothing", received);

    EXPECT_EQ(received.columnCount, 2u);
    EXPECT_EQ(received.rowCount, 1u);
}
