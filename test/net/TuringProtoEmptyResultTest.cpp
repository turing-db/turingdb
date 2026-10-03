#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <chrono>
#include <memory>
#include <string>

#include "DBServerConfig.h"
#include "Graph.h"
#include "QueryStatus.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringClient.h"
#include "TuringServer.h"
#include "dataframe/Dataframe.h"

#include "RemoteTestUtils.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

struct ReceivedResult {
    size_t dataframeCount {0};
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
            graph = system.createGraph("simpledb");
        }
        SimpleGraph::createSimpleGraph(graph);

        _port = reserveFreePort();

        DBServerConfig serverConfig;
        serverConfig.setAddress("127.0.0.1");
        serverConfig.setPort(_port);
        serverConfig.setWorkerCount(1);
        serverConfig.setMaxConnections(16);

        ProtoEnvScope protoScope;
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

    void runQuery(const std::string& query, ReceivedResult& result) {
        net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
        client.setGraphName("simpledb");
        client.connect();

        const QueryStatus status = client.sendQuery(query, [&result](const Dataframe* dataframe) {
            result.dataframeCount++;
            result.rowCount += dataframe->getLogicalRowCount();
            result.columnCount = dataframe->size();
        });

        client.disconnect();

        ASSERT_TRUE(status.isOk()) << status.getError();
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<TuringServer> _server;
    uint16_t _port {0};
};

TEST_F(TuringProtoEmptyResultTest, missingPropertyNextToNode) {
    ReceivedResult result;
    runQuery("MATCH (n) WHERE n.name = 'Nobody' RETURN n, n.doesNotExist", result);

    EXPECT_EQ(result.dataframeCount, 1);
    EXPECT_EQ(result.columnCount, 2);
    EXPECT_EQ(result.rowCount, 0);
}

TEST_F(TuringProtoEmptyResultTest, missingPropertyAlone) {
    ReceivedResult result;
    runQuery("MATCH (n) WHERE n.name = 'Nobody' RETURN n.doesNotExist", result);

    EXPECT_EQ(result.dataframeCount, 1);
    EXPECT_EQ(result.columnCount, 1);
    EXPECT_EQ(result.rowCount, 0);
}

TEST_F(TuringProtoEmptyResultTest, nullLiteralNextToNode) {
    ReceivedResult result;
    runQuery("MATCH (n) WHERE n.name = 'Nobody' RETURN n, null AS nothing", result);

    EXPECT_EQ(result.dataframeCount, 1);
    EXPECT_EQ(result.columnCount, 2);
    EXPECT_EQ(result.rowCount, 0);
}

TEST_F(TuringProtoEmptyResultTest, nullLiteralWithRows) {
    ReceivedResult result;
    runQuery("MATCH (n) WHERE n.name = 'Remy' RETURN n, null AS nothing", result);

    EXPECT_EQ(result.columnCount, 2);
    EXPECT_EQ(result.rowCount, 1);
}
