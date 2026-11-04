#include <gtest/gtest.h>

#include <stddef.h>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "DBServerConfig.h"
#include "Graph.h"
#include "ProtoTestUtils.h"
#include "QueryStatus.h"
#include "RemoteTestUtils.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringClient.h"
#include "TuringServer.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr const char* GRAPH_NAME = "simpledb";
constexpr const char* SIMPLEDB_NODE_COUNT = "18";
constexpr size_t CHANGE_COUNT = 17;

}

TEST(ChangeIDTextTest, changeIdIsDecimal) {
    const auto ten = ChangeID::fromString("10");
    ASSERT_TRUE(ten);
    EXPECT_EQ(ten.value().get(), 10u);

    EXPECT_FALSE(ChangeID::fromString("a"));
    EXPECT_FALSE(ChangeID::fromString("1f"));
    EXPECT_EQ(ChangeID::fromString("head").value(), ChangeID::head());

    std::string text;
    ChangeID {16}.appendString(text);
    EXPECT_EQ(text, "16");

    text.clear();
    ChangeID::head().appendString(text);
    EXPECT_EQ(text, "head");
}

TEST(ChangeIDTextTest, commitHashStaysHexadecimal) {
    const auto hash = CommitHash::fromString("10");
    ASSERT_TRUE(hash);
    EXPECT_EQ(hash.value().get(), 16u);

    std::string text;
    CommitHash {0xabcdef}.appendString(text);
    EXPECT_EQ(text, "abcdef");

    text.clear();
    CommitHash::head().appendString(text);
    EXPECT_EQ(text, "head");
}

class ChangeIDParamTest : public TuringTest {
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

        _client = std::make_unique<net::proto::TuringClient>("127.0.0.1", std::to_string(_port), &_env->getMem());
        _client->setGraphName(GRAPH_NAME);
        _client->connect();
    }

    void terminate() override {
        if (_client) {
            _client->disconnect();
        }

        if (_server) {
            _server->stop();
            _server->wait();
        }
    }

    void post(ChangeID changeID, const std::string& query, ResultRows& rows, QueryStatus& status) {
        _client->setChangeID(changeID);

        runProtoQuery(_client.get(), query, nullptr, rows, status);
    }

    void run(ChangeID changeID, const std::string& query, ResultRows& rows) {
        QueryStatus status;
        post(changeID, query, rows, status);
        ASSERT_TRUE(status.isOk()) << changeID << " " << query << ": " << status.getError();
    }

    void readChangeIDs(const ResultRows& rows, std::vector<ChangeID>& changeIDs) {
        for (const std::vector<std::string>& row : rows) {
            ASSERT_EQ(row.size(), 1u);

            const auto changeID = ChangeID::fromString(row[0]);
            ASSERT_TRUE(changeID) << row[0];
            changeIDs.push_back(changeID.value());
        }
    }

    void openChanges(std::vector<ChangeID>& changeIDs) {
        for (size_t index = 0; index < CHANGE_COUNT; index++) {
            ResultRows rows;
            run(ChangeID::head(), "CHANGE NEW", rows);
            ASSERT_EQ(rows.size(), 1u);
            readChangeIDs(rows, changeIDs);
        }
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<TuringServer> _server;
    std::unique_ptr<net::proto::TuringClient> _client;
    uint16_t _port {0};
};

TEST_F(ChangeIDParamTest, theIdChangeNewReturnsSelectsThatChange) {
    std::vector<ChangeID> changeIDs;
    openChanges(changeIDs);

    for (const ChangeID changeID : changeIDs) {
        ResultRows rows;
        run(changeID, fmt::format("CREATE (:Marker {{name: 'change-{}'}})", changeID), rows);
        run(changeID, "COMMIT", rows);
    }

    for (const ChangeID changeID : changeIDs) {
        ResultRows names;
        run(changeID, "MATCH (m:Marker) RETURN m.name", names);
        EXPECT_EQ(names, (ResultRows {{fmt::format("change-{}", changeID)}})) << changeID;
    }
}

TEST_F(ChangeIDParamTest, everyIdChangeListReturnsSelectsAChange) {
    std::vector<ChangeID> opened;
    openChanges(opened);

    ResultRows listed;
    run(ChangeID::head(), "CHANGE LIST", listed);

    std::vector<ChangeID> changeIDs;
    readChangeIDs(listed, changeIDs);
    ASSERT_EQ(changeIDs.size(), CHANGE_COUNT);

    for (const ChangeID changeID : changeIDs) {
        ResultRows counts;
        run(changeID, "MATCH (n) RETURN count(n)", counts);
        EXPECT_EQ(counts, (ResultRows {{SIMPLEDB_NODE_COUNT}})) << changeID;
    }
}

TEST_F(ChangeIDParamTest, aChangeIdThatDoesNotExistIsNamed) {
    std::vector<ChangeID> changeIDs;
    openChanges(changeIDs);

    const ChangeID missing {changeIDs.back().get() + 1};

    ResultRows rows;
    QueryStatus status;
    post(missing, "MATCH (n) RETURN count(n)", rows, status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::CHANGE_NOT_FOUND);
    EXPECT_EQ(status.getError(), fmt::format("Change '{}' does not exist", missing));
    EXPECT_TRUE(rows.empty());
}
