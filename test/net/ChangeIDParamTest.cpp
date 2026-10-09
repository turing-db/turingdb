#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>
#include <spdlog/fmt/fmt.h>

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
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr const char* GRAPH_NAME = "simpledb";
constexpr uint64_t SIMPLEDB_NODE_COUNT = 18;
constexpr size_t CHANGE_COUNT = 17;

std::string changeError(std::string_view value) {
    return fmt::format("The change parameter '{}' is not a decimal change ID or 'head'", value);
}

void firstColumn(const nlohmann::json& json, nlohmann::json& values) {
    values = nlohmann::json::array();
    for (const nlohmann::json& chunk : json["data"]) {
        for (const nlohmann::json& value : chunk[0]) {
            values.push_back(value);
        }
    }
}

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

        _serverConfig.setAddress("127.0.0.1");
        _serverConfig.setPort(_port);
        _serverConfig.setWorkerCount(1);
        _serverConfig.setMaxConnections(16);
    }

    void terminate() override {
        if (_server) {
            _server->stop();
            _server->wait();
        }
    }

    void startServer() {
        _server = std::make_unique<TuringServer>(_serverConfig, _env->getDB());
        _server->start();

        ASSERT_TRUE(waitUntilListening(_port, std::chrono::milliseconds(2000)));
    }

    void post(const std::string& params, const std::string& query, nlohmann::json& json) {
        HttpResponse response;
        postHttpQuery(_port, "/query?graph=simpledb" + params, query, response);
        json = nlohmann::json::parse(response.body);
    }

    void run(const std::string& params, const std::string& query, nlohmann::json& json) {
        post(params, query, json);
        ASSERT_FALSE(json.contains("error")) << params << " " << query << ": " << json.dump();
    }

    void openChanges(std::vector<uint64_t>& changeIDs) {
        for (size_t index = 0; index < CHANGE_COUNT; index++) {
            nlohmann::json json;
            run("", "CHANGE NEW", json);

            nlohmann::json values;
            firstColumn(json, values);
            ASSERT_EQ(values.size(), 1u) << json.dump();
            changeIDs.push_back(values[0].get<uint64_t>());
        }
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<TuringServer> _server;
    DBServerConfig _serverConfig;
    uint16_t _port {0};
};

TEST_F(ChangeIDParamTest, theIdChangeNewReturnsSelectsThatChange) {
    startServer();

    std::vector<uint64_t> changeIDs;
    openChanges(changeIDs);

    for (const uint64_t changeID : changeIDs) {
        const std::string params = "&change=" + std::to_string(changeID);
        const std::string create = fmt::format("CREATE (:Marker {{name: 'change-{}'}})", changeID);

        nlohmann::json json;
        run(params, create, json);
        run(params, "COMMIT", json);
    }

    for (const uint64_t changeID : changeIDs) {
        nlohmann::json json;
        run("&change=" + std::to_string(changeID), "MATCH (m:Marker) RETURN m.name", json);

        nlohmann::json names;
        firstColumn(json, names);
        EXPECT_EQ(names, nlohmann::json::array({fmt::format("change-{}", changeID)})) << changeID;
    }
}

TEST_F(ChangeIDParamTest, everyIdChangeListReturnsSelectsAChange) {
    startServer();

    std::vector<uint64_t> changeIDs;
    openChanges(changeIDs);

    nlohmann::json json;
    run("", "CHANGE LIST", json);

    nlohmann::json listed;
    firstColumn(json, listed);
    ASSERT_EQ(listed.size(), CHANGE_COUNT) << json.dump();

    for (const nlohmann::json& changeID : listed) {
        nlohmann::json countJson;
        run("&change=" + std::to_string(changeID.get<uint64_t>()), "MATCH (n) RETURN count(n)", countJson);

        nlohmann::json counts;
        firstColumn(countJson, counts);
        EXPECT_EQ(counts, nlohmann::json::array({SIMPLEDB_NODE_COUNT})) << changeID;
    }
}

TEST_F(ChangeIDParamTest, aChangeIdThatIsNotDecimalIsRejected) {
    startServer();

    std::vector<uint64_t> changeIDs;
    openChanges(changeIDs);

    for (const std::string value : {"a", "1f", "0x10", "-1"}) {
        nlohmann::json json;
        post("&change=" + value, "MATCH (n) RETURN count(n)", json);

        EXPECT_EQ(json["error"], "CHANGE_NOT_FOUND") << value << ": " << json.dump();
        EXPECT_EQ(json["error_details"], changeError(value)) << value;
        EXPECT_FALSE(json.contains("data")) << value;
    }
}

TEST_F(ChangeIDParamTest, aChangeIdThatDoesNotExistIsNamed) {
    startServer();

    std::vector<uint64_t> changeIDs;
    openChanges(changeIDs);

    const std::string missing = std::to_string(changeIDs.back() + 1);

    nlohmann::json json;
    post("&change=" + missing, "MATCH (n) RETURN count(n)", json);

    EXPECT_EQ(json["error"], "CHANGE_NOT_FOUND") << json.dump();
    EXPECT_EQ(json["error_details"], fmt::format("Change '{}' does not exist", missing));
    EXPECT_FALSE(json.contains("data"));
}

TEST_F(ChangeIDParamTest, theBinaryClientSendsTheChangeIdItIsGiven) {
    ProtoEnvScope protoScope;
    startServer();

    std::vector<ChangeID> changeIDs;
    {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        for (size_t index = 0; index < CHANGE_COUNT; index++) {
            const auto change = system.newChange(GRAPH_NAME);
            ASSERT_TRUE(change);
            changeIDs.push_back(change.value()->id());
        }
    }

    net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
    client.setGraphName(GRAPH_NAME);
    client.connect();

    const auto ignoreOutput = [](const Dataframe* dataframe) {};

    for (const ChangeID changeID : changeIDs) {
        client.setChangeID(changeID);

        const QueryStatus createStatus = client.sendQuery(fmt::format("CREATE (:Marker {{name: 'change-{}'}})", changeID),
                                                          ignoreOutput);
        ASSERT_TRUE(createStatus.isOk()) << changeID << ": " << createStatus.getError();

        const QueryStatus commitStatus = client.sendQuery("COMMIT", ignoreOutput);
        ASSERT_TRUE(commitStatus.isOk()) << changeID << ": " << commitStatus.getError();
    }

    for (const ChangeID changeID : changeIDs) {
        client.setChangeID(changeID);

        size_t rowCount = 0;
        const QueryStatus status = client.sendQuery(fmt::format("MATCH (m:Marker) WHERE m.name = 'change-{}' RETURN m", changeID),
                                                    [&rowCount](const Dataframe* dataframe) {
                                                        rowCount += dataframe->getLogicalRowCount();
                                                    });
        ASSERT_TRUE(status.isOk()) << changeID << ": " << status.getError();
        EXPECT_EQ(rowCount, 1u) << changeID;
    }

    client.disconnect();
}
