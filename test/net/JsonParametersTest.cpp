#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>

#include <nlohmann/json.hpp>

#include "DBServerConfig.h"
#include "Graph.h"
#include "RemoteTestUtils.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringServer.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

void firstColumn(const nlohmann::json& json, nlohmann::json& values) {
    values = nlohmann::json::array();
    for (const nlohmann::json& chunk : json["data"]) {
        for (const nlohmann::json& value : chunk[0]) {
            values.push_back(value);
        }
    }
}

}

class JsonParametersTest : public TuringTest {
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

        _serverConfig.setAddress("127.0.0.1");
        _serverConfig.setPort(_port);
        _serverConfig.setWorkerCount(1);
        _serverConfig.setMaxConnections(16);

        _server = std::make_unique<TuringServer>(_serverConfig, _env->getDB());
        _server->start();

        ASSERT_TRUE(waitUntilListening(_port, std::chrono::milliseconds(2000)));
    }

    void terminate() override {
        if (_server) {
            _server->stop();
            _server->wait();
        }
    }

    void postJson(const nlohmann::json& body, nlohmann::json& json) {
        HttpResponse response;
        postHttpBody(_port, "/query?graph=simpledb", body.dump(), response);
        json = nlohmann::json::parse(response.body);
    }

    void expectValues(const nlohmann::json& body, const nlohmann::json& expected) {
        nlohmann::json json;
        postJson(body, json);
        ASSERT_FALSE(json.contains("error")) << json.dump();

        nlohmann::json values;
        firstColumn(json, values);
        EXPECT_EQ(values, expected) << json.dump();
    }

    void expectError(const std::string& body, const std::string& error, const std::string& details) {
        HttpResponse response;
        postHttpBody(_port, "/query?graph=simpledb", body, response);

        const nlohmann::json json = nlohmann::json::parse(response.body);
        EXPECT_EQ(json.value("error", ""), error) << json.dump();
        EXPECT_EQ(json.value("error_details", ""), details) << json.dump();
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<TuringServer> _server;
    DBServerConfig _serverConfig;
    uint16_t _port {0};
};

TEST_F(JsonParametersTest, aStringParameterNamesTheLabel) {
    const nlohmann::json body = {
        {"query", "MATCH (n:$label) RETURN n.name ORDER BY n.name"},
        {"parameters", {{"label", "Founder"}}},
    };

    expectValues(body, {"Adam", "Remy"});
}

TEST_F(JsonParametersTest, aListParameterNamesEveryLabel) {
    const nlohmann::json body = {
        {"query", "MATCH (n:$labels) RETURN n.name ORDER BY n.name"},
        {"parameters", {{"labels", {"Interest", "Exotic"}}}},
    };

    expectValues(body, {"Eighties", "Ghosts"});
}

TEST_F(JsonParametersTest, aParameterNamesTheEdgeType) {
    const nlohmann::json body = {
        {"query", "MATCH ()-[e:$type]->() RETURN count(e)"},
        {"parameters", {{"type", "KNOWS_WELL"}}},
    };

    expectValues(body, {3});
}

TEST_F(JsonParametersTest, aBodyWithoutParametersRunsTheQuery) {
    const nlohmann::json body = {{"query", "MATCH (n) RETURN count(n)"}};

    expectValues(body, {18});
}

TEST_F(JsonParametersTest, aMissingParameterIsReported) {
    const nlohmann::json body = {{"query", "MATCH (n:$label) RETURN n"}};

    expectError(body.dump(), "EXEC_ERROR", "Parameter $label is not defined");
}

TEST_F(JsonParametersTest, aMalformedBodyIsRejected) {
    const std::string notObject = "The request body must be a JSON object";

    expectError("MATCH (n) RETURN n", "PARSE_ERROR", notObject);
    expectError("[\"MATCH (n) RETURN n\"]", "PARSE_ERROR", notObject);
    expectError("{}", "PARSE_ERROR", "The request body has no query field");
    expectError("{\"query\": 1}", "PARSE_ERROR", "The query field must be a string");
    expectError("{\"query\": \"RETURN 1\", \"params\": {}}", "PARSE_ERROR", "Unknown field 'params' in the request body");
    expectError("{\"query\": \"RETURN 1\", \"parameters\": []}", "PARSE_ERROR", "The parameters must be a JSON object");
}

TEST_F(JsonParametersTest, anIntegerBeyondInt64IsRejected) {
    expectError("{\"query\": \"RETURN 1\", \"parameters\": {\"big\": 9223372036854775808}}",
                "PARSE_ERROR",
                "Invalid value for parameter $big: Integer 9223372036854775808 exceeds the Int64 range");
}

TEST_F(JsonParametersTest, aDeeplyNestedParameterIsRead) {
    constexpr size_t DEPTH = 400000;

    const std::string body = "{\"query\": \"RETURN 1\", \"parameters\": {\"deep\": "
                           + std::string(DEPTH, '[') + std::string(DEPTH, ']') + "}}";

    HttpResponse response;
    postHttpBody(_port, "/query?graph=simpledb", body, response);

    const nlohmann::json json = nlohmann::json::parse(response.body);
    nlohmann::json values;
    firstColumn(json, values);
    EXPECT_EQ(values, nlohmann::json::array({1})) << json.dump();
}
