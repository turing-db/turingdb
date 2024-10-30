#include "gtest/gtest.h"

#include <thread>
#include <string>
#include <iostream>

#include <nlohmann/json.hpp>

#include "DBServerConfig.h"
#include "DBServer.h"
#include "PipeUtils.h"

#include "HTTPClient.h"

using namespace db;
using namespace net::HTTP;

class ServerTest : public ::testing::Test {
protected:
    HTTPClient::ResponseBuffer _buffer;
};

constexpr auto HttpServerStartSleepDelay = std::chrono::milliseconds(60);
constexpr auto MaxWaitIterations = 60000/HttpServerStartSleepDelay.count();

TEST_F(ServerTest, queryEndpointOK) {
    PipeSample sample("server_test");
    sample.createSimpleGraph();

    bool bufferQueryEmpty = false;
    bool jsonQueryEmpty = false;
    bool jsonQueryDataEmpty = false;
    bool jsonQueryNoNodes = false;

    const auto checkQuery = [&](const DBServerConfig& serverConfig) {
        HTTPClient client;

        const std::string url = serverConfig.getURL()+"/query";
        const std::string query = "SELECT * FROM n:";

        const auto res = client.fetch(url, query, _buffer);

        if (res != Status::OK) {
            return false;
        }

        // Check that the buffer is not empty
        if (_buffer._data.empty()) {
            bufferQueryEmpty = true;
            return true;
        }

        std::cout << _buffer.getString() << "\n";

        // Parse json response and check that we got data
        const auto json = nlohmann::json::parse(_buffer.getString());
        const auto dataIt = json.find("data");
        if (dataIt == json.end()) {
            jsonQueryEmpty = true;
            return true;
        }

        const auto& data = *dataIt;
        if (data.empty()) {
            jsonQueryDataEmpty = true;
            return true;
        }

        for (const auto& child : data) {
            if (!child.is_array() || child.empty()) {
                jsonQueryNoNodes = true;
                return true;
            }
        }

        return true;
    };

    /*
    const auto checkNotFound = [&](const DBServerConfig& serverConfig,
                                                   const std::string& endpoint) {
        HTTPClient client;

        const std::string url = serverConfig.getURL()+endpoint;

        const auto res = client.fetch(url, "hello", _buffer);

        return res == Status::NOT_FOUND;
    };
    */

    std::thread serverThread([&]() {
        sample.startHttpServer();
    });

    std::thread clientThread([&]() {
        // Wait for the http server to accept connections on /query
        for (size_t i = 0;; i++) {
            std::this_thread::sleep_for(HttpServerStartSleepDelay);
            if (checkQuery(sample.getServerConfig())) {
                break;
            }

            // Test fails if more than MaxWaitIterations
            ASSERT_TRUE(i < MaxWaitIterations);
        }
    });

    // Wait for client thread to terminate
    clientThread.join();

    // Terminate server thread
    sample.getServer()->stop();
    serverThread.join();

    ASSERT_FALSE(bufferQueryEmpty);
    ASSERT_FALSE(jsonQueryEmpty);
    ASSERT_FALSE(jsonQueryDataEmpty);
    ASSERT_FALSE(jsonQueryNoNodes);
}