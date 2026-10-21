#include <gtest/gtest.h>

#include <stdint.h>
#include <chrono>
#include <string>

#include "LocalMemory.h"
#include "RemoteTestUtils.h"
#include "TuringAsyncClient.h"
#include "TuringException.h"

using namespace db;
using namespace turing::test;

TEST(TuringAsyncClientConnectTest, aRefusedConnectionIsAConnectError) {
    LocalMemory localMem;
    const std::string port = std::to_string(reserveFreePort());
    net::proto::TuringAsyncClient client("127.0.0.1", port, &localMem);

    std::string message;
    try {
        client.connect();
        client.sendQuery("RETURN 1", [](const Dataframe* dataframe) {});

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!client.isSendComplete() && std::chrono::steady_clock::now() < deadline) {
            client.send();
        }
    } catch (const TuringException& e) {
        message = e.what();
    }

    EXPECT_TRUE(message.starts_with("Failed to connect to 127.0.0.1:" + port)) << message;
}
