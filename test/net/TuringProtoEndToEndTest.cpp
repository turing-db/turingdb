#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "DBServerConfig.h"
#include "Graph.h"
#include "QueryConfig.h"
#include "QueryResultFormatter.h"
#include "RemoteTestUtils.h"
#include "SystemManager.h"
#include "TuringClient.h"
#include "TuringDB.h"
#include "TuringException.h"
#include "TuringServer.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"
#include "dataframe/Dataframe.h"
#include "metadata/PropertyType.h"
#include "writers/GraphWriter.h"

using namespace turing::test;

namespace {

constexpr const char* GRAPH_NAME = "proto_e2e";

struct EndToEndResult {
    std::vector<std::string> columnNames;
    std::vector<std::vector<std::string>> rows;
    size_t dataframesReceived {0};
};

// One end-to-end run: spin up a server with the requested QueryConfig and
// proto buffer capacity, seed the graph via the seeder callback, then send
// the query through a client built with the same buffer capacity (the server
// can emit a CHUNK as large as its buffer; TuringClient::recvAll truncates to
// _inBuf.capacity(), so client buffer must be at least as big as server's —
// equal is the simplest way to guarantee that).
EndToEndResult runEndToEnd(const std::string& outDir,
                           size_t queryChunkRows,
                           size_t bufferCapacity,
                           const std::function<void(db::Graph*, db::JobSystem*)>& seeder,
                           const std::string& query) {
    db::QueryConfig queryConfig;
    queryConfig.setChunkSize(queryChunkRows);

    auto env = TuringTestEnv::create(fs::Path {outDir} / "turing", queryConfig);
    env->getSystemManager().createGraph("default");
    db::Graph* graph = env->getSystemManager().createGraph(GRAPH_NAME);
    seeder(graph, env->getSystemManager().getJobSystem());

    const uint16_t port = reserveFreePort();
    db::DBServerConfig serverConfig;
    serverConfig.setAddress("127.0.0.1");
    serverConfig.setPort(port);
    serverConfig.setWorkerCount(1);
    serverConfig.setMaxConnections(16);
    serverConfig.setProtoBufferCapacity(bufferCapacity);

    ProtoEnvScope protoScope;
    db::TuringServer server(serverConfig, env->getDB());
    server.start();

    EndToEndResult result;
    bool serverStarted = true;
    auto stopServer = [&]() {
        if (serverStarted) {
            server.stop();
            server.wait();
            serverStarted = false;
        }
    };

    try {
        if (!waitUntilListening(port, std::chrono::milliseconds(2000))) {
            throw TuringException("End-to-end test server failed to listen");
        }

        net::proto::TuringClient client("127.0.0.1",
                                        std::to_string(port),
                                        &env->getMem(),
                                        bufferCapacity);
        client.setGraphName(GRAPH_NAME);
        client.connect();

        std::vector<std::string> values;
        bool headerSeen = false;
        const db::QueryStatus status =
            client.sendQuery(query, [&](const db::Dataframe* df) {
                if (!headerSeen) {
                    QueryResultFormatter::appendHeader(result.columnNames, df);
                    headerSeen = true;
                }
                QueryResultFormatter::appendRows(result.rows, values, df);
                ++result.dataframesReceived;
            });

        client.disconnect();
        stopServer();

        if (!status.isOk()) {
            throw TuringException("Remote query failed: " + std::string(status.getError()));
        }
    } catch (...) {
        stopServer();
        throw;
    }

    return result;
}

} // namespace

class TuringProtoEndToEndTest : public TuringTest {};

// chunkRows=1 forces the query pipeline to emit one Dataframe per row. Each
// server-side writeDataframe() produces its own CHUNK_HEADER + CHUNK +
// END_CHUNK group on the wire and the client fires onOutputData once per
// END_CHUNK. With 12 seeded rows we therefore expect exactly 12 client
// callbacks. Verifies the streaming path delivers many small dataframes in
// order without losing or merging rows.
TEST_F(TuringProtoEndToEndTest, ManySmallQueryChunks) {
    constexpr size_t ROW_COUNT = 12;

    auto seeder = [&](db::Graph* graph, db::JobSystem* jobSystem) {
        db::GraphWriter writer(graph, jobSystem);
        for (size_t i = 0; i < ROW_COUNT; ++i) {
            const auto node = writer.addNode({"TestLabel"});
            const std::string name = "node_" + std::to_string(i);
            writer.addNodeProperty<db::types::String>(node, "name", std::string_view(name));
        }
        ASSERT_TRUE(writer.commit());
        ASSERT_TRUE(writer.submit());
    };

    const auto result = runEndToEnd(_outDir,
                                    /*queryChunkRows=*/1,
                                    /*bufferCapacity=*/net::proto::DEFAULT_BUFFER_CAPACITY,
                                    seeder,
                                    "MATCH (n:TestLabel) RETURN n.name AS name");

    EXPECT_EQ(result.dataframesReceived, ROW_COUNT);
    EXPECT_EQ(result.rows.size(), ROW_COUNT);
    ASSERT_EQ(result.columnNames.size(), 1u);
    EXPECT_EQ(result.columnNames[0], "name");

    std::vector<std::string> received;
    received.reserve(result.rows.size());
    for (const auto& row : result.rows) {
        ASSERT_EQ(row.size(), 1u);
        received.push_back(row[0]);
    }
    std::sort(received.begin(), received.end());

    std::vector<std::string> expected;
    expected.reserve(ROW_COUNT);
    for (size_t i = 0; i < ROW_COUNT; ++i) {
        expected.push_back("node_" + std::to_string(i));
    }
    std::sort(expected.begin(), expected.end());

    EXPECT_EQ(received, expected);
}

// One row whose value is far larger than the proto buffer. The server's
// TuringProtoEncoder writes the value into _buffer; whenever it fills, the
// buffer-full callback emits a CHUNK packet and resets. So this single value
// must round-trip across many CHUNK packets that collectively encode one
// row. The decoded string must equal the source byte-for-byte.
TEST_F(TuringProtoEndToEndTest, HugeValueExceedsProtoBuffer) {
    constexpr size_t BUFFER_CAPACITY = 512;
    const std::string hugeValue(8192, 'x');

    auto seeder = [&](db::Graph* graph, db::JobSystem* jobSystem) {
        db::GraphWriter writer(graph, jobSystem);
        const auto node = writer.addNode({"TestLabel"});
        writer.addNodeProperty<db::types::String>(node, "blob", std::string_view(hugeValue));
        ASSERT_TRUE(writer.commit());
        ASSERT_TRUE(writer.submit());
    };

    const auto result = runEndToEnd(_outDir,
                                    /*queryChunkRows=*/net::proto::DEFAULT_BUFFER_CAPACITY,
                                    BUFFER_CAPACITY,
                                    seeder,
                                    "MATCH (n:TestLabel) RETURN n.blob AS blob");

    EXPECT_EQ(result.dataframesReceived, 1u);
    ASSERT_EQ(result.rows.size(), 1u);
    ASSERT_EQ(result.rows[0].size(), 1u);
    EXPECT_EQ(result.rows[0][0], hugeValue);
}

// Both effects compound: chunkRows=1 forces many dataframes (each its own
// CHUNK_HEADER / CHUNK / END_CHUNK group on the wire), and a small proto
// buffer forces each of those CHUNKs to split mid-value because the seeded
// strings exceed the buffer. Stresses dataframe-boundary AND value-mid-flush
// reassembly together.
TEST_F(TuringProtoEndToEndTest, MixedSmallChunksTinyBuffer) {
    constexpr size_t ROW_COUNT = 6;
    constexpr size_t BUFFER_CAPACITY = 512;
    constexpr size_t VALUE_SIZE = 1024;

    std::vector<std::string> values;
    values.reserve(ROW_COUNT);
    for (size_t i = 0; i < ROW_COUNT; ++i) {
        values.emplace_back(VALUE_SIZE, static_cast<char>('a' + i));
    }

    auto seeder = [&](db::Graph* graph, db::JobSystem* jobSystem) {
        db::GraphWriter writer(graph, jobSystem);
        for (size_t i = 0; i < ROW_COUNT; ++i) {
            const auto node = writer.addNode({"TestLabel"});
            writer.addNodeProperty<db::types::String>(node, "blob", std::string_view(values[i]));
        }
        ASSERT_TRUE(writer.commit());
        ASSERT_TRUE(writer.submit());
    };

    const auto result = runEndToEnd(_outDir,
                                    /*queryChunkRows=*/1,
                                    BUFFER_CAPACITY,
                                    seeder,
                                    "MATCH (n:TestLabel) RETURN n.blob AS blob");

    EXPECT_EQ(result.dataframesReceived, ROW_COUNT);
    ASSERT_EQ(result.rows.size(), ROW_COUNT);

    std::vector<std::string> received;
    received.reserve(result.rows.size());
    for (const auto& row : result.rows) {
        ASSERT_EQ(row.size(), 1u);
        received.push_back(row[0]);
    }
    std::sort(received.begin(), received.end());

    std::vector<std::string> expected = values;
    std::sort(expected.begin(), expected.end());

    EXPECT_EQ(received, expected);
}

// Many rows of a fixed-width column (Int64) where the cumulative bytes far
// exceed the proto buffer. Strings have length prefixes and are inherently
// "splittable" mid-value, but fixed-width data goes through a different
// encoder path that writes a contiguous run of identically-sized values.
// This test forces the buffer-full callback to fire repeatedly while
// emitting that run, exercising boundary handling for the fixed-width path
// (no length prefix to anchor on) — analog of HugeValueExceedsProtoBuffer
// but for the fixed-width code path.
TEST_F(TuringProtoEndToEndTest, FixedWidthValuesAcrossPackets) {
    constexpr size_t ROW_COUNT = 500;
    constexpr size_t BUFFER_CAPACITY = 256;

    auto seeder = [&](db::Graph* graph, db::JobSystem* jobSystem) {
        db::GraphWriter writer(graph, jobSystem);
        for (size_t i = 0; i < ROW_COUNT; ++i) {
            const auto node = writer.addNode({"TestLabel"});
            writer.addNodeProperty<db::types::Int64>(node,
                                                     "age",
                                                     static_cast<int64_t>(i));
        }
        ASSERT_TRUE(writer.commit());
        ASSERT_TRUE(writer.submit());
    };

    const auto result = runEndToEnd(_outDir,
                                    /*queryChunkRows=*/net::proto::DEFAULT_BUFFER_CAPACITY,
                                    BUFFER_CAPACITY,
                                    seeder,
                                    "MATCH (n:TestLabel) RETURN n.age AS age");

    ASSERT_EQ(result.rows.size(), ROW_COUNT);

    std::vector<int64_t> received;
    received.reserve(result.rows.size());
    for (const auto& row : result.rows) {
        ASSERT_EQ(row.size(), 1u);
        received.push_back(std::stoll(row[0]));
    }
    std::sort(received.begin(), received.end());

    std::vector<int64_t> expected(ROW_COUNT);
    for (size_t i = 0; i < ROW_COUNT; ++i) {
        expected[i] = static_cast<int64_t>(i);
    }
    EXPECT_EQ(received, expected);
}

// Many rows where most have no value for the property: the optional column
// emits a null mask that's much larger than the values themselves. With
// 8000 rows the mask is 1000 bytes (well above the 128-byte buffer), but
// only a handful of rows carry a non-null Int64 so the value bytes are a
// minor fraction of the wire output. The encoder must therefore split the
// mask itself across multiple CHUNK packets — this exercises the mask
// serialization path independent of the value path.
TEST_F(TuringProtoEndToEndTest, BigMaskSplitAcrossPackets) {
    constexpr size_t ROW_COUNT = 8000;
    constexpr size_t NON_NULL_COUNT = 50;
    constexpr size_t BUFFER_CAPACITY = 128;

    auto seeder = [&](db::Graph* graph, db::JobSystem* jobSystem) {
        db::GraphWriter writer(graph, jobSystem);
        for (size_t i = 0; i < ROW_COUNT; ++i) {
            const auto node = writer.addNode({"TestLabel"});
            if (i < NON_NULL_COUNT) {
                writer.addNodeProperty<db::types::Int64>(node,
                                                         "tag",
                                                         static_cast<int64_t>(i));
            }
        }
        ASSERT_TRUE(writer.commit());
        ASSERT_TRUE(writer.submit());
    };

    const auto result = runEndToEnd(_outDir,
                                    /*queryChunkRows=*/net::proto::DEFAULT_BUFFER_CAPACITY,
                                    BUFFER_CAPACITY,
                                    seeder,
                                    "MATCH (n:TestLabel) RETURN n.tag AS tag");

    ASSERT_EQ(result.rows.size(), ROW_COUNT);

    size_t nullCount = 0;
    std::vector<int64_t> nonNullValues;
    nonNullValues.reserve(NON_NULL_COUNT);
    for (const auto& row : result.rows) {
        ASSERT_EQ(row.size(), 1u);
        if (row[0] == "null") {
            ++nullCount;
        } else {
            nonNullValues.push_back(std::stoll(row[0]));
        }
    }

    EXPECT_EQ(nullCount, ROW_COUNT - NON_NULL_COUNT);
    ASSERT_EQ(nonNullValues.size(), NON_NULL_COUNT);

    std::sort(nonNullValues.begin(), nonNullValues.end());
    std::vector<int64_t> expected(NON_NULL_COUNT);
    for (size_t i = 0; i < NON_NULL_COUNT; ++i) {
        expected[i] = static_cast<int64_t>(i);
    }
    EXPECT_EQ(nonNullValues, expected);
}
