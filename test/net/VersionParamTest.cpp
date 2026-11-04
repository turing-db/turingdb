#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <chrono>
#include <memory>
#include <string>
#include <string_view>

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
#include "TuringProtoHeaders.h"
#include "TuringServer.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"
#include "versioning/CommitHash.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr const char* GRAPH_NAME = "simpledb";
constexpr const char* COUNT_QUERY = "MATCH (n) RETURN count(n)";
constexpr const char* SIMPLEDB_NODE_COUNT = "18";

std::string commitError(std::string_view value) {
    return fmt::format("The commit parameter '{}' is not a hexadecimal commit hash or 'head'", value);
}

std::string changeError(std::string_view value) {
    return fmt::format("The change parameter '{}' is not a decimal change ID or 'head'", value);
}

}

class VersionParamTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        Graph* graph = nullptr;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            system.createGraph("default");
            graph = system.createGraph(GRAPH_NAME);
        }

        _emptyCommit = graph->getHeadHash();
        SimpleGraph::createSimpleGraph(graph);
        _headCommit = graph->getHeadHash();

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

    void expectCount(CommitHash commit, const std::string& expectedCount) {
        net::proto::TuringClient client("127.0.0.1", std::to_string(_port), &_env->getMem());
        client.setGraphName(GRAPH_NAME);
        client.setCommitHash(commit);
        client.connect();

        ResultRows rows;
        QueryStatus status;
        runProtoQuery(&client, COUNT_QUERY, nullptr, rows, status);

        client.disconnect();

        const std::string commitText = fmt::format("{}", commit);
        ASSERT_TRUE(status.isOk()) << commitText << ": " << status.getError();
        EXPECT_EQ(rows, (ResultRows {{expectedCount}})) << commitText;
    }

    void post(const std::string& params, HttpResponse& response) {
        std::string body;
        makeProtoRequestBody(COUNT_QUERY, body);
        postHttpBody(_port, "/query?" + params, body, response);
    }

    void expectNoError(const std::string& params) {
        HttpResponse response;
        post(params, response);

        EXPECT_EQ(response.statusLine, "HTTP/1.1 200 OK") << params;

        net::proto::ProtoHeader header;
        size_t position = 0;
        while (position < response.body.size()) {
            header = net::proto::ProtoHeader::decode(response.body.data() + position,
                                                     response.body.size() - position);
            EXPECT_NE(header._type, net::proto::MessageTypes::ERROR) << params;
            EXPECT_NE(header._type, net::proto::MessageTypes::PROTOCOL_ERROR) << params;

            position += net::proto::ProtoHeader::wireSize() + header._dataLen;
        }

        EXPECT_EQ(header._type, net::proto::MessageTypes::END) << params;
    }

    void expectError(const std::string& params,
                     QueryStatus::Status expectedStatus,
                     const std::string& expectedDetails) {
        HttpResponse response;
        post(params, response);

        EXPECT_EQ(response.statusLine, "HTTP/1.1 200 OK") << params;

        const size_t headerSize = net::proto::ProtoHeader::wireSize();
        ASSERT_GE(response.body.size(), headerSize + sizeof(QueryStatus::Status)) << params;

        const net::proto::ProtoHeader header = net::proto::ProtoHeader::decode(response.body.data(),
                                                                              response.body.size());
        ASSERT_EQ(header._type, net::proto::MessageTypes::ERROR) << params;

        QueryStatus::Status status {};
        memcpy(&status, response.body.data() + headerSize, sizeof(status));
        EXPECT_EQ(status, expectedStatus) << params;

        const size_t detailsBegin = headerSize + sizeof(status);
        const std::string details = response.body.substr(detailsBegin, header._dataLen - sizeof(status));
        EXPECT_EQ(details, expectedDetails) << params;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<TuringServer> _server;
    CommitHash _emptyCommit;
    CommitHash _headCommit;
    uint16_t _port {0};
};

TEST_F(VersionParamTest, wellFormedCommitsReadTheirVersion) {
    expectCount(CommitHash::head(), SIMPLEDB_NODE_COUNT);
    expectCount(_headCommit, SIMPLEDB_NODE_COUNT);
    expectCount(_emptyCommit, "0");
}

TEST_F(VersionParamTest, absentAndHeadVersionsRunTheQuery) {
    expectNoError("graph=simpledb");
    expectNoError("graph=simpledb&commit=head&change=head");
    expectNoError(fmt::format("graph=simpledb&commit={}", _headCommit));
}

TEST_F(VersionParamTest, unparseableCommitIsRejected) {
    using enum QueryStatus::Status;

    const std::string emptyCommit = fmt::format("{}", _emptyCommit);

    expectError("graph=simpledb&commit=zzz", COMMIT_NOT_FOUND, commitError("zzz"));
    expectError("graph=simpledb&commit=xyz123", COMMIT_NOT_FOUND, commitError("xyz123"));
    expectError("graph=simpledb&commit=" + emptyCommit + "zz", COMMIT_NOT_FOUND, commitError(emptyCommit + "zz"));
    expectError("graph=simpledb&commit=" + emptyCommit + "=1", COMMIT_NOT_FOUND, commitError(emptyCommit + "=1"));
}

TEST_F(VersionParamTest, emptyCommitIsRejected) {
    using enum QueryStatus::Status;

    expectError("graph=simpledb&commit=", COMMIT_NOT_FOUND, commitError(""));
    expectError("commit=&graph=simpledb", COMMIT_NOT_FOUND, commitError(""));
    expectError("graph=simpledb&commit", COMMIT_NOT_FOUND, commitError(""));
}

TEST_F(VersionParamTest, unparseableChangeIsRejected) {
    using enum QueryStatus::Status;

    expectError("graph=simpledb&change=zzz", CHANGE_NOT_FOUND, changeError("zzz"));
    expectError("graph=simpledb&change=", CHANGE_NOT_FOUND, changeError(""));
    expectError("change=&graph=simpledb", CHANGE_NOT_FOUND, changeError(""));

    for (const std::string value : {"a", "1f", "0x10", "-1"}) {
        expectError("graph=simpledb&change=" + value, CHANGE_NOT_FOUND, changeError(value));
    }
}

TEST_F(VersionParamTest, headSuffixIsAProtocolError) {
    HttpResponse response;
    post(fmt::format("graph=simpledb&commit={}(HEAD)", _headCommit), response);

    EXPECT_EQ(response.statusLine, "HTTP/1.1 400 Bad Request");
    ASSERT_GE(response.body.size(), net::proto::ProtoHeader::wireSize());

    const net::proto::ProtoHeader header = net::proto::ProtoHeader::decode(response.body.data(),
                                                                          response.body.size());
    EXPECT_EQ(header._type, net::proto::MessageTypes::PROTOCOL_ERROR);
    EXPECT_NE(response.body.find("The request URI is invalid."), std::string::npos) << response.body;
}
