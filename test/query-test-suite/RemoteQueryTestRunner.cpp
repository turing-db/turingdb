#include "RemoteQueryTestRunner.h"

#include <algorithm>
#include <chrono>

#include <spdlog/fmt/bundled/format.h>

#include "DBServerConfig.h"
#include "Graph.h"
#include "QueryConfig.h"
#include "QueryResultFormatter.h"
#include "QueryTestRunner.h"
#include "RemoteTestUtils.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "TuringClient.h"
#include "TuringDB.h"
#include "TuringServer.h"
#include "TuringTestEnv.h"
#include "TuringTime.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"

namespace turing::test {

QueryTestResult RemoteQueryTestRunner::runTest(const QueryTestSpec& spec,
                                               const fs::Path& outDir) {
    QueryTestResult result;
    result._name = spec._name;

    db::QueryConfig queryConfig;
    const bool forceVHJ =
        std::find(spec._tags.begin(), spec._tags.end(), "value-hash-join") != spec._tags.end();
    if (forceVHJ) {
        queryConfig.getPlanGenConfig().setForceValueHashJoin(true);
    }

    auto env = turing::test::TuringTestEnv::create(outDir, queryConfig);
    db::Graph* graph = nullptr;
    {
        db::SystemAccessor system = env->getSystemManager().accessUnique();
        graph = system.createGraph(spec._graphName);
    }
    db::SimpleGraph::createSimpleGraph(graph);
    db::TuringDB* db = &env->getDB();

    const uint16_t port = reserveFreePort();
    db::DBServerConfig serverConfig;
    serverConfig.setAddress("127.0.0.1");
    serverConfig.setPort(port);
    serverConfig.setWorkerCount(1);
    serverConfig.setMaxConnections(16);

    db::TuringServer server(serverConfig, *db);

    ProtoEnvScope protoScope;

    bool serverStarted = false;
    auto stopServer = [&]() {
        if (serverStarted) {
            server.stop();
            server.wait();
            serverStarted = false;
        }
    };

    try {
        server.start();
        serverStarted = true;

        if (!waitUntilListening(port, std::chrono::milliseconds(2000))) {
            throw FatalException(fmt::format("Remote test server failed to listen on port {}", port));
        }

        net::proto::TuringClient client("127.0.0.1", std::to_string(port), &env->getMem());
        client.setGraphName(spec._graphName);
        client.connect();

        if (spec._writeRequired) {
            // Remote writes follow the same explicit change lifecycle as a real
            // client session, so tests opt in case by case.
            db::ChangeID changeID = db::ChangeID::head();
            const db::QueryStatus changeStatus =
                client.sendQuery("CHANGE NEW", [&](const db::Dataframe* df) {
                    bioassert(df != nullptr, "CHANGE NEW returned null dataframe");
                    bioassert(df->cols().size() == 1, "CHANGE NEW should return a single column");

                    const auto* changeIDs = df->cols().front()->as<db::ColumnVector<db::ChangeID>>();
                    bioassert(changeIDs != nullptr, "CHANGE NEW should return a ChangeID vector");
                    bioassert(changeIDs->size() == 1, "CHANGE NEW should return exactly one row");

                    changeID = (*changeIDs)[0];
                });

            if (!changeStatus.isOk()) {
                throw FatalException(fmt::format("CHANGE NEW failed for remote test '{}': {}",
                                                 spec._name,
                                                 changeStatus.getError()));
            }

            client.setChangeID(changeID);
        }

        std::vector<std::string> columnNames;
        std::vector<std::vector<std::string>> rows;
        std::vector<std::string> values;
        bool headerWritten = false;
        const auto queryStart = Clock::now();
        const db::QueryStatus status = client.sendQuery(spec._query, [&](const db::Dataframe* df) {
            if (!headerWritten) {
                QueryResultFormatter::appendHeader(columnNames, df);
                headerWritten = true;
            }
            QueryResultFormatter::appendRows(rows, values, df);
        });
        const auto queryEnd = Clock::now();

        if (spec._writeRequired) {
            const db::QueryStatus submitStatus =
                client.sendQuery("CHANGE SUBMIT", [](const db::Dataframe*) {});
            if (!submitStatus.isOk()) {
                throw FatalException(fmt::format("CHANGE SUBMIT failed for remote test '{}': {}",
                                                 spec._name,
                                                 submitStatus.getError()));
            }
            client.setChangeID(db::ChangeID::head());
        }

        client.disconnect();

        result._timeUs = static_cast<uint64_t>(duration<Microseconds>(queryStart, queryEnd));
        QueryTestRunner::normalizeOutput(
            result._resultOutput,
            QueryResultFormatter::formatResultOutput(status, columnNames, rows));
        result._planMatched = true;

        std::string expected;
        QueryTestRunner::normalizeOutput(expected, spec._expectResult);
        result._resultMatched = expected == result._resultOutput;

        stopServer();
    } catch (...) {
        stopServer();
        throw;
    }

    return result;
}

}
