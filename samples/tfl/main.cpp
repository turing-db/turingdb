#include <stdlib.h>
#include <algorithm>
#include <filesystem>
#include <map>
#include <set>
#include <span>
#include <string>
#include <vector>

#include <argparse.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/fmt/bundled/format.h>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "TuringConfig.h"
#include "SystemManager.h"
#include "Graph.h"
#include "LocalMemory.h"
#include "NLOutputSink.h"
#include "columns/ColumnVector.h"
#include "versioning/Change.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "GraphPath.h"

#include "ToolInit.h"

using namespace db;

namespace {

struct Connection {
    std::string station1;
    std::string station2;
    std::string line;
    double _time = 0.0;
};

using StringColumn = ColumnVector<std::string>;

class ConnectionNLSink : public NLOutputSink {
public:
    ConnectionNLSink(std::vector<Connection>& connections, std::set<std::string>& stations)
        : _connections(connections),
        _stations(stations)
    {
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        const StringColumn* station1Column = static_cast<const StringColumn*>(chunks[0]);
        const StringColumn* station2Column = static_cast<const StringColumn*>(chunks[1]);
        const StringColumn* lineColumn = static_cast<const StringColumn*>(chunks[2]);
        const StringColumn* timeColumn = static_cast<const StringColumn*>(chunks[3]);

        for (size_t row = offset; row < offset + rowCount; row++) {
            _connections.push_back({
                (*station1Column)[row],
                (*station2Column)[row],
                (*lineColumn)[row],
                std::stod((*timeColumn)[row]),
            });

            _stations.insert((*station1Column)[row]);
            _stations.insert((*station2Column)[row]);
        }
    }

private:
    std::vector<Connection>& _connections;
    std::set<std::string>& _stations;
};

class ShortestPathNLSink : public NLOutputSink {
public:
    ShortestPathNLSink(double& distance, Path& path)
        : _distance(distance),
        _path(path)
    {
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        if (rowCount == 0) {
            return;
        }

        _distance = (*static_cast<const ColumnVector<double>*>(chunks[0]))[offset];
        _path = (*static_cast<const ColumnVector<Path>*>(chunks[1]))[offset];
    }

private:
    double& _distance;
    Path& _path;
};

}

int main(int argc, const char** argv) {
    ToolInit toolInit("tfl");
    toolInit.disableOutputDir();

    std::string fromStation = "Stratford";
    std::string toStation = "Bank";
    double penalty = 4.0;

    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("-from")
             .metavar("station")
             .store_into(fromStation)
             .help("Source station (default: Stratford)");
    argParser.add_argument("-to")
             .metavar("station")
             .store_into(toStation)
             .help("Destination station (default: Bank)");
    argParser.add_argument("-penalty")
             .metavar("minutes")
             .store_into(penalty)
             .help("Transfer penalty in minutes"
                   " (default: 4)");

    toolInit.init(argc, argv);

    fs::Path turingDir = fs::Path(SAMPLE_DIR) / ".turing";
    if (turingDir.exists()) {
        turingDir.rm();
    }

    TuringConfig config;
    config.setTuringDirectory(turingDir);
    config.setSyncedOnDisk(false);

    TuringDB db(&config);
    LocalMemory mem;
    db.init();

    const std::string graphName = "tfl";
    {
        SystemAccessor system = db.getSystemManager().accessUnique();
        system.createGraph(graphName);
    }

    // ---------------------------------------------------------------
    // Step 1: Load connections.csv using LOAD CSV
    // ---------------------------------------------------------------
    // Copy CSV into the data directory so LOAD CSV can find it
    fs::Path srcCsv = fs::Path(SAMPLE_DIR) / "connections.csv";
    fs::Path dstCsv = config.getDataDir() / "connections.csv";
    std::filesystem::copy_file(srcCsv.get(), dstCsv.get());

    const std::string loadQuery =
        "LOAD CSV 'connections.csv' WITH HEADERS AS row "
        "RETURN row.station1 AS s1, row.station2 AS s2, "
        "row.line AS line, row.time AS time";

    std::vector<Connection> connections;
    std::set<std::string> stationSet;

    QueryConfig queryConfig;

    const auto runQuery = [&](std::string_view q,
                              NLOutputSink* sink,
                              ChangeID chg = ChangeID::head()) {
        const QueryState state(graphName, &mem, &queryConfig, sink, CommitHash::head(), chg);
        return db.query(q, state);
    };

    ConnectionNLSink connectionSink(connections, stationSet);
    const auto status = runQuery(loadQuery, &connectionSink);

    if (!status.isOk()) {
        spdlog::error("LOAD CSV failed: {}", status.getError());
        return EXIT_FAILURE;
    }

    fmt::print("Loaded {} connections between {} stations\n",
               connections.size(), stationSet.size());

    // ---------------------------------------------------------------
    // Step 2: Build expanded graph with transfer penalties
    // ---------------------------------------------------------------
    if (!stationSet.contains(fromStation)) {
        spdlog::error("Unknown station: {}", fromStation);
        return EXIT_FAILURE;
    }
    if (!stationSet.contains(toStation)) {
        spdlog::error("Unknown station: {}", toStation);
        return EXIT_FAILURE;
    }

    // Map each station to its set of lines (excluding Walk)
    std::map<std::string, std::set<std::string>> stationLines;
    for (const auto& conn : connections) {
        if (conn.line == "Walk") continue;
        stationLines[conn.station1].insert(conn.line);
        stationLines[conn.station2].insert(conn.line);
    }

    // Platform nodes: one per (station, line) pair
    using Platform = std::pair<std::string, std::string>;
    std::map<Platform, std::string> platformVar;
    size_t idx = 0;
    for (const auto& [station, lines] : stationLines) {
        for (const auto& line : lines) {
            platformVar[{station, line}] =
                "p" + std::to_string(idx++);
        }
    }

    std::string createQuery = "CREATE ";
    size_t edgeCount = 0;

    // Platform nodes
    bool first = true;
    for (const auto& [key, var] : platformVar) {
        if (!first) createQuery += ",\n";
        createQuery += fmt::format(
            "({}:Station {{name: \"{}\"}})",
            var, key.first);
        first = false;
    }

    // Virtual hub nodes for single-result shortest path
    createQuery +=
        ",\n(__src:Station {name: \"__src\"})"
        ",\n(__dst:Station {name: \"__dst\"})";

    // Connection edges (non-Walk, bidirectional)
    for (const auto& conn : connections) {
        if (conn.line == "Walk") continue;
        const auto& v1 =
            platformVar[{conn.station1, conn.line}];
        const auto& v2 =
            platformVar[{conn.station2, conn.line}];
        createQuery += fmt::format(
            ",\n({})-[:CONNECTED_TO {{time: {:.1f},"
            " line: \"{}\"}}]->({})"
            ",\n({})-[:CONNECTED_TO {{time: {:.1f},"
            " line: \"{}\"}}]->({})",
            v1, conn._time, conn.line, v2,
            v2, conn._time, conn.line, v1);
        edgeCount += 2;
    }

    // Transfer edges (different lines at same station)
    for (const auto& [station, lines] : stationLines) {
        std::vector<std::string> lineVec(
            lines.begin(), lines.end());
        for (size_t i = 0; i < lineVec.size(); i++) {
            for (size_t j = i + 1; j < lineVec.size(); j++) {
                const auto& v1 =
                    platformVar[{station, lineVec[i]}];
                const auto& v2 =
                    platformVar[{station, lineVec[j]}];
                createQuery += fmt::format(
                    ",\n({})-[:CONNECTED_TO"
                    " {{time: {:.1f},"
                    " line: \"Walk\"}}]->({})"
                    ",\n({})-[:CONNECTED_TO"
                    " {{time: {:.1f},"
                    " line: \"Walk\"}}]->({})",
                    v1, penalty, v2,
                    v2, penalty, v1);
                edgeCount += 2;
            }
        }
    }

    // Walk edges (all platforms of s1 <-> all platforms of s2)
    for (const auto& conn : connections) {
        if (conn.line != "Walk") continue;
        double walkWeight = conn._time + penalty;
        for (const auto& l1 :
             stationLines[conn.station1]) {
            for (const auto& l2 :
                 stationLines[conn.station2]) {
                const auto& v1 =
                    platformVar[{conn.station1, l1}];
                const auto& v2 =
                    platformVar[{conn.station2, l2}];
                createQuery += fmt::format(
                    ",\n({})-[:CONNECTED_TO"
                    " {{time: {:.1f},"
                    " line: \"Walk\"}}]->({})"
                    ",\n({})-[:CONNECTED_TO"
                    " {{time: {:.1f},"
                    " line: \"Walk\"}}]->({})",
                    v1, walkWeight, v2,
                    v2, walkWeight, v1);
                edgeCount += 2;
            }
        }
    }

    // Hub edges: __src -> source platforms
    for (const auto& line : stationLines[fromStation]) {
        const auto& v = platformVar[{fromStation, line}];
        createQuery += fmt::format(
            ",\n(__src)-[:CONNECTED_TO"
            " {{time: 0.0, line: \"Walk\"}}]->({})", v);
        edgeCount++;
    }
    // Hub edges: dest platforms -> __dst
    for (const auto& line : stationLines[toStation]) {
        const auto& v = platformVar[{toStation, line}];
        createQuery += fmt::format(
            ",\n({})-[:CONNECTED_TO"
            " {{time: 0.0, line: \"Walk\"}}]->(__dst)", v);
        edgeCount++;
    }

    // ---------------------------------------------------------------
    // Step 3: Create change, execute CREATE, submit
    // ---------------------------------------------------------------
    ChangeID changeID = ChangeID::head();
    {
        SystemAccessor system = db.getSystemManager().accessUnique();
        auto changeRes = system.newChange(graphName);
        if (!changeRes) {
            spdlog::error("Failed to create change");
            return EXIT_FAILURE;
        }
        Change* change = changeRes.value();
        changeID = change->id();
    }

    const auto createStatus = runQuery(createQuery, nullptr, changeID);
    if (!createStatus.isOk()) {
        spdlog::error("CREATE failed: {}", createStatus.getError());
        return EXIT_FAILURE;
    }

    const auto submitStatus = runQuery("CHANGE SUBMIT", nullptr, changeID);
    if (!submitStatus.isOk()) {
        spdlog::error("CHANGE SUBMIT failed: {}",
                      submitStatus.getError());
        return EXIT_FAILURE;
    }

    fmt::print("Created {} platform nodes and {} edges"
               " (penalty: {} min)\n",
               platformVar.size(), edgeCount, penalty);

    // ---------------------------------------------------------------
    // Step 4: Shortest path
    // ---------------------------------------------------------------
    const std::string spQuery =
        "MATCH (a:Station {name: \"__src\"}), "
        "(b:Station {name: \"__dst\"}) "
        "SHORTESTPATH(a, b, time, dist, path) "
        "RETURN dist, path";

    double distance = 0;
    Path pathResult;

    ShortestPathNLSink shortestPathSink(distance, pathResult);
    const auto spStatus = runQuery(spQuery, &shortestPathSink);

    if (!spStatus.isOk()) {
        spdlog::error("SHORTESTPATH failed: {}",
                      spStatus.getError());
        return EXIT_FAILURE;
    }

    // Resolve node IDs in the path to station names
    SystemAccessor system = db.getSystemManager().accessShared();
    auto txRes = system.openTransaction(
        graphName, CommitHash::head(), ChangeID::head());
    if (!txRes) {
        spdlog::error("Failed to open transaction");
        return EXIT_FAILURE;
    }
    Transaction& tx = txRes.value();
    GraphReader reader = tx.readGraph();

    auto namePropOpt =
        reader.getMetadata().propTypes().get("name");
    if (!namePropOpt) {
        spdlog::error("Property 'name' not found");
        return EXIT_FAILURE;
    }
    PropertyTypeID namePropID = namePropOpt.value()._id;

    auto linePropOpt =
        reader.getMetadata().propTypes().get("line");
    if (!linePropOpt) {
        spdlog::error("Property 'line' not found");
        return EXIT_FAILURE;
    }
    PropertyTypeID linePropID = linePropOpt.value()._id;

    // Path is [target, edge, node, edge, node, ...] reversed
    // Even indices are nodes, odd indices are edges
    std::vector<std::string> stops;
    std::vector<std::string> edgeLines;
    for (size_t i = 0; i < pathResult.size(); i++) {
        if (i % 2 == 0) {
            NodeID nodeID(pathResult[i].getValue());
            const auto* name =
                reader.tryGetNodeProperty<types::String>(
                    namePropID, nodeID);
            if (name) {
                stops.emplace_back(*name);
            }
        } else {
            EdgeID edgeID(pathResult[i].getValue());
            const auto* line =
                reader.tryGetEdgeProperty<types::String>(
                    linePropID, edgeID);
            if (line) {
                edgeLines.emplace_back(*line);
            }
        }
    }

    // Reverse: path goes target->source, we want source->target
    std::reverse(stops.begin(), stops.end());
    std::reverse(edgeLines.begin(), edgeLines.end());

    // Strip virtual hub nodes (__src at front, __dst at back)
    if (stops.size() >= 2 && stops.front() == "__src") {
        stops.erase(stops.begin());
        if (!edgeLines.empty()) {
            edgeLines.erase(edgeLines.begin());
        }
    }
    if (stops.size() >= 2 && stops.back() == "__dst") {
        stops.pop_back();
        if (!edgeLines.empty()) {
            edgeLines.pop_back();
        }
    }

    // Collapse consecutive duplicate station names (transfers
    // through platform nodes produce e.g. Bank, Bank)
    std::vector<std::string> finalStops;
    std::vector<std::string> finalEdges;
    for (size_t i = 0; i < stops.size(); i++) {
        if (i > 0 && stops[i] == stops[i - 1]) {
            continue;
        }
        finalStops.push_back(stops[i]);
        if (i > 0) {
            finalEdges.push_back(edgeLines[i - 1]);
        }
    }

    fmt::print("\nShortest path: {} -> {}\n",
               fromStation, toStation);
    fmt::print("  Distance: {} minutes\n", distance);
    fmt::print("  Stops: {}\n", finalStops.size());
    fmt::print("  Route:\n");

    // finalEdges[i] is the line between finalStops[i] and
    // finalStops[i+1]. Show each station under its line header,
    // print a new header when the line changes.
    for (size_t i = 0; i < finalStops.size(); i++) {
        if (i == 0 && !finalEdges.empty()) {
            fmt::print("    [{}]\n", finalEdges[0]);
        }
        fmt::print("      {}\n", finalStops[i]);
        if (i > 0 && i < finalEdges.size()
            && finalEdges[i] != finalEdges[i - 1]) {
            fmt::print("    [{}]\n", finalEdges[i]);
        }
    }

    return EXIT_SUCCESS;
}
