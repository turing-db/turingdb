#include <stdlib.h>
#include <algorithm>
#include <map>
#include <set>
#include <string>
#include <vector>

#include <argparse.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/fmt/bundled/format.h>

#include "TuringDB.h"
#include "TuringConfig.h"
#include "SystemManager.h"
#include "Graph.h"
#include "LocalMemory.h"
#include "dataframe/Dataframe.h"
#include "columns/ColumnVector.h"
#include "versioning/Change.h"
#include "versioning/Transaction.h"
#include "reader/GraphReader.h"
#include "GraphPath.h"

#include "ToolInit.h"

using namespace db;

int main(int argc, const char** argv) {
    ToolInit toolInit("tfl");
    toolInit.disableOutputDir();

    std::string fromStation = "Stratford";
    std::string toStation = "Bank";

    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("-from")
             .metavar("station")
             .store_into(fromStation)
             .help("Source station (default: Stratford)");
    argParser.add_argument("-to")
             .metavar("station")
             .store_into(toStation)
             .help("Destination station (default: Bank)");

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
    db.getSystemManager().createGraph(graphName);

    // ---------------------------------------------------------------
    // Step 1: Load connections.csv using LOAD CSV
    // ---------------------------------------------------------------
    fs::Path csvPath = fs::Path(SAMPLE_DIR) / "connections.csv";

    const std::string loadQuery =
        "LOAD CSV '" + std::string(csvPath.get())
        + "' WITH HEADERS AS row "
        "RETURN row.station1 AS s1, row.station2 AS s2, "
        "row.line AS line, row.time AS time";

    struct Connection {
        std::string station1;
        std::string station2;
        std::string line;
        double time;
    };

    std::vector<Connection> connections;
    std::set<std::string> stationSet;

    auto status = db.query(loadQuery, graphName, &mem,
        [&](const Dataframe* df) {
            using StringCol = ColumnVector<std::string>;

            auto* s1Col = df->cols()[0]->as<StringCol>();
            auto* s2Col = df->cols()[1]->as<StringCol>();
            auto* lineCol = df->cols()[2]->as<StringCol>();
            auto* timeCol = df->cols()[3]->as<StringCol>();

            for (size_t i = 0; i < s1Col->size(); i++) {
                connections.push_back({
                    std::string((*s1Col)[i]),
                    std::string((*s2Col)[i]),
                    std::string((*lineCol)[i]),
                    std::stod(std::string((*timeCol)[i])),
                });
                stationSet.insert(std::string((*s1Col)[i]));
                stationSet.insert(std::string((*s2Col)[i]));
            }
        });

    if (!status.isOk()) {
        spdlog::error("LOAD CSV failed: {}", status.getError());
        return EXIT_FAILURE;
    }

    fmt::print("Loaded {} connections between {} stations\n",
               connections.size(), stationSet.size());

    // ---------------------------------------------------------------
    // Step 2: Build CREATE query for all stations and connections
    // ---------------------------------------------------------------
    std::map<std::string, std::string> stationVar;
    size_t idx = 0;
    for (const auto& name : stationSet) {
        stationVar[name] = "st" + std::to_string(idx++);
    }

    std::string createQuery = "CREATE ";

    // Station nodes
    bool first = true;
    for (const auto& [name, var] : stationVar) {
        if (!first) createQuery += ",\n";
        // Use double quotes so apostrophes in names are safe
        createQuery += fmt::format("({}:Station {{name: \"{}\"}})",
                                   var, name);
        first = false;
    }

    // Bidirectional edges (tube connections are undirected)
    // Use .1f to ensure double literal (not integer) in Cypher
    for (const auto& conn : connections) {
        createQuery += fmt::format(
            ",\n({})-[:CONNECTED_TO {{time: {:.1f},"
            " line: \"{}\"}}]->({})"
            ",\n({})-[:CONNECTED_TO {{time: {:.1f},"
            " line: \"{}\"}}]->({})",
            stationVar[conn.station1], conn.time,
            conn.line, stationVar[conn.station2],
            stationVar[conn.station2], conn.time,
            conn.line, stationVar[conn.station1]);
    }

    // ---------------------------------------------------------------
    // Step 3: Create change, execute CREATE, submit
    // ---------------------------------------------------------------
    auto changeRes = db.getSystemManager().newChange(graphName);
    if (!changeRes) {
        spdlog::error("Failed to create change");
        return EXIT_FAILURE;
    }
    Change* change = changeRes.value();

    status = db.query(createQuery, graphName, &mem,
                      CommitHash::head(), change->id());
    if (!status.isOk()) {
        spdlog::error("CREATE failed: {}", status.getError());
        return EXIT_FAILURE;
    }

    status = db.query("CHANGE SUBMIT", graphName, &mem,
                      CommitHash::head(), change->id());
    if (!status.isOk()) {
        spdlog::error("CHANGE SUBMIT failed: {}", status.getError());
        return EXIT_FAILURE;
    }

    fmt::print("Created {} stations and {} edges\n",
               stationSet.size(), connections.size() * 2);

    // ---------------------------------------------------------------
    // Step 4: Shortest path
    // ---------------------------------------------------------------
    if (!stationSet.contains(fromStation)) {
        spdlog::error("Unknown station: {}", fromStation);
        return EXIT_FAILURE;
    }
    if (!stationSet.contains(toStation)) {
        spdlog::error("Unknown station: {}", toStation);
        return EXIT_FAILURE;
    }

    const std::string spQuery =
        "MATCH (a:Station {name: \"" + fromStation + "\"}), "
        "(b:Station {name: \"" + toStation + "\"}) "
        "SHORTESTPATH(a, b, time, dist, path) "
        "RETURN dist, path";

    double distance = 0;
    Path pathResult;

    status = db.query(spQuery, graphName, &mem,
        [&](const Dataframe* df) {
            auto* distCol =
                df->cols()[0]->as<ColumnVector<double>>();
            auto* pathCol =
                df->cols()[1]->as<ColumnVector<Path>>();

            if (distCol && distCol->size() > 0) {
                distance = (*distCol)[0];
            }
            if (pathCol && pathCol->size() > 0) {
                pathResult = (*pathCol)[0];
            }
        });

    if (!status.isOk()) {
        spdlog::error("SHORTESTPATH failed: {}", status.getError());
        return EXIT_FAILURE;
    }

    // Resolve node IDs in the path to station names
    auto txRes = db.getSystemManager().openTransaction(
        graphName, CommitHash::head(), ChangeID::head());
    if (!txRes) {
        spdlog::error("Failed to open transaction");
        return EXIT_FAILURE;
    }
    Transaction& tx = txRes.value();
    GraphReader reader = tx.readGraph();

    auto namePropOpt = reader.getMetadata().propTypes().get("name");
    if (!namePropOpt) {
        spdlog::error("Property 'name' not found");
        return EXIT_FAILURE;
    }
    PropertyTypeID namePropID = namePropOpt.value()._id;

    auto linePropOpt = reader.getMetadata().propTypes().get("line");
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

    fmt::print("\nShortest path: {} -> {}\n", fromStation, toStation);
    fmt::print("  Distance: {} minutes\n", distance);
    fmt::print("  Stops: {}\n", stops.size());
    fmt::print("  Route:\n");

    // edgeLines[i] is the line between stops[i] and stops[i+1]
    for (size_t i = 0; i < stops.size(); i++) {
        bool lineChange = (i < edgeLines.size())
            && (i == 0 || edgeLines[i] != edgeLines[i - 1]);
        if (lineChange) {
            fmt::print("    [{}]\n", edgeLines[i]);
        }
        fmt::print("      {}\n", stops[i]);
    }

    return EXIT_SUCCESS;
}
