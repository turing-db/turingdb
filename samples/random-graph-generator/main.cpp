#include <stdlib.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_set>

#include <argparse.hpp>
#include <spdlog/spdlog.h>
#include <spdlog/fmt/bundled/format.h>

#include "Graph.h"
#include "JobSystem.h"
#include "SystemManager.h"
#include "TuringConfig.h"
#include "TuringDB.h"
#include "ToolInit.h"
#include "metadata/PropertyType.h"
#include "writers/GraphWriter.h"

using namespace db;

namespace {

class SplitMix64 {
public:
    explicit SplitMix64(uint64_t seed)
        : _state(seed) {
    }

    uint64_t next() {
        uint64_t z = (_state += 0x9E3779B97F4A7C15ULL);
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
        return z ^ (z >> 31);
    }

private:
    uint64_t _state;
};

std::string makeRandomName(SplitMix64& rng, size_t length) {
    static constexpr std::string_view alphabet =
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789";

    std::string name(length, 'a');
    for (size_t i = 0; i < length; ++i) {
        name[i] = alphabet[rng.next() % alphabet.size()];
    }
    return name;
}

std::string formatUInt128(unsigned __int128 value) {
    if (value == 0) {
        return "0";
    }

    std::string out;
    while (value > 0) {
        out.push_back(static_cast<char>('0' + (value % 10)));
        value /= 10;
    }
    std::reverse(out.begin(), out.end());
    return out;
}

bool commitOrExit(GraphWriter& writer, const char* phase, uint64_t progressCount) {
    if (writer.commit()) {
        return true;
    }

    spdlog::error("Failed to commit during {} at progress {}", phase, progressCount);
    return false;
}

}

int main(int argc, const char** argv) {
    ToolInit toolInit("random-graph-generator");
    toolInit.disableOutputDir();

    std::string graphName = "random_graph";
    std::string turingDirArg;
    uint64_t numNodes = 100000;
    uint64_t degree = 4;
    uint64_t batchSize = 100000;
    uint64_t seed = 1;
    size_t nameLength = 16;
    bool allowSelfLoops = false;
    bool inMemory = false;
    bool mergeDataParts = false;

    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("-graph-name")
             .metavar("name")
             .store_into(graphName)
             .help("Graph name (default: random_graph)");
    argParser.add_argument("-turing-dir")
             .metavar("path")
             .store_into(turingDirArg)
             .help("Root Turing directory (default: SAMPLE_DIR/.turing)");
    argParser.add_argument("-nodes")
             .metavar("N")
             .store_into(numNodes)
             .help("Number of nodes to create (default: 100000)");
    argParser.add_argument("-degree")
             .metavar("D")
             .store_into(degree)
             .help("Random outgoing edges per node (default: 4)");
    argParser.add_argument("-batch-size")
             .metavar("N")
             .store_into(batchSize)
             .help("Commit every N node or edge writes (default: 100000)");
    argParser.add_argument("-seed")
             .metavar("N")
             .store_into(seed)
             .help("Random seed (default: 1)");
    argParser.add_argument("-name-length")
             .metavar("N")
             .store_into(nameLength)
             .help("Random name length (default: 16)");
    argParser.add_argument("-allow-self-loops")
             .default_value(false)
             .implicit_value(true)
             .store_into(allowSelfLoops)
             .help("Allow edges from a node to itself");
    argParser.add_argument("-in-memory")
             .default_value(false)
             .implicit_value(true)
             .store_into(inMemory)
             .help("Disable on-disk sync and keep generation in memory");
    argParser.add_argument("-merge-dataparts")
             .default_value(false)
             .implicit_value(true)
             .store_into(mergeDataParts)
             .help("Merge all dataparts into a single datapartment after generation");

    toolInit.init(argc, argv);

    if (numNodes > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        spdlog::error("Node count {} exceeds supported int64 id property range", numNodes);
        return EXIT_FAILURE;
    }

    if (batchSize == 0) {
        spdlog::error("Batch size must be greater than 0");
        return EXIT_FAILURE;
    }

    if (nameLength == 0) {
        spdlog::error("Name length must be greater than 0");
        return EXIT_FAILURE;
    }

    const uint64_t maxTargetsPerNode = allowSelfLoops
        ? numNodes
        : (numNodes == 0 ? 0 : numNodes - 1);
    if (degree > maxTargetsPerNode) {
        spdlog::error(
            "Requested degree {} is impossible for {} nodes when self loops are {}",
            degree,
            numNodes,
            allowSelfLoops ? "enabled" : "disabled");
        return EXIT_FAILURE;
    }

    fs::Path turingDir;
    if (!turingDirArg.empty()) {
        turingDir = fs::Path(turingDirArg);
        if (!turingDir.toAbsolute()) {
            spdlog::error("Failed to resolve turing directory {}", turingDirArg);
            return EXIT_FAILURE;
        }
    } else {
        turingDir = fs::Path(SAMPLE_DIR) / ".turing";
    }

    if (turingDir.exists()) {
        turingDir.rm();
    }

    TuringConfig config;
    config.setTuringDirectory(turingDir);
    config.setSyncedOnDisk(!inMemory);

    TuringDB db(&config);
    db.init();

    Graph* graph = db.getSystemManager().createGraph(graphName);
    if (!graph) {
        spdlog::error("Failed to create graph {}", graphName);
        return EXIT_FAILURE;
    }

    GraphWriter writer(graph);
    const PropertyType idProperty = writer.addPropertyType("id", types::Int64::_valueType);
    const PropertyType nameProperty = writer.addPropertyType("name", types::String::_valueType);

    SplitMix64 rng(seed);
    const auto startTime = std::chrono::steady_clock::now();

    spdlog::info(
        "Generating graph '{}' in {} with {} nodes and degree {} (batch size {}, seed {}, sync-to-disk {})",
        graphName,
        turingDir.get(),
        numNodes,
        degree,
        batchSize,
        seed,
        !inMemory);

    uint64_t pendingNodes = 0;
    for (uint64_t i = 0; i < numNodes; ++i) {
        const NodeID node = writer.addNode({"Node"});
        if (node.getValue() != i) {
            spdlog::error("Expected sequential NodeID {}, got {}", i, node.getValue());
            return EXIT_FAILURE;
        }

        writer.addNodeProperty<types::Int64>(node, idProperty, static_cast<int64_t>(i));
        writer.addNodeProperty<types::String>(node, nameProperty, makeRandomName(rng, nameLength));

        ++pendingNodes;
        if (pendingNodes == batchSize) {
            if (!commitOrExit(writer, "node creation", i + 1)) {
                return EXIT_FAILURE;
            }
            spdlog::info("Created {} / {} nodes", i + 1, numNodes);
            pendingNodes = 0;
        }
    }

    if (pendingNodes > 0) {
        if (!commitOrExit(writer, "node creation", numNodes)) {
            return EXIT_FAILURE;
        }
    }

    const unsigned __int128 totalEdgeTarget =
        static_cast<unsigned __int128>(numNodes) * static_cast<unsigned __int128>(degree);

    uint64_t pendingEdges = 0;
    unsigned __int128 createdEdges = 0;
    std::unordered_set<uint64_t> targets;
    targets.reserve(std::max<uint64_t>(degree * 2, 16));

    for (uint64_t src = 0; src < numNodes; ++src) {
        targets.clear();
        while (targets.size() < degree) {
            const uint64_t tgt = rng.next() % numNodes;
            if (!allowSelfLoops && tgt == src) {
                continue;
            }
            targets.insert(tgt);
        }

        const NodeID srcID(src);
        for (uint64_t tgt : targets) {
            writer.addEdge("CONNECTED_TO", srcID, NodeID(tgt));
            ++pendingEdges;
            ++createdEdges;

            if (pendingEdges == batchSize) {
                if (!commitOrExit(writer, "edge creation", static_cast<uint64_t>(createdEdges))) {
                    return EXIT_FAILURE;
                }
                spdlog::info("Created {} / {} edges",
                             formatUInt128(createdEdges),
                             formatUInt128(totalEdgeTarget));
                pendingEdges = 0;
            }
        }
    }

    if (pendingEdges > 0) {
        if (!commitOrExit(writer, "edge creation", static_cast<uint64_t>(createdEdges))) {
            return EXIT_FAILURE;
        }
    }

    const auto endTime = std::chrono::steady_clock::now();
    const auto elapsedMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();

    if (!writer.submit()) {
        spdlog::error("Failed to submit generated graph '{}'", graphName);
        return EXIT_FAILURE;
    }

    if (mergeDataParts) {
        auto jobSystem = JobSystem::create();
        const auto mergeRes = db.getSystemManager().mergeDataParts(graph, *jobSystem);
        if (!mergeRes) {
            spdlog::error("Failed to merge dataparts for graph '{}': {}", graphName, mergeRes.error().fmtMessage());
            return EXIT_FAILURE;
        }
    }

    if (!inMemory) {
        const auto dumpRes = db.getSystemManager().dumpGraph(graphName);
        if (!dumpRes) {
            spdlog::error("Failed to dump generated graph '{}': {}", graphName, dumpRes.error().fmtMessage());
            return EXIT_FAILURE;
        }
    }

    spdlog::info(
        "Finished graph '{}' with {} nodes and {} edges in {} ms",
        graphName,
        numNodes,
        formatUInt128(createdEdges),
        elapsedMs);

    return EXIT_SUCCESS;
}
