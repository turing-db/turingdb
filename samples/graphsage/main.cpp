#include <stdlib.h>
#include <algorithm>
#include <array>
#include <charconv>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <argparse.hpp>
#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>

#include "Graph.h"
#include "JobSystem.h"
#include "SimpleGraph.h"
#include "columns/ColumnIDs.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "samplers/GraphSAGESampler.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "ToolInit.h"

using namespace db;

namespace {

using NodeCol = GraphSAGESampler::NodeCol;
using Fanouts = GraphSAGESampler::Fanouts;
using NodeNames = std::unordered_map<uint64_t, std::string_view>;

constexpr size_t hops = GraphSAGESampler::hops;

void collectNodeNames(const GraphReader& reader, NodeNames& names) {
    const PropertyTypeID namePropertyID {0};

    auto it = reader.scanNodeProperties<types::String>(namePropertyID).begin();
    for (; it.isValid(); it.next()) {
        names.emplace(it.getCurrentNodeID().getValue(), it.get());
    }
}

void splitList(std::string_view list, std::vector<std::string>& parts) {
    size_t start = 0;

    while (true) {
        const size_t comma = list.find(',', start);
        const size_t end = (comma == std::string_view::npos) ? list.size() : comma;
        const std::string_view part = list.substr(start, end - start);

        if (!part.empty()) {
            parts.emplace_back(part);
        }
        if (comma == std::string_view::npos) {
            return;
        }

        start = comma + 1;
    }
}

bool parseSize(std::string_view text, size_t& value) {
    const char* const begin = text.data();
    const char* const end = text.data() + text.size();
    const std::from_chars_result result = std::from_chars(begin, end, value);

    return result.ec == std::errc {} && result.ptr == end;
}

void formatNode(std::string& out, const std::optional<NodeID>& node, const NodeNames& names) {
    if (!node.has_value()) {
        out = "null";
        return;
    }

    const uint64_t value = node->getValue();
    const NodeNames::const_iterator it = names.find(value);
    const std::string_view name = (it == names.cend()) ? "?" : it->second;

    out = fmt::format("{}({})", name, value);
}

void printNodeColumn(std::string_view title, const NodeCol& column, const NodeNames& names) {
    fmt::print("  {} [{} rows]\n", title, column.size());

    std::string node;
    for (size_t row = 0; row < column.size(); row++) {
        formatNode(node, column[row], names);
        fmt::print("    {:>4}  {}\n", row, node);
    }
}

void printEdgeColumns(const NodeCol& srcs, const NodeCol& tgts, const NodeNames& names) {
    const size_t rows = std::max(srcs.size(), tgts.size());
    fmt::print("  srcs [{} rows] / tgts [{} rows]\n", srcs.size(), tgts.size());

    std::string src;
    std::string tgt;
    for (size_t row = 0; row < rows; row++) {
        const std::optional<NodeID> srcNode = (row < srcs.size()) ? srcs[row] : std::optional<NodeID> {};
        const std::optional<NodeID> tgtNode = (row < tgts.size()) ? tgts[row] : std::optional<NodeID> {};

        formatNode(src, srcNode, names);
        formatNode(tgt, tgtNode, names);
        fmt::print("    {:>4}  {:<18} -> {}\n", row, src, tgt);
    }
}

}

int main(int argc, const char** argv) {
    ToolInit toolInit("graphsage");
    toolInit.disableOutputDir();

    std::string seedList = "0";
    std::string fanoutList = "2,2,2";

    auto& argParser = toolInit.getArgParser();
    argParser.add_argument("--seeds", "-s")
             .metavar("ids")
             .store_into(seedList)
             .help(fmt::format("Comma separated node IDs to seed hop 0 (default: {})", seedList));
    argParser.add_argument("--fanouts", "-f")
             .metavar("n,n,n")
             .store_into(fanoutList)
             .help(fmt::format("Comma separated neighbourhood sample size per hop (default: {})", fanoutList));

    toolInit.init(argc, argv);

    std::vector<std::string> fanoutParts;
    splitList(fanoutList, fanoutParts);

    if (fanoutParts.size() != hops) {
        spdlog::error("-fanouts takes exactly {} values, got {}", hops, fanoutParts.size());
        return EXIT_FAILURE;
    }

    Fanouts fanouts {};
    for (size_t hop = 0; hop < hops; hop++) {
        if (!parseSize(fanoutParts[hop], fanouts[hop])) {
            spdlog::error("Invalid fanout: {}", fanoutParts[hop]);
            return EXIT_FAILURE;
        }
    }

    std::vector<std::string> seedParts;
    splitList(seedList, seedParts);

    if (seedParts.empty()) {
        spdlog::error("--seeds takes at least one node ID");
        return EXIT_FAILURE;
    }

    ColumnNodeIDs seeds;
    for (const std::string& seedPart : seedParts) {
        size_t seed = 0;
        if (!parseSize(seedPart, seed)) {
            spdlog::error("Invalid node ID: {}", seedPart);
            return EXIT_FAILURE;
        }

        seeds.push_back(NodeID(seed));
    }

    JobSystem jobSystem;
    jobSystem.init();

    const std::unique_ptr<Graph> graph = Graph::create();
    SimpleGraph::createSimpleGraph(graph.get());

    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();
    const GraphView& view = reader.getView();

    NodeNames names;
    collectNodeNames(reader, names);

    std::array<NodeCol, hops> srcs;
    std::array<NodeCol, hops> tgts;
    std::array<NodeCol, hops> dstNodes;

    GraphSAGESampler sampler(&view);
    for (size_t hop = 0; hop < hops; hop++) {
        sampler.setHopData(hop, &srcs[hop], &tgts[hop], &dstNodes[hop], fanouts[hop]);
    }

    sampler.sample(&seeds);

    fmt::print("\nseeds: {}\nfanouts: {}\n", seedList, fanoutList);

    for (size_t hop = 0; hop < hops; hop++) {
        fmt::print("\n=== hop {} (fanout {}) ===\n", hop, fanouts[hop]);
        printNodeColumn("dstNodes", dstNodes[hop], names);
        printEdgeColumns(srcs[hop], tgts[hop], names);
    }

    fmt::print("\n");

    jobSystem.terminate();

    return EXIT_SUCCESS;
}
