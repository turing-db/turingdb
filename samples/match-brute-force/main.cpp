#include <stdlib.h>
#include <algorithm>
#include <map>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <argparse.hpp>
#include <spdlog/spdlog.h>

#include "TuringDB.h"
#include "TuringConfig.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"
#include "NLOutputSink.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "LocalMemory.h"
#include "reader/GraphReader.h"
#include "datapart/EdgeRecord.h"
#include "versioning/Transaction.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "columns/ColumnIDs.h"
#include "ID.h"

#include "ToolInit.h"

using namespace db;

namespace {

using Row = std::vector<uint64_t>;
using RowCounts = std::map<Row, size_t>;
using Adjacency = std::vector<std::vector<NodeID>>;

constexpr std::string_view graphName = "simpledb";

constexpr std::string_view joinBadKeysQuery =
    "MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g) RETURN a";

constexpr std::string_view doubleDiamondQuery =
    "MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g),(h),(i),(e),(h),(c)-->(j) RETURN a,c,e,g";

constexpr std::string_view doubleDiamondLimitQuery =
    "MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g),(h),(i),(e),(h),(c)-->(j) RETURN a,c,e,g LIMIT 10";

constexpr size_t doubleDiamondLimit = 10;

struct Binding {
    NodeID _a;
    NodeID _b;
    NodeID _c;
    NodeID _d;
    NodeID _e;
    NodeID _f;
    NodeID _g;
    NodeID _h;
    NodeID _i;
    NodeID _j;
};

using BoundVariable = NodeID Binding::*;

constexpr BoundVariable returnA[] = {&Binding::_a};
constexpr BoundVariable returnACEG[] = {&Binding::_a, &Binding::_c, &Binding::_e, &Binding::_g};

class NodeRowSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override;

    const std::vector<Row>& getRows() const { return _rows; }

private:
    std::vector<Row> _rows;
};

void NodeRowSink::appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) {
    for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
        Row& row = _rows.emplace_back();
        for (const Column* chunk : chunks) {
            const ColumnNodeIDs* nodeIDs = static_cast<const ColumnNodeIDs*>(chunk);
            row.push_back(nodeIDs->getRaw()[rowIndex].getValue());
        }
    }
}

void readOutNeighbours(std::vector<NodeID>& nodes, Adjacency& outNeighbours, Graph* graph) {
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    nodes.clear();
    uint64_t maxNodeID = 0;
    for (auto it = reader.scanNodes().begin(); it.isValid(); it.next()) {
        const NodeID node = it.get();
        nodes.push_back(node);
        maxNodeID = std::max(maxNodeID, node.getValue());
    }

    outNeighbours.assign(maxNodeID + 1, {});
    for (const NodeID node : nodes) {
        const ColumnNodeIDs source {node};
        for (const EdgeRecord& edge : reader.getOutEdges(&source)) {
            outNeighbours[node.getValue()].push_back(edge._otherID);
        }
    }
}

const std::vector<NodeID>& outOf(const Adjacency& outNeighbours, NodeID node) {
    return outNeighbours[node.getValue()];
}

bool hasEdge(const Adjacency& outNeighbours, NodeID source, NodeID target) {
    const std::vector<NodeID>& targets = outOf(outNeighbours, source);
    return std::find(targets.begin(), targets.end(), target) != targets.end();
}

void matchDiamond(std::vector<Binding>& bindings, std::span<const NodeID> nodes, const Adjacency& outNeighbours) {
    bindings.clear();

    for (const NodeID a : nodes) {
        for (const NodeID b : outOf(outNeighbours, a)) {
            for (const NodeID c : nodes) {
                for (const NodeID d : outOf(outNeighbours, c)) {
                    for (const NodeID e : outOf(outNeighbours, d)) {
                        for (const NodeID f : outOf(outNeighbours, a)) {
                            for (const NodeID g : outOf(outNeighbours, f)) {
                                if (hasEdge(outNeighbours, c, g)) {
                                    bindings.push_back({a, b, c, d, e, f, g});
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

// The repeated (e) and (h) patterns name variables the diamond already binds, so they add no loop.
void matchDoubleDiamond(std::vector<Binding>& bindings, std::span<const NodeID> nodes, const Adjacency& outNeighbours) {
    std::vector<Binding> diamonds;
    matchDiamond(diamonds, nodes, outNeighbours);

    bindings.clear();
    for (const Binding& diamond : diamonds) {
        for (const NodeID h : nodes) {
            for (const NodeID i : nodes) {
                for (const NodeID j : outOf(outNeighbours, diamond._c)) {
                    Binding binding = diamond;
                    binding._h = h;
                    binding._i = i;
                    binding._j = j;
                    bindings.push_back(binding);
                }
            }
        }
    }
}

void project(RowCounts& rows, std::span<const Binding> bindings, std::span<const BoundVariable> returnItems) {
    rows.clear();

    Row row;
    for (const Binding& binding : bindings) {
        row.clear();
        for (const BoundVariable variable : returnItems) {
            row.push_back((binding.*variable).getValue());
        }
        rows[row]++;
    }
}

void countRows(RowCounts& counts, std::span<const Row> rows) {
    counts.clear();
    for (const Row& row : rows) {
        counts[row]++;
    }
}

size_t totalRows(const RowCounts& counts) {
    size_t total = 0;
    for (const auto& [row, count] : counts) {
        total += count;
    }
    return total;
}

void formatRow(std::string& text, const Row& row) {
    text.clear();
    for (const uint64_t value : row) {
        if (!text.empty()) {
            text += ",";
        }
        text += std::to_string(value);
    }
}

bool runV3(std::vector<Row>& rows, QueryInterpreterV3& interpreter, LocalMemory& memory, std::string_view query) {
    NodeRowSink sink;
    QueryStatus status;
    interpreter.execute(status, query, graphName, CommitHash::head(), ChangeID::head(), &memory, &sink);
    memory.clear();

    if (!status.isOk()) {
        spdlog::error("v3 failed on '{}': {}", query, status.getError());
        return false;
    }

    rows = sink.getRows();
    return true;
}

bool compareRows(std::string_view query, const RowCounts& bruteForce, const RowCounts& v3) {
    spdlog::info("{}", query);
    spdlog::info("  brute force: {} rows, {} distinct", totalRows(bruteForce), bruteForce.size());
    spdlog::info("  v3:          {} rows, {} distinct", totalRows(v3), v3.size());

    bool matched = true;
    std::string text;

    for (const auto& [row, count] : bruteForce) {
        const auto it = v3.find(row);
        const size_t v3Count = it == v3.end() ? 0 : it->second;
        if (v3Count != count) {
            formatRow(text, row);
            spdlog::error("  row [{}]: brute force {} times, v3 {} times", text, count, v3Count);
            matched = false;
        }
    }

    for (const auto& [row, count] : v3) {
        if (!bruteForce.contains(row)) {
            formatRow(text, row);
            spdlog::error("  row [{}]: brute force 0 times, v3 {} times", text, count);
            matched = false;
        }
    }

    spdlog::info("  {}", matched ? "identical" : "DIFFERENT");
    return matched;
}

bool checkLimitedRows(std::string_view query, const RowCounts& bruteForce, std::span<const Row> v3Rows, size_t limit) {
    spdlog::info("{}", query);

    const size_t expectedCount = std::min(limit, totalRows(bruteForce));
    bool matched = v3Rows.size() == expectedCount;
    if (!matched) {
        spdlog::error("  v3 returned {} rows, the limit admits {}", v3Rows.size(), expectedCount);
    }

    std::string text;
    for (const Row& row : v3Rows) {
        if (!bruteForce.contains(row)) {
            formatRow(text, row);
            spdlog::error("  row [{}] is not a match of the pattern", text);
            matched = false;
        }
    }

    spdlog::info("  {}", matched ? "every row is a match" : "DIFFERENT");
    return matched;
}

}

int main(int argc, const char** argv) {
    ToolInit toolInit("match-brute-force");

    auto& argParser = toolInit.getArgParser();

    std::string turingDirArg;
    argParser.add_argument("-turing-dir")
             .metavar("path")
             .store_into(turingDirArg)
             .help("Root Turing directory (defaults to SAMPLE_DIR/.turing)");

    toolInit.init(argc, argv);

    fs::Path turingDir;
    if (!turingDirArg.empty()) {
        turingDir = fs::Path(turingDirArg);
        if (!turingDir.toAbsolute()) {
            spdlog::error("Failed to get absolute path of turing directory {}", turingDirArg);
            return EXIT_FAILURE;
        }
    } else {
        turingDir = fs::Path(SAMPLE_DIR) / ".turing";
    }

    TuringConfig config;
    config.setTuringDirectory(turingDir);
    config.setSyncedOnDisk(false);

    TuringDB db(&config);
    db.init();

    Graph* graph = nullptr;
    {
        SystemAccessor system = db.getSystemManager().accessUnique();
        graph = system.createGraph(graphName);
    }
    SimpleGraph::createSimpleGraph(graph);

    std::vector<NodeID> nodes;
    Adjacency outNeighbours;
    readOutNeighbours(nodes, outNeighbours, graph);

    LocalMemory memory;
    QueryInterpreterV3 interpreter(&db.getSystemManager());

    std::vector<Binding> bindings;
    RowCounts bruteForce;
    RowCounts v3;
    std::vector<Row> rows;
    bool allMatched = true;

    matchDiamond(bindings, nodes, outNeighbours);
    project(bruteForce, bindings, returnA);
    if (!runV3(rows, interpreter, memory, joinBadKeysQuery)) {
        return EXIT_FAILURE;
    }
    countRows(v3, rows);
    allMatched = compareRows(joinBadKeysQuery, bruteForce, v3) && allMatched;

    matchDoubleDiamond(bindings, nodes, outNeighbours);
    project(bruteForce, bindings, returnACEG);
    if (!runV3(rows, interpreter, memory, doubleDiamondQuery)) {
        return EXIT_FAILURE;
    }
    countRows(v3, rows);
    allMatched = compareRows(doubleDiamondQuery, bruteForce, v3) && allMatched;

    if (!runV3(rows, interpreter, memory, doubleDiamondLimitQuery)) {
        return EXIT_FAILURE;
    }
    allMatched = checkLimitedRows(doubleDiamondLimitQuery, bruteForce, rows, doubleDiamondLimit) && allMatched;

    return allMatched ? EXIT_SUCCESS : EXIT_FAILURE;
}
