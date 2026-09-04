#include <stdlib.h>
#include <algorithm>
#include <map>
#include <optional>
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
#include "metadata/PropertyType.h"
#include "versioning/Transaction.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "ID.h"

#include "ToolInit.h"
#include "TuringException.h"

using namespace db;

namespace {

using Row = std::vector<std::string>;
using RowCounts = std::map<Row, size_t>;

constexpr std::string_view graphName = "simpledb";

struct MatchGraph {
    std::vector<NodeID> _nodes;
    std::vector<std::vector<NodeID>> _outNeighbours;
    std::vector<std::string> _names;
};

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
    NodeID _x;
};

using BoundVariable = NodeID Binding::*;
using MatchFunction = void (*)(std::vector<Binding>&, const MatchGraph&);
using WherePredicate = bool (*)(const Binding&, const MatchGraph&);

struct ReturnItem {
    BoundVariable _variable {nullptr};
    bool _name {false};
};

struct SuiteCase {
    std::string_view _test;
    std::string_view _query;
    MatchFunction _match {nullptr};
    WherePredicate _where {nullptr};
    std::span<const ReturnItem> _returnItems;
    size_t _limit {0};
};

class TextRowSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override;

    const std::vector<Row>& getRows() const { return _rows; }

private:
    std::vector<Row> _rows;

    static void cellText(std::string& text, const Column* chunk, size_t rowIndex);
};

void TextRowSink::appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) {
    for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
        Row& row = _rows.emplace_back();
        for (const Column* chunk : chunks) {
            cellText(row.emplace_back(), chunk, rowIndex);
        }
    }
}

void TextRowSink::cellText(std::string& text, const Column* chunk, size_t rowIndex) {
    const ColumnKind::Code kind = chunk->getKind();

    if (kind == ColumnNodeIDs::staticKind()) {
        const ColumnNodeIDs* nodes = static_cast<const ColumnNodeIDs*>(chunk);
        text = std::to_string(nodes->getRaw()[rowIndex].getValue());
    } else if (kind == ColumnVector<std::string_view>::staticKind()) {
        const ColumnVector<std::string_view>* strings = static_cast<const ColumnVector<std::string_view>*>(chunk);
        text = std::string(strings->getRaw()[rowIndex]);
    } else if (kind == ColumnOptVector<std::string_view>::staticKind()) {
        const ColumnOptVector<std::string_view>* strings = static_cast<const ColumnOptVector<std::string_view>*>(chunk);
        const std::optional<std::string_view>& value = strings->getRaw()[rowIndex];
        text = value ? std::string(*value) : "null";
    } else {
        throw TuringException("The sample cannot read a column of type " + std::string(chunk->getTypeName()));
    }
}

void readMatchGraph(MatchGraph& matchGraph, Graph* graph) {
    const FrozenCommitTx transaction = graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    const GraphReader reader = transaction.readGraph();

    matchGraph._nodes.clear();
    uint64_t maxNodeID = 0;
    for (auto it = reader.scanNodes().begin(); it.isValid(); it.next()) {
        const NodeID node = it.get();
        matchGraph._nodes.push_back(node);
        maxNodeID = std::max(maxNodeID, node.getValue());
    }

    matchGraph._outNeighbours.assign(maxNodeID + 1, {});
    for (const NodeID node : matchGraph._nodes) {
        const ColumnNodeIDs source {node};
        for (const EdgeRecord& edge : reader.getOutEdges(&source)) {
            matchGraph._outNeighbours[node.getValue()].push_back(edge._otherID);
        }
    }

    matchGraph._names.assign(maxNodeID + 1, "");
    const PropertyType nameType = view.metadata().propTypes().get("name").value();
    for (auto it = reader.scanNodeProperties<types::String>(nameType._id).begin(); it.isValid(); ++it) {
        matchGraph._names[it.getCurrentNodeID().getValue()] = std::string(it.get());
    }
}

const std::vector<NodeID>& outOf(const MatchGraph& graph, NodeID node) {
    return graph._outNeighbours[node.getValue()];
}

const std::string& nameOf(const MatchGraph& graph, NodeID node) {
    return graph._names[node.getValue()];
}

bool hasEdge(const MatchGraph& graph, NodeID source, NodeID target) {
    const std::vector<NodeID>& targets = outOf(graph, source);
    return std::find(targets.begin(), targets.end(), target) != targets.end();
}

// Extends every binding with one more node having an edge into its x, for a further (v)-->(x) pattern.
void addNodeIntoX(std::vector<Binding>& bindings, const MatchGraph& graph, BoundVariable variable) {
    std::vector<Binding> partials;
    partials.swap(bindings);

    for (const Binding& partial : partials) {
        for (const NodeID node : graph._nodes) {
            if (hasEdge(graph, node, partial._x)) {
                Binding binding = partial;
                binding.*variable = node;
                bindings.push_back(binding);
            }
        }
    }
}

void matchTwoIntoX(std::vector<Binding>& bindings, const MatchGraph& graph) {
    bindings.clear();

    for (const NodeID a : graph._nodes) {
        for (const NodeID x : outOf(graph, a)) {
            Binding binding;
            binding._a = a;
            binding._x = x;
            bindings.push_back(binding);
        }
    }

    addNodeIntoX(bindings, graph, &Binding::_b);
}

void matchThreeIntoX(std::vector<Binding>& bindings, const MatchGraph& graph) {
    matchTwoIntoX(bindings, graph);
    addNodeIntoX(bindings, graph, &Binding::_c);
}

void matchFourIntoX(std::vector<Binding>& bindings, const MatchGraph& graph) {
    matchThreeIntoX(bindings, graph);
    addNodeIntoX(bindings, graph, &Binding::_d);
}

void matchFiveIntoX(std::vector<Binding>& bindings, const MatchGraph& graph) {
    matchFourIntoX(bindings, graph);
    addNodeIntoX(bindings, graph, &Binding::_e);
}

void matchTwoIntoXTwoOutToE(std::vector<Binding>& bindings, const MatchGraph& graph) {
    std::vector<Binding> intoX;
    matchTwoIntoX(intoX, graph);

    bindings.clear();
    for (const Binding& partial : intoX) {
        for (const NodeID c : outOf(graph, partial._x)) {
            for (const NodeID e : outOf(graph, c)) {
                for (const NodeID d : outOf(graph, partial._x)) {
                    if (hasEdge(graph, d, e)) {
                        Binding binding = partial;
                        binding._c = c;
                        binding._d = d;
                        binding._e = e;
                        bindings.push_back(binding);
                    }
                }
            }
        }
    }
}

void matchTwoHopWalk(std::vector<Binding>& bindings, const MatchGraph& graph) {
    bindings.clear();

    for (const NodeID b : graph._nodes) {
        for (const NodeID c : outOf(graph, b)) {
            for (const NodeID a : graph._nodes) {
                if (hasEdge(graph, a, b)) {
                    Binding binding;
                    binding._a = a;
                    binding._b = b;
                    binding._c = c;
                    bindings.push_back(binding);
                }
            }
        }
    }
}

void matchDiamond(std::vector<Binding>& bindings, const MatchGraph& graph) {
    bindings.clear();

    for (const NodeID a : graph._nodes) {
        for (const NodeID b : outOf(graph, a)) {
            for (const NodeID c : graph._nodes) {
                for (const NodeID d : outOf(graph, c)) {
                    for (const NodeID e : outOf(graph, d)) {
                        for (const NodeID f : outOf(graph, a)) {
                            for (const NodeID g : outOf(graph, f)) {
                                if (hasEdge(graph, c, g)) {
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
void matchDoubleDiamond(std::vector<Binding>& bindings, const MatchGraph& graph) {
    std::vector<Binding> diamonds;
    matchDiamond(diamonds, graph);

    bindings.clear();
    for (const Binding& diamond : diamonds) {
        for (const NodeID h : graph._nodes) {
            for (const NodeID i : graph._nodes) {
                for (const NodeID j : outOf(graph, diamond._c)) {
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

bool namesPairUp(const Binding& binding, const MatchGraph& graph) {
    const bool aIsB = nameOf(graph, binding._a) == nameOf(graph, binding._b);
    const bool bIsC = nameOf(graph, binding._b) == nameOf(graph, binding._c);
    return aIsB || bIsC;
}

bool namesPairUpOrBIsX(const Binding& binding, const MatchGraph& graph) {
    const bool bIsX = nameOf(graph, binding._b) == nameOf(graph, binding._x);
    return namesPairUp(binding, graph) || bIsX;
}

void filter(std::vector<Binding>& bindings, const MatchGraph& graph, WherePredicate where) {
    std::erase_if(bindings, [&](const Binding& binding) { return !where(binding, graph); });
}

void project(RowCounts& rows, std::span<const Binding> bindings, std::span<const ReturnItem> returnItems, const MatchGraph& graph) {
    rows.clear();

    Row row;
    for (const Binding& binding : bindings) {
        row.clear();
        for (const ReturnItem& item : returnItems) {
            const NodeID node = binding.*item._variable;
            row.push_back(item._name ? nameOf(graph, node) : std::to_string(node.getValue()));
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
    for (const std::string& value : row) {
        if (!text.empty()) {
            text += ",";
        }
        text += value;
    }
}

bool runV3(std::vector<Row>& rows, QueryInterpreterV3& interpreter, LocalMemory& memory, std::string_view query) {
    TextRowSink sink;
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

bool compareRows(const RowCounts& bruteForce, const RowCounts& v3) {
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

bool checkLimitedRows(const RowCounts& bruteForce, std::span<const Row> v3Rows, size_t limit) {
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

constexpr ReturnItem returnA[] = {{&Binding::_a, false}};
constexpr ReturnItem returnABC[] = {{&Binding::_a, false}, {&Binding::_b, false}, {&Binding::_c, false}};
constexpr ReturnItem returnACEG[] = {{&Binding::_a, false},
                                     {&Binding::_c, false},
                                     {&Binding::_e, false},
                                     {&Binding::_g, false}};
constexpr ReturnItem returnNamesABX[] = {{&Binding::_a, true}, {&Binding::_b, true}, {&Binding::_x, true}};
constexpr ReturnItem returnNamesABC[] = {{&Binding::_a, true}, {&Binding::_b, true}, {&Binding::_c, true}};
constexpr ReturnItem returnNamesABCX[] = {{&Binding::_a, true},
                                          {&Binding::_b, true},
                                          {&Binding::_c, true},
                                          {&Binding::_x, true}};
constexpr ReturnItem returnNamesABCDX[] = {{&Binding::_a, true},
                                           {&Binding::_b, true},
                                           {&Binding::_c, true},
                                           {&Binding::_d, true},
                                           {&Binding::_x, true}};
constexpr ReturnItem returnNamesABCDEX[] = {{&Binding::_a, true},
                                            {&Binding::_b, true},
                                            {&Binding::_c, true},
                                            {&Binding::_d, true},
                                            {&Binding::_e, true},
                                            {&Binding::_x, true}};

constexpr SuiteCase suiteCases[] = {
    {"success-reads-joins-on-filters-0",
     "MATCH (a)-->(x), (b)-->(x) RETURN a.name, b.name, x.name",
     matchTwoIntoX, nullptr, returnNamesABX},

    {"success-reads-joins-on-filters-2",
     "MATCH (a)-->(x), (b)-->(x), (c)-->(x), (d)-->(x) RETURN a.name, b.name, c.name, d.name, x.name",
     matchFourIntoX, nullptr, returnNamesABCDX},

    {"success-reads-joins-on-filters-3",
     "MATCH (a)-->(x), (b)-->(x), (c)-->(x), (d)-->(x) RETURN a.name, b.name, c.name, d.name, x.name",
     matchFourIntoX, nullptr, returnNamesABCDX},

    {"success-reads-joins-on-filters-4",
     "MATCH (a)-->(x), (b)-->(x), (c)-->(x), (d)-->(x), (e)-->(x) RETURN a.name, b.name, c.name, d.name, e.name, x.name",
     matchFiveIntoX, nullptr, returnNamesABCDEX},

    {"success-reads-joins-on-filters-5",
     "MATCH (a)-->(x), (b)-->(x), (x)-->(c)-->(e), (x)-->(d)-->(e) RETURN a",
     matchTwoIntoXTwoOutToE, nullptr, returnA},

    {"success-reads-match-8",
     "MATCH (b)-->(c), (a)-->(b) RETURN a, b, c;",
     matchTwoHopWalk, nullptr, returnABC},

    {"success-reads-match-9",
     "MATCH (b)-->(c), (a)-->(b)-->(c) RETURN a, b, c;",
     matchTwoHopWalk, nullptr, returnABC},

    {"success-reads-match-where-2",
     "MATCH (a)-->(x), (b)-->(x), (c)-->(x) WHERE (a.name = b.name) OR (b.name = c.name) RETURN a.name, b.name, c.name, x.name",
     matchThreeIntoX, namesPairUp, returnNamesABCX},

    {"success-reads-match-where-3",
     "MATCH (a)-->(x), (b)-->(x), (c)-->(x) WHERE (a.name = b.name) OR (b.name = c.name) OR (b.name = x.name) RETURN a.name, b.name, c.name",
     matchThreeIntoX, namesPairUpOrBIsX, returnNamesABC},

    {"join-bad-keys-error",
     "MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g) RETURN a",
     matchDiamond, nullptr, returnA},

    {"large-double-diamond-join without its LIMIT",
     "MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g),(h),(i),(e),(h),(c)-->(j) RETURN a,c,e,g",
     matchDoubleDiamond, nullptr, returnACEG},

    {"large-double-diamond-join",
     "MATCH (a)-->(b),(c)-->(d)-->(e),(a)-->(f)-->(g),(c)-->(g),(h),(i),(e),(h),(c)-->(j) RETURN a,c,e,g LIMIT 10",
     matchDoubleDiamond, nullptr, returnACEG, 10},
};

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

    MatchGraph matchGraph;
    readMatchGraph(matchGraph, graph);

    LocalMemory memory;
    QueryInterpreterV3 interpreter(&db.getSystemManager());

    std::vector<Binding> bindings;
    RowCounts bruteForce;
    RowCounts v3;
    std::vector<Row> rows;
    bool allMatched = true;

    for (const SuiteCase& suiteCase : suiteCases) {
        suiteCase._match(bindings, matchGraph);
        if (suiteCase._where) {
            filter(bindings, matchGraph, suiteCase._where);
        }
        project(bruteForce, bindings, suiteCase._returnItems, matchGraph);

        if (!runV3(rows, interpreter, memory, suiteCase._query)) {
            return EXIT_FAILURE;
        }

        spdlog::info("{}", suiteCase._test);
        spdlog::info("  {}", suiteCase._query);

        if (suiteCase._limit == 0) {
            countRows(v3, rows);
            allMatched = compareRows(bruteForce, v3) && allMatched;
        } else {
            allMatched = checkLimitedRows(bruteForce, rows, suiteCase._limit) && allMatched;
        }
    }

    return allMatched ? EXIT_SUCCESS : EXIT_FAILURE;
}
